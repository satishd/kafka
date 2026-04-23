/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.rsm.hdfs;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.rsm.hdfs.generated.ConnectorCustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.oracle.bmc.hdfs.BmcConstants.NUM_READ_AHEAD_THREADS_KEY;
import static com.oracle.bmc.hdfs.BmcConstants.READ_AHEAD_BLOCK_COUNT_KEY;
import static com.oracle.bmc.hdfs.BmcConstants.READ_AHEAD_BLOCK_SIZE_KEY;
import static com.oracle.bmc.hdfs.BmcConstants.READ_AHEAD_KEY;
import static com.oracle.bmc.hdfs.BmcConstants.READ_DIRECT_RANGED_KEY;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_OCI_BUCKETS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_ENABLE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP;

/**
 * This class manages FileSystem instances by bucket.
 */
public class FileSystemManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemManager.class);
    private static final String HDFS_BUCKET_PREFIX = RemoteStorageProvider.HDFS + "://";
    private static final String OCI_BUCKET_PREFIX = RemoteStorageProvider.OCI + "://";

    static final Map<String, String> DYNAMIC_READ_AHEAD_CONFIG_MAP = Utils.mkMap(
        Utils.mkEntry(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP, READ_AHEAD_BLOCK_SIZE_KEY),
        Utils.mkEntry(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP, READ_AHEAD_BLOCK_COUNT_KEY),
        Utils.mkEntry(OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP, NUM_READ_AHEAD_THREADS_KEY)
    );

    private final Map<String, BiConsumer<String, Map<String, ?>>> dynamicConfigHandlers = Utils.mkMap(
        Utils.mkEntry(HDFS_OCI_BUCKETS_PROP, (k, c) -> reconfigureBuckets((String) c.get(k))),
        Utils.mkEntry(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP, (k, c) -> setReadAheadBlockCount(Integer.parseInt((String) c.get(k)))),
        Utils.mkEntry(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP, (k, c) -> setReadAheadBlockSize(Integer.parseInt((String) c.get(k)))),
        Utils.mkEntry(OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP, (k, c) -> setReadAheadThreadCount(Integer.parseInt((String) c.get(k))))
    );

    private static final Map<String, Function<String, Number>> NUMBER_CONFIG_PARSERS = Utils.mkMap(
        Utils.mkEntry(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP, Integer::parseInt),
        Utils.mkEntry(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP, Integer::parseInt),
        Utils.mkEntry(OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP, Integer::parseInt)
    );

    private String hdfsBucket;
    private final List<String> ociBuckets = new CopyOnWriteArrayList<>();
    private final Map<FileSystemKey, FileSystem> fileSystemByBucket = new ConcurrentHashMap<>();
    private final AtomicBoolean isReadAheadConfigChanged = new AtomicBoolean();

    private Configuration defaultHadoopConf;
    private volatile Configuration prefetchEnabledHadoopConf;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
            ThreadUtils.createThreadFactory("hdfs-rsm-scheduler", false));

    /**
     * Initialize this instance with the given configs
     *
     * @param configs Key-Value pairs of configuration parameters
     */
    public void configure(Map<String, ?> configs) {
        HDFSRemoteStorageManagerConfig conf = new HDFSRemoteStorageManagerConfig(configs, true);

        if (defaultHadoopConf == null) {
            // Loads configuration from hadoop configuration files in class path
            defaultHadoopConf = new Configuration();
        }

        String authentication = defaultHadoopConf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
        if (authentication != null && authentication.equalsIgnoreCase("kerberos")) {
            String user = conf.getString(HDFSRemoteStorageManagerConfig.HDFS_USER_PROP);
            String keytabPath = conf.getString(HDFSRemoteStorageManagerConfig.HDFS_KEYTAB_PATH_PROP);
            try {
                UserGroupInformation.setConfiguration(defaultHadoopConf);
                UserGroupInformation.loginUserFromKeytab(user, keytabPath);
            } catch (final Exception ex) {
                throw new RuntimeException(String.format("Unable to login as user: %s", user), ex);
            }
        }
        LOGGER.info("Default Hadoop Configuration: {}", confToString(defaultHadoopConf));

        if (prefetchEnabledHadoopConf == null) {
            Configuration hadoopConf = new Configuration(defaultHadoopConf);
            setOCIPrefetchConfiguration(hadoopConf, conf);
            // Disable cache for oci, otherwise FileSystem get will return the default filesystem for OCI
            hadoopConf.setBoolean("fs.oci.impl.disable.cache", true);
            prefetchEnabledHadoopConf = hadoopConf;
        }

        hdfsBucket = conf.getString(HDFSRemoteStorageManagerConfig.HDFS_DEFAULT_FS_URI_PROP);
        // Don't instantiate the FileSystem for HDFS eagerly.

        ociBuckets.addAll(conf.getList(HDFSRemoteStorageManagerConfig.HDFS_OCI_BUCKETS_PROP));
        for (String ociBucket : ociBuckets) {
            validateScheme(ociBucket, RemoteStorageProvider.OCI);
            FileSystemOptions defaultOciOpts = new FileSystemOptions(ociBucket);
            getFS(defaultOciOpts);

            FileSystemOptions prefetchOciOpts = new FileSystemOptions(ociBucket, true);
            getFS(prefetchOciOpts);
        }

        // Schedule periodic tasks
        executor.scheduleWithFixedDelay(this::relogin, 0, 5, TimeUnit.MINUTES);
        executor.scheduleWithFixedDelay(this::handleDynamicReadAheadConfigUpdates, 0, 1, TimeUnit.MINUTES);
    }

    /**
     * Configures the OCI-specific settings in the given Hadoop configuration to enable prefetch.
     * <p>
     * This method sets the read ahead block count, block size, and number of threads
     * in the Hadoop configuration based on the values specified in the provided
     * HDFS remote storage manager configuration when read-ahead option is enabled. Otherwise, it enables the
     * BmcDirectFSInputStream
     *
     * @param conf                           the Hadoop configuration object to be updated
     * @param hdfsRemoteStorageManagerConfig the configuration object containing OCI-specific prefetch settings
     */
    public void setOCIPrefetchConfiguration(Configuration conf, HDFSRemoteStorageManagerConfig hdfsRemoteStorageManagerConfig) {
        // This setting is to ensure that BmcDirectFSInputStream is enabled instead of BmcDirectRangedFSInputStream
        // for prefetch feature when OCI readAhead is disabled.
        conf.setBoolean(READ_DIRECT_RANGED_KEY, false);
        Boolean isOciReadAheadEnabled = hdfsRemoteStorageManagerConfig.getBoolean(OCI_PREFETCH_CLIENT_READ_AHEAD_ENABLE_PROP);
        conf.setBoolean(READ_AHEAD_KEY, isOciReadAheadEnabled);
        if (isOciReadAheadEnabled) {
            // overwrite config only when readAhead is enabled.
            Integer readAheadBlockCount = hdfsRemoteStorageManagerConfig.getInt(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP);
            Integer readAheadBlockSize = hdfsRemoteStorageManagerConfig.getInt(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP);
            Integer readAheadNumThreads = hdfsRemoteStorageManagerConfig.getInt(OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP);
            conf.setInt(READ_AHEAD_BLOCK_COUNT_KEY, readAheadBlockCount);
            conf.setInt(READ_AHEAD_BLOCK_SIZE_KEY, readAheadBlockSize);
            conf.setInt(NUM_READ_AHEAD_THREADS_KEY, readAheadNumThreads);
        }
        LOGGER.info("Hadoop configuration after setting prefetch properties for OCI : {}", confToString(conf));
    }

    private static String confToString(Configuration conf) {
        SortedMap<String, String> map = new TreeMap<>();
        conf.iterator().forEachRemaining(entry -> map.put(entry.getKey(), entry.getValue()));
        return map.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.joining("\n"));
    }

    /**
     * Validates the scheme of the bucket URI.
     *
     * @param bucket   the bucket URI
     * @param provider the expected provider
     */
    public void validateScheme(String bucket, RemoteStorageProvider provider) {
        if (!bucket.startsWith(provider.toString() + "://")) {
            throw new IllegalArgumentException(String.format("Invalid bucket URI: %s. It should start with %s://", bucket, provider));
        }
    }

    /**
     * Relogin to Kerberos if necessary.
     */
    void relogin() {
        try {
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "relogin: currentUser={}, loginUser={}",
                        currentUser,
                        UserGroupInformation.getLoginUser());
            }
            currentUser.checkTGTAndReloginFromKeytab();
        } catch (IOException e) {
            LOGGER.error("relogin: failed", e);
        }
    }

    public void handleDynamicReadAheadConfigUpdates() {
        try {
            if (isReadAheadConfigChanged.compareAndSet(true, false)) {
                for (String ociBucket : ociBuckets) {
                    FileSystemOptions readAheadOpts = new FileSystemOptions(ociBucket, true);
                    FileSystem readAheadEnabledFs = getFS(readAheadOpts);
                    if (shouldHandleReadAheadBlockCountChange(ociBucket, readAheadEnabledFs) ||
                        shouldHandleReadAheadBlockSizeChange(ociBucket, readAheadEnabledFs) ||
                        shouldHandleReadAheadNumThreadsChange(ociBucket, readAheadEnabledFs)) {
                        FileSystemKey fileSystemKey = new FileSystemKey(ociBucket, true);
                        FileSystem oldFs = fileSystemByBucket.put(fileSystemKey, createFileSystem(ociBucket, prefetchEnabledHadoopConf));
                        Utils.closeQuietly(oldFs, "Closed old FileSystem for bucket: " + ociBucket + " with readAhead enabled");
                        LOGGER.info("Dynamic readAhead config updated for bucket: {}", ociBucket);
                    }
                }
                LOGGER.info("Dynamic read ahead config change handled");
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to handle dynamic read ahead config updates", ex);
        }
    }

    private boolean shouldHandleReadAheadBlockCountChange(String ociBucket, FileSystem readAheadEnabledFs) {
        String key = READ_AHEAD_BLOCK_COUNT_KEY;
        int defaultValue = DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT;

        long currentValue = readAheadEnabledFs.getConf().getInt(key, defaultValue);
        long newValue = prefetchEnabledHadoopConf.getInt(key, defaultValue);
        if (currentValue != newValue) {
            LOGGER.debug("Read ahead block count for bucket: {}, changed from {} to {}", ociBucket, currentValue, newValue);
            return true;
        }
        return false;
    }

    private boolean shouldHandleReadAheadBlockSizeChange(String ociBucket, FileSystem readAheadEnabledFs) {
        String key = READ_AHEAD_BLOCK_SIZE_KEY;
        int defaultValue = DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE;

        long currentValue = readAheadEnabledFs.getConf().getInt(key, defaultValue);
        long newValue = prefetchEnabledHadoopConf.getInt(key, defaultValue);
        if (currentValue != newValue) {
            LOGGER.debug("Read ahead block size for bucket: {}, changed from {} to {}", ociBucket, currentValue, newValue);
            return true;
        }
        return false;
    }

    private boolean shouldHandleReadAheadNumThreadsChange(String ociBucket, FileSystem readAheadEnabledFs) {
        String key = NUM_READ_AHEAD_THREADS_KEY;
        int defaultValue = DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS;

        long currentValue = readAheadEnabledFs.getConf().getInt(key, defaultValue);
        long newValue = prefetchEnabledHadoopConf.getInt(key, defaultValue);
        if (currentValue != newValue) {
            LOGGER.debug("Read ahead num threads for bucket: {}, changed from {} to {}", ociBucket, currentValue, newValue);
            return true;
        }
        return false;
    }

    /**
     * Returns the set of reconfigurable configs.
     *
     * @return the set of reconfigurable configs
     */
    public Set<String> reconfigurableConfigs() {
        Set<String> reconfigurableConfigs = new HashSet<>(DYNAMIC_READ_AHEAD_CONFIG_MAP.keySet());
        reconfigurableConfigs.add(HDFS_OCI_BUCKETS_PROP);
        LOGGER.debug("Reconfigurable configs: {}", reconfigurableConfigs);
        return reconfigurableConfigs;
    }

    /**
     * Validates the reconfiguration.
     *
     * @param configs the new configs
     * @throws ConfigException if the reconfiguration is invalid
     */
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        LOGGER.debug("Validating dynamic configuration update: {}", configs);
        validateNumberConfigs(configs);
        validateNonNumberConfigs(configs);
    }

    private void validateNumberConfigs(Map<String, ?> configs) {
        for (Map.Entry<String, Function<String, Number>> entry : NUMBER_CONFIG_PARSERS.entrySet()) {
            String prop = entry.getKey();
            if (configs.containsKey(prop)) {
                // Find the corresponding Hadoop configuration property and configuration
                String hadoopConfKey = DYNAMIC_READ_AHEAD_CONFIG_MAP.get(prop);

                if (hadoopConfKey == null) {
                    throw new ConfigException(
                        String.format("Supplied property %s is not a dynamic readAhead config", prop));
                }

                Function<String, Number> parserFunc = entry.getValue();
                Number oldValue = parserFunc.apply(prefetchEnabledHadoopConf.get(hadoopConfKey));
                Number newValue = parserFunc.apply((String) configs.get(prop));
                RSMUtils.validateConfigValueRange(prop, oldValue.longValue(), newValue.longValue());
            }
        }
    }



    private void validateNonNumberConfigs(Map<String, ?> configs) {
        if (configs.containsKey(HDFS_OCI_BUCKETS_PROP)) {
            String newOciBuckets = (String) configs.get(HDFS_OCI_BUCKETS_PROP);
            if (newOciBuckets == null || newOciBuckets.isEmpty()) {
                throw new ConfigException(String.format("Dynamic config update validation failed for %s, value cannot be null or empty", HDFS_OCI_BUCKETS_PROP));
            }
            String[] ociBucketsArray = newOciBuckets.split(",");
            for (String ociBucket : ociBucketsArray) {
                validateScheme(ociBucket.trim(), RemoteStorageProvider.OCI);
            }
        }
    }

    /**
     * Reconfigures with the given configs.
     *
     * @param configs the new configs
     */
    public void reconfigure(Map<String, ?> configs) {
        LOGGER.info("Reconfiguring with configs: {}", configs);
        dynamicConfigHandlers.forEach((key, handler) -> {
            String value = (String) configs.get(key);
            if (value != null) {
                handler.accept(key, configs);
            }
        });
    }

    /**
     * Reconfigures the OCI buckets.
     *
     * @param newOciBucketsStr the new OCI buckets string
     */
    private void reconfigureBuckets(String newOciBucketsStr) {
        Set<String> newOciBuckets = Arrays.stream(newOciBucketsStr.split(","))
                .map(String::trim)
                .collect(Collectors.toSet());
        Set<String> bucketsToRemove = ociBuckets.stream()
                .filter(bucket -> !newOciBuckets.contains(bucket))
                .collect(Collectors.toSet());
        Set<String> bucketsToAdd = newOciBuckets.stream()
                .filter(bucket -> !ociBuckets.contains(bucket))
                .collect(Collectors.toSet());
        String previousBuckets = String.join(", ", ociBuckets);
        ociBuckets.addAll(bucketsToAdd);
        ociBuckets.removeAll(bucketsToRemove);
        LOGGER.info("Updated the OCI buckets. previousBuckets: [{}], bucketsToAdd: {}, bucketsToRemove: {}. " +
                        "updatedBuckets: {}", previousBuckets, bucketsToAdd, bucketsToRemove, ociBuckets);
    }

    void setReadAheadBlockCount(int readAheadBlockCount) {
        LOGGER.info("Setting read ahead block count to: {}", readAheadBlockCount);
        updateReadAheadHadoopConf(READ_AHEAD_BLOCK_COUNT_KEY, String.valueOf(readAheadBlockCount));
        isReadAheadConfigChanged.set(true);
    }

    void setReadAheadBlockSize(int readAheadBlockSize) {
        LOGGER.info("Setting read ahead block size to: {}", readAheadBlockSize);
        updateReadAheadHadoopConf(READ_AHEAD_BLOCK_SIZE_KEY, String.valueOf(readAheadBlockSize));
        isReadAheadConfigChanged.set(true);
    }

    void setReadAheadThreadCount(int readAheadThreadCount) {
        LOGGER.info("Setting read ahead thread count to: {}", readAheadThreadCount);
        updateReadAheadHadoopConf(NUM_READ_AHEAD_THREADS_KEY, String.valueOf(readAheadThreadCount));
        isReadAheadConfigChanged.set(true);
    }

    /**
     * Updates the read-ahead configuration in the Hadoop configuration object.
     * If the provided key is not in the allowed list of dynamic read-ahead configurations,
     * an exception will be thrown.
     *
     * @param key   the configuration key to be updated
     * @param value the new value for the configuration key
     * @throws IllegalArgumentException if the provided key is not allowed for updates
     */
    private void updateReadAheadHadoopConf(String key, String value) {
        if (!DYNAMIC_READ_AHEAD_CONFIG_MAP.containsValue(key)) {
            throw new IllegalArgumentException(String.format("ReadAhead configuration: %s is not allowed for updates", key));
        }

        Configuration hadoopConf = new Configuration(prefetchEnabledHadoopConf);
        hadoopConf.set(key, value);
        this.prefetchEnabledHadoopConf = hadoopConf;
    }

    public FileSystem getFS(FileSystemOptions options) {
        String bucket = options.bucket();
        // Only use read ahead for OCI buckets when explicitly enabled
        boolean isPrefetchEnabled = !bucket.equals(hdfsBucket) && options.prefetchEnabled();

        Configuration conf = isPrefetchEnabled ? prefetchEnabledHadoopConf : defaultHadoopConf;
        FileSystemKey key = new FileSystemKey(bucket, isPrefetchEnabled);
        return fileSystemByBucket.computeIfAbsent(key, k -> createFileSystem(bucket, conf));
    }

    /**
     * Creates a FileSystem for the given bucket and configuration.
     *
     * @param bucket the bucket
     * @param conf   the configuration
     * @return the FileSystem
     */
    private FileSystem createFileSystem(String bucket, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(new URI(bucket), conf);
            LOGGER.info("FileSystem created for uri: {}", bucket);
            return fs;
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException("Unable to create file system instance for uri: " + bucket, e);
        }
    }

    /**
     * Gets the HDFS bucket.
     *
     * @return the HDFS bucket
     */
    public String getHdfsBucket() {
        return hdfsBucket;
    }

    /**
     * Gets the OCI buckets.
     *
     * @return the OCI buckets
     */
    public List<String> getOciBuckets() {
        return ociBuckets;
    }

    /**
     * Gets all bucket names.
     *
     * @return all bucket names
     */
    public Set<String> getAllBucketNames() {
        return fileSystemByBucket.keySet().stream().map(key -> key.bucket).collect(Collectors.toSet());
    }

    public Configuration getDefaultHadoopConf() {
        return defaultHadoopConf;
    }

    /**
     * Sets the default Hadoop configuration.
     *
     * @param configuration the default Hadoop configuration
     */
    public void setDefaultHadoopConfiguration(Configuration configuration) {
        this.defaultHadoopConf = configuration;
    }

    /**
     * Closes all FileSystem instances.
     */
    public void close() {
        for (Map.Entry<FileSystemKey, FileSystem> entry : fileSystemByBucket.entrySet()) {
            Utils.closeQuietly(entry.getValue(), "Closed FileSystem for bucket: " + entry.getKey().bucket);
        }
        // Don't clear the map to avoid unintended FileSystem instance recreation.
        ThreadUtils.shutdownExecutorServiceQuietly(executor, 5, TimeUnit.SECONDS);
    }

    /**
     * Finds the bucket name for the given provider. This should be used only for writing data.
     * If multiple OCI buckets are configured, then it returns the bucket in round-robin fashion.
     * @param provider storage provider
     * @param segmentId the segment ID
     * @return bucket name
     */
    public String findBucket(RemoteStorageProvider provider, RemoteLogSegmentId segmentId) {
        if (provider == RemoteStorageProvider.HDFS) {
            return hdfsBucket;
        } else if (provider == RemoteStorageProvider.OCI) {
            if (ociBuckets.isEmpty()) {
                throw new IllegalArgumentException("No OCI buckets are configured for writing");
            }
            int idx = Math.abs(segmentId.topicIdPartition().hashCode() % ociBuckets.size());
            return ociBuckets.get(idx);
        } else {
            throw new IllegalArgumentException("Unknown remote storage provider: " + provider);
        }
    }

    public String getBucket(RemoteLogSegmentMetadata metadata) {
        return metadata.customMetadata()
            .map(FileSystemManager::getBucket)
            .orElse(hdfsBucket);
    }

    static String getBucket(RemoteLogSegmentMetadata.CustomMetadata customMetadata) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(customMetadata.value());
        ConnectorCustomMetadata connectorCustomMetadata =
                new ConnectorCustomMetadata(new ByteBufferAccessor(byteBuffer), ConnectorCustomMetadata.LOWEST_SUPPORTED_VERSION);
        return connectorCustomMetadata.uri();
    }

    /**
     * Gets the remote storage provider for the given bucket.
     *
     * @param bucket the bucket
     * @return the remote storage provider
     */
    public RemoteStorageProvider getRemoteStorageProvider(String bucket) {
        if (bucket.isEmpty() || bucket.startsWith(HDFS_BUCKET_PREFIX)) {
            return RemoteStorageProvider.HDFS;
        } else if (bucket.startsWith(OCI_BUCKET_PREFIX)) {
            return RemoteStorageProvider.OCI;
        } else {
            throw new IllegalArgumentException("Unknown remote storage provider for bucket: " + bucket);
        }
    }

    /**
     * Key class for the fileSystemByBucket map.
     */
    static class FileSystemKey {
        final String bucket;
        final boolean prefetchEnabled;

        public FileSystemKey(String bucket, boolean prefetchEnabled) {
            this.bucket = bucket;
            this.prefetchEnabled = prefetchEnabled;
        }

        @Override
        public String toString() {
            return "FileSystemKey{" +
                "bucket='" + bucket + '\'' +
                ", prefetchEnabled=" + prefetchEnabled +
                '}';
        }


        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            FileSystemKey that = (FileSystemKey) o;
            return prefetchEnabled == that.prefetchEnabled && Objects.equals(bucket, that.bucket);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucket, prefetchEnabled);
        }
    }
}
