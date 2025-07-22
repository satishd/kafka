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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_OCI_BUCKETS_PROP;

/**
 * This class manages FileSystem instances by bucket.
 */
public class FileSystemManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemManager.class);
    private static final String HDFS_BUCKET_PREFIX = RemoteStorageProvider.HDFS + "://";
    private static final String OCI_BUCKET_PREFIX = RemoteStorageProvider.OCI + "://";

    static final Map<String, String> DYNAMIC_HEDGED_READS_CONFIG_MAP = Utils.mkMap(
        Utils.mkEntry(HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP, THRESHOLD_MILLIS_KEY),
        Utils.mkEntry(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP, CORE_SIZE_KEY),
        Utils.mkEntry(HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP, MAX_SIZE_KEY)
    );

    private static final Map<String, Function<String, Number>> CONFIG_PARSERS = Utils.mkMap(
        Utils.mkEntry(HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP, Long::parseLong),
        Utils.mkEntry(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP, Integer::parseInt),
        Utils.mkEntry(HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP, Integer::parseInt)
    );

    private static final Map<String, String> BUCKET_MAPPING = new HashMap<>();

    static {
        // deprecated buckets
        BUCKET_MAPPING.put("oci://uber-prod-ea6bj@ax9estk6tuja/jwj42", "oci://uber-prod-ea6bj@ax9estk6tuja");
    }


    private String hdfsBucket;
    private final List<String> ociBuckets = new CopyOnWriteArrayList<>();
    private final Map<FileSystemKey, FileSystem> fileSystemByBucket = new ConcurrentHashMap<>();
    private final AtomicBoolean isHedgedReadsThresholdChanged = new AtomicBoolean();
    private final AtomicBoolean isHedgedReadsThreadConfigChanged = new AtomicBoolean();

    private Configuration defaultHadoopConf;
    private volatile Configuration hedgedReadsHadoopConf;
    private Configuration readAheadHadoopConf;

    // TODO fix the thread factory name pattern
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

        if (hedgedReadsHadoopConf == null) {
            Configuration hadoopConf = new Configuration(defaultHadoopConf);
            setHedgedReadsConfiguration(hadoopConf, conf);
            // Disable cache, otherwise FileSystem get will return the same filesystem for HDFS without hedged reads enabled
            hadoopConf.setBoolean("fs.hdfs.impl.disable.cache", true);
            hedgedReadsHadoopConf = hadoopConf;
        }

        hdfsBucket = conf.getString(HDFSRemoteStorageManagerConfig.HDFS_DEFAULT_FS_URI_PROP);
        validateScheme(hdfsBucket, RemoteStorageProvider.HDFS);
        // FileSystem for HDFS without hedged reads enabled
        FileSystemOptions defaultHdfsOpts = new FileSystemOptions(hdfsBucket);
        getFS(defaultHdfsOpts);
        // FileSystem for HDFS with hedged reads enabled
        FileSystemOptions hedgedReadsOpts = new FileSystemOptions(hdfsBucket, true);
        getFS(hedgedReadsOpts);

        ociBuckets.addAll(conf.getList(HDFSRemoteStorageManagerConfig.HDFS_OCI_BUCKETS_PROP));
        for (String ociBucket : ociBuckets) {
            validateScheme(ociBucket, RemoteStorageProvider.OCI);
            FileSystemOptions defaultOciOpts = new FileSystemOptions(ociBucket);
            getFS(defaultOciOpts);
        }

        // Schedule periodic tasks
        executor.scheduleWithFixedDelay(this::relogin, 0, 5, TimeUnit.MINUTES);
        executor.scheduleWithFixedDelay(this::handleDynamicHedgedReadsConfigUpdates, 0, 1, TimeUnit.MINUTES);
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

    /**
     * Handles dynamic hedged reads configuration updates.
     */
    public void handleDynamicHedgedReadsConfigUpdates() {
        try {
            FileSystemOptions hedgedReadsOpts = new FileSystemOptions(hdfsBucket, true);
            if (isHedgedReadsThresholdChanged.compareAndSet(true, false)) {
                FileSystem hedgedReadsEnabledFs = getFS(hedgedReadsOpts);
                if (shouldHandleHedgedReadsThresholdMsChange(hedgedReadsEnabledFs)) {
                    FileSystemKey fileSystemKey = new FileSystemKey(hdfsBucket, true, false);
                    FileSystem oldFs = fileSystemByBucket.put(fileSystemKey, createFileSystem(hdfsBucket, hedgedReadsHadoopConf));
                    Utils.closeQuietly(oldFs, "Closed old FileSystem for bucket: " + hdfsBucket + " with hedged reads enabled");
                }
                LOGGER.info("Dynamic hedged reads thresholdMs config change handled");
            }
            if (isHedgedReadsThreadConfigChanged.compareAndSet(true, false)) {
                FileSystem hedgedReadsEnabledFs = getFS(hedgedReadsOpts);
                handleReadThreadPoolCoreSizeChange(hedgedReadsEnabledFs);
                handleReadThreadPoolMaxSizeChange(hedgedReadsEnabledFs);
                LOGGER.info("Dynamic hedged reads thread pool configuration change handled");
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to handle dynamic hedged reads config updates", ex);
        }
    }

    /**
     * Returns the set of reconfigurable configs.
     *
     * @return the set of reconfigurable configs
     */
    public Set<String> reconfigurableConfigs() {
        Set<String> reconfigurableConfigs = new HashSet<>(DYNAMIC_HEDGED_READS_CONFIG_MAP.keySet());
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
        for (Map.Entry<String, Function<String, Number>> entry : CONFIG_PARSERS.entrySet()) {
            String prop = entry.getKey();
            Function<String, Number> parserFunc = entry.getValue();
            if (configs.containsKey(prop)) {
                String hdfsConfig = DYNAMIC_HEDGED_READS_CONFIG_MAP.get(prop);
                Number oldValue = parserFunc.apply(hedgedReadsHadoopConf.get(hdfsConfig));
                Number newValue = parserFunc.apply((String) configs.get(prop));
                validate(prop, oldValue.longValue(), newValue.longValue());
            }
        }
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
     * Validates the new value against the current value.
     *
     * @param prop         the property name
     * @param currentValue the current value
     * @param newValue     the new value
     * @throws ConfigException if the validation fails
     */
    private void validate(String prop, long currentValue, long newValue) {
        String errorMsg = String.format("Dynamic config update validation failed for %s=%s", prop, newValue);
        if (newValue != currentValue) {
            if (newValue < currentValue / 2) {
                throw new ConfigException(String.format("%s, value should be at least half the current value: %d",
                        errorMsg, currentValue));
            }
            if (newValue > currentValue * 2) {
                throw new ConfigException(String.format("%s, value should not be greater than double the current value: %d",
                        errorMsg, currentValue));
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
        String thresholdMillis = (String) configs.get(HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP);
        if (thresholdMillis != null) {
            setHedgedReadThresholdMillis(Long.parseLong(thresholdMillis));
        }
        String coreSize = (String) configs.get(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP);
        if (coreSize != null) {
            setReadThreadPoolCoreSize(Integer.parseInt(coreSize));
        }
        String maxSize = (String) configs.get(HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP);
        if (maxSize != null) {
            setReadThreadPoolMaxSize(Integer.parseInt(maxSize));
        }
        String newOciBucketsStr = (String) configs.get(HDFS_OCI_BUCKETS_PROP);
        if (newOciBucketsStr != null) {
            reconfigureBuckets(newOciBucketsStr);
        }
    }

    /**
     * Sets the hedged reads configuration.
     *
     * @param conf                        the Hadoop configuration
     * @param hdfsRemoteStorageManagerConfig the HDFS remote storage manager config
     */
    public void setHedgedReadsConfiguration(Configuration conf, HDFSRemoteStorageManagerConfig hdfsRemoteStorageManagerConfig) {
        LOGGER.debug("Hadoop configuration before setting hedged read properties: {}", conf);
        Long hedgedReadThresholdMillis = hdfsRemoteStorageManagerConfig.getLong(HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP);
        Integer clientReadThreadPoolCoreSize = hdfsRemoteStorageManagerConfig.getInt(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP);
        Integer clientReadThreadPoolMaxSize = hdfsRemoteStorageManagerConfig.getInt(HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP);
        Integer clientReadThreadPoolKeepAliveTime = hdfsRemoteStorageManagerConfig.getInt(HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS_PROP);
        Boolean isClientReadThreadPoolCoreThreadTimeoutAllowed = hdfsRemoteStorageManagerConfig.getBoolean(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED_PROP);

        conf.set(HdfsClientConfigKeys.HedgedRead.ENABLED, Boolean.TRUE.toString());
        conf.set(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY, hedgedReadThresholdMillis.toString());
        conf.set(HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY, clientReadThreadPoolCoreSize.toString());
        conf.set(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY, clientReadThreadPoolMaxSize.toString());
        conf.set(HdfsClientConfigKeys.ReadThreadPool.KEEP_ALIVE_TIME_KEY, clientReadThreadPoolKeepAliveTime.toString());
        conf.set(HdfsClientConfigKeys.ReadThreadPool.ALLOW_CORE_THREAD_TIMEOUT_KEY, isClientReadThreadPoolCoreThreadTimeoutAllowed.toString());
        LOGGER.debug("Hadoop configuration after setting hedged read properties: {}", conf);
    }

    /**
     * Sets the hedged read threshold millis.
     *
     * @param hedgedReadThresholdMillis the hedged read threshold millis
     */
    void setHedgedReadThresholdMillis(long hedgedReadThresholdMillis) {
        LOGGER.info("Setting hedged read threshold millis to: {}", hedgedReadThresholdMillis);
        updateHedgedReadsHadoopConf(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY, String.valueOf(hedgedReadThresholdMillis));
        isHedgedReadsThresholdChanged.set(true);
    }

    /**
     * Sets the read thread pool core size.
     *
     * @param coreSize the core size
     */
    void setReadThreadPoolCoreSize(int coreSize) {
        LOGGER.info("Setting read thread pool core size to: {}", coreSize);
        updateHedgedReadsHadoopConf(HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY, String.valueOf(coreSize));
        isHedgedReadsThreadConfigChanged.set(true);
    }

    /**
     * Sets the read thread pool max size.
     *
     * @param maxSize the max size
     */
    void setReadThreadPoolMaxSize(int maxSize) {
        LOGGER.info("Setting read thread pool max size to: {}", maxSize);
        updateHedgedReadsHadoopConf(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY, String.valueOf(maxSize));
        isHedgedReadsThreadConfigChanged.set(true);
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

    /**
     * Updates the hedged reads Hadoop configuration.
     *
     * @param key   the key
     * @param value the value
     */
    private void updateHedgedReadsHadoopConf(String key, String value) {
        if (!DYNAMIC_HEDGED_READS_CONFIG_MAP.containsValue(key)) {
            throw new IllegalArgumentException(String.format("Hedged reads configuration: %s is not allowed for updates", key));
        }

        Configuration hadoopConf = new Configuration(hedgedReadsHadoopConf);
        hadoopConf.set(key, value);
        this.hedgedReadsHadoopConf = hadoopConf;
    }

    public FileSystem getFS(FileSystemOptions options) {
        String bucket = options.bucket();
        // Only use hedged reads for the HDFS bucket when explicitly enabled
        boolean useHedgedReads = bucket.equals(hdfsBucket) && options.hedgedReadsEnabled();
        // Only use read ahead for OCS buckets when explicitly enabled
        boolean useReadAhead = !bucket.equals(hdfsBucket) && options.readAheadEnabled();

        Configuration conf = useHedgedReads ? hedgedReadsHadoopConf : useReadAhead ? readAheadHadoopConf : defaultHadoopConf;
        FileSystemKey key = new FileSystemKey(bucket, useHedgedReads, useReadAhead);
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
     * Checks if the hedged reads threshold has changed.
     *
     * @param hedgedReadsEnabledFs the hedged reads enabled FileSystem
     * @return true if the threshold has changed, false otherwise
     */
    private boolean shouldHandleHedgedReadsThresholdMsChange(FileSystem hedgedReadsEnabledFs) {
        String key = HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY;
        long defaultValue = DEFAULT_HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS;

        long currentThreshold = hedgedReadsEnabledFs.getConf().getLong(key, defaultValue);
        long newThreshold = hedgedReadsHadoopConf.getLong(key, defaultValue);
        if (currentThreshold != newThreshold) {
            LOGGER.debug("Hedged reads thresholdMs changed from {} ms to {} ms", currentThreshold, newThreshold);
            return true;
        }
        return false;
    }

    /**
     * Handles changes to the read thread pool core size.
     *
     * @param hedgedReadsEnabledFs the hedged reads enabled FileSystem
     */
    private void handleReadThreadPoolCoreSizeChange(FileSystem hedgedReadsEnabledFs) {
        String key = HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY;
        int defaultValue = DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE;

        int currentCoreSize = hedgedReadsEnabledFs.getConf().getInt(key, defaultValue);
        int newCoreSize = hedgedReadsHadoopConf.getInt(key, defaultValue);
        if (currentCoreSize != newCoreSize) {
            LOGGER.debug("Core pool size changed from {} to {}", currentCoreSize, newCoreSize);
            ((DistributedFileSystem) hedgedReadsEnabledFs).setDFSClientReaderThreadPoolCoreSize(newCoreSize);
        }
    }

    /**
     * Handles changes to the read thread pool max size.
     *
     * @param hedgedReadsEnabledFs the hedged reads enabled FileSystem
     */
    private void handleReadThreadPoolMaxSizeChange(FileSystem hedgedReadsEnabledFs) {
        String key = HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY;
        int defaultValue = DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE;

        int currentMaxSize = hedgedReadsEnabledFs.getConf().getInt(key, defaultValue);
        int newMaxSize = hedgedReadsHadoopConf.getInt(key, defaultValue);
        if (currentMaxSize != newMaxSize) {
            LOGGER.debug("Max pool size changed from {} to {}", currentMaxSize, newMaxSize);
            ((DistributedFileSystem) hedgedReadsEnabledFs).setDFSClientReaderThreadPoolMaxSize(newMaxSize);
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
        fileSystemByBucket.clear();
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
            String bucket = ociBuckets.get(idx);
            return BUCKET_MAPPING.getOrDefault(bucket, bucket);
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
        String bucket;
        try {
            ByteBuffer byteBuffer = ByteBuffer.wrap(customMetadata.value());
            ConnectorCustomMetadata connectorCustomMetadata =
                new ConnectorCustomMetadata(new ByteBufferAccessor(byteBuffer), ConnectorCustomMetadata.LOWEST_SUPPORTED_VERSION);
            bucket = connectorCustomMetadata.uri();
        } catch (Exception e) {
            // Backward compatibility
            // `kafka-dev1-dca` is already deployed with the old build. This can be removed once the stress test is completed.
            bucket = new String(customMetadata.value(), StandardCharsets.UTF_8);
        }
        return BUCKET_MAPPING.getOrDefault(bucket, bucket);
    }

    /**
     * Gets the remote storage provider for the given bucket.
     *
     * @param bucket the bucket
     * @return the remote storage provider
     */
    public RemoteStorageProvider getRemoteStorageProvider(String bucket) {
        if (bucket.startsWith(HDFS_BUCKET_PREFIX)) {
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
        final boolean hedgedReadsEnabled;
        final boolean readAheadEnabled;

        public FileSystemKey(String bucket, boolean hedgedReadsEnabled, boolean readAheadEnabled) {
            this.bucket = bucket;
            this.hedgedReadsEnabled = hedgedReadsEnabled;
            this.readAheadEnabled = readAheadEnabled;
        }

        @Override
        public String toString() {
            return "FileSystemKey{" +
                "bucket='" + bucket + '\'' +
                ", hedgedReadsEnabled=" + hedgedReadsEnabled +
                ", readAheadEnabled=" + readAheadEnabled +
                '}';
        }


        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            FileSystemKey that = (FileSystemKey) o;
            return hedgedReadsEnabled == that.hedgedReadsEnabled && readAheadEnabled == that.readAheadEnabled && Objects.equals(bucket, that.bucket);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucket, hedgedReadsEnabled, readAheadEnabled);
        }
    }
}
