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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.rsm.hdfs.pool.ByteBufferPool;
import org.apache.kafka.rsm.hdfs.pool.ByteBufferPoolImpl;
import org.apache.kafka.rsm.hdfs.pool.ByteBufferWrapper;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteReadContext;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageProvider;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DEFAULT_FS_URI_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_THREAD_TIMEOUT_ALLOWED_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_KEEP_ALIVE_TIME_SECS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_KEYTAB_PATH_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_OCI_BUCKETS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_BUFFER_POOL_MAX_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_BYTES_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_USER_PROP;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.LEADER_EPOCH_CHECKPOINT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.OFFSET_INDEX;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.SEGMENT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.TIMESTAMP_INDEX;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.TRANSACTION_INDEX;

public class HDFSRemoteStorageManager implements RemoteStorageManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRemoteStorageManager.class);
    static final String KLOAK_USER = Path.SEPARATOR + "user" + Path.SEPARATOR + "kloak" + Path.SEPARATOR;
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
    private static final RemoteReadContext DEFAULT_READ_CONTEXT = new RemoteReadContext(true, false);

    private final AtomicLong auxBytesReadFromRemote = new AtomicLong(0);
    private String baseDir;
    private Configuration defaultHadoopConf;
    private volatile Configuration hedgedReadsHadoopConf;
    private int cacheLineSize;
    private LRUCache readCache;
    private ByteBufferPool byteBufferPool;
    private final Time time = Time.SYSTEM;
    private final Cache<RemoteLogSegmentId, SegmentHeaderHolder> segmentHeaderHolderCache =
            Caffeine.newBuilder()
                    .maximumSize(20_000)
                    .expireAfterWrite(Duration.ofMinutes(10))
                    .build();
    private final HDFSRemoteStorageManagerMetrics metrics;
    private final AtomicInteger openInputStreamCount = new AtomicInteger();
    private final AtomicInteger openOutputStreamCount = new AtomicInteger();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
            ThreadUtils.createThreadFactory("hdfs-rsm-scheduler", false));

    private String hdfsBucket;
    private final List<String> ociBuckets = new CopyOnWriteArrayList<>();
    private final Map<FileSystemKey, FileSystem> fileSystemByBucket = new ConcurrentHashMap<>();
    private final AtomicBoolean isHedgedReadsThresholdChanged = new AtomicBoolean();
    private final AtomicBoolean isHedgedReadsThreadConfigChanged = new AtomicBoolean();

    public HDFSRemoteStorageManager() {
        this.metrics = new HDFSRemoteStorageManagerMetrics();
    }

    /**
     * Initialize this instance with the given configs
     *
     * @param configs Key-Value pairs of configuration parameters
     */
    @Override
    public void configure(Map<String, ?> configs) {
        HDFSRemoteStorageManagerConfig conf = new HDFSRemoteStorageManagerConfig(configs, true);
        baseDir = KLOAK_USER + conf.getString(HDFS_BASE_DIR_PROP);
        cacheLineSize = conf.getInt(HDFS_REMOTE_READ_BYTES_PROP);
        long cacheSize = conf.getLong(HDFS_REMOTE_READ_CACHE_BYTES_PROP);
        if (cacheSize < cacheLineSize) {
            throw new IllegalArgumentException(String.format("%s is larger than %s", HDFS_REMOTE_READ_BYTES_PROP, HDFS_REMOTE_READ_CACHE_BYTES_PROP));
        }
        readCache = new LRUCache(cacheSize);
        int bufferPoolMaxSize = conf.getInt(HDFS_REMOTE_READ_CACHE_BUFFER_POOL_MAX_SIZE_PROP);
        byteBufferPool = new ByteBufferPoolImpl(cacheLineSize, bufferPoolMaxSize);

        if (defaultHadoopConf == null) {
            // Loads configuration from hadoop configuration files in class path
            defaultHadoopConf = new Configuration();
        }

        String authentication = defaultHadoopConf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
        if (authentication.equalsIgnoreCase("kerberos")) {
            String user = conf.getString(HDFS_USER_PROP);
            String keytabPath = conf.getString(HDFS_KEYTAB_PATH_PROP);
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

        hdfsBucket = conf.getString(HDFS_DEFAULT_FS_URI_PROP);
        validateScheme(hdfsBucket, RemoteStorageProvider.HDFS);
        // FileSystem for HDFS without hedged reads enabled
        getFS(hdfsBucket);
        // FileSystem for HDFS with hedged reads enabled
        getFS(hdfsBucket, true);

        ociBuckets.addAll(conf.getList(HDFS_OCI_BUCKETS_PROP));
        for (String ociBucket : ociBuckets) {
            validateScheme(ociBucket, RemoteStorageProvider.OCI);
            getFS(ociBucket);
        }

        registerMetrics(readCache);
        registerBufferPoolMetrics();
        registerHedgedReadMetrics();
        registerHDFSReadMetrics();
        registerStreamMetrics();

        executor.scheduleWithFixedDelay(this::relogin, 0, 5, TimeUnit.MINUTES);
        executor.scheduleWithFixedDelay(this::handleDynamicHedgedReadsConfigUpdates, 0, 1, TimeUnit.MINUTES);
        LOGGER.info("Configured with baseDir: {}, cacheLineSize: {}, cacheSize: {}, defaultFsUri: {}, ociBuckets: {}",
                baseDir, cacheLineSize, cacheSize, hdfsBucket, ociBuckets);
    }

    private void validateScheme(String bucket, RemoteStorageProvider provider) {
        if (!bucket.startsWith(provider.toString() + "://")) {
            throw new IllegalArgumentException(String.format("Invalid bucket URI: %s. It should start with %s://", bucket, provider));
        }
    }

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

    void handleDynamicHedgedReadsConfigUpdates() {
        try {
            if (isHedgedReadsThresholdChanged.compareAndSet(true, false)) {
                FileSystem hedgedReadsEnabledFs = getFS(hdfsBucket, true);
                if (shouldHandleHedgedReadsThresholdMsChange(hedgedReadsEnabledFs)) {
                    FileSystemKey fileSystemKey = new FileSystemKey(hdfsBucket, true);
                    FileSystem oldFs = fileSystemByBucket.put(fileSystemKey, createFileSystem(hdfsBucket, hedgedReadsHadoopConf));
                    Utils.closeQuietly(oldFs, "Closed old FileSystem for bucket: " + hdfsBucket + " with hedged reads enabled");
                }
                LOGGER.info("Dynamic hedged reads thresholdMs config change handled");
            }
            if (isHedgedReadsThreadConfigChanged.compareAndSet(true, false)) {
                FileSystem hedgedReadsEnabledFs = getFS(hdfsBucket, true);
                handleReadThreadPoolCoreSizeChange(hedgedReadsEnabledFs);
                handleReadThreadPoolMaxSizeChange(hedgedReadsEnabledFs);
                LOGGER.info("Dynamic hedged reads thread pool configuration change handled");
            }
        } catch (Exception ex) {
            LOGGER.error("Failed to handle dynamic hedged reads config updates", ex);
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        Set<String> reconfigurableConfigs = new HashSet<>(DYNAMIC_HEDGED_READS_CONFIG_MAP.keySet());
        reconfigurableConfigs.add(HDFS_OCI_BUCKETS_PROP);
        LOGGER.debug("Reconfigurable configs: {}", reconfigurableConfigs);
        return reconfigurableConfigs;
    }

    @Override
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

    @Override
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

    void setHedgedReadsConfiguration(Configuration conf, HDFSRemoteStorageManagerConfig hdfsRemoteStorageManagerConfig) {
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

    void setHedgedReadThresholdMillis(long hedgedReadThresholdMillis) {
        LOGGER.info("Setting hedged read threshold millis to: {}", hedgedReadThresholdMillis);
        updateHedgedReadsHadoopConf(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY, String.valueOf(hedgedReadThresholdMillis));
        isHedgedReadsThresholdChanged.set(true);
    }

    void setReadThreadPoolCoreSize(int coreSize) {
        LOGGER.info("Setting read thread pool core size to: {}", coreSize);
        updateHedgedReadsHadoopConf(HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY, String.valueOf(coreSize));
        isHedgedReadsThreadConfigChanged.set(true);
    }

    void setReadThreadPoolMaxSize(int maxSize) {
        LOGGER.info("Setting read thread pool max size to: {}", maxSize);
        updateHedgedReadsHadoopConf(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY, String.valueOf(maxSize));
        isHedgedReadsThreadConfigChanged.set(true);
    }

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

    private void updateHedgedReadsHadoopConf(String key, String value) {
        if (!DYNAMIC_HEDGED_READS_CONFIG_MAP.containsValue(key)) {
            throw new IllegalArgumentException(String.format("Hedged reads configuration: %s is not allowed for updates", key));
        }

        Configuration hadoopConf = new Configuration(hedgedReadsHadoopConf);
        hadoopConf.set(key, value);
        this.hedgedReadsHadoopConf = hadoopConf;
    }

    void registerMetrics(LRUCache cache) {
        metrics.registerCacheMetrics(cache);
    }

    void registerBufferPoolMetrics() {
        metrics.registerBufferPoolMetrics(byteBufferPool);
    }

    void registerHedgedReadMetrics() {
        metrics.registerHedgedReadMetrics(() -> {
            // We can use either:
            //  1. getFs(bucket) or
            //  2. getFS(bucket, true)
            // since those metrics are captured at the JVM level and not at FS instance level.
            // When using `getFS(bucket, true)`, then it might create another FileSystem instance for Hedged reads,
            // even though none of the topics are enabled with Hedged reads.
            return getFS(hdfsBucket);
        });
    }

    void registerHDFSReadMetrics() {
        metrics.registerHDFSReadMetrics();
    }

    void registerStreamMetrics() {
        metrics.registerStreamMetrics(openInputStreamCount, openOutputStreamCount);
    }

    /**
     * Finds the bucket name for the given provider. This should be used only for writing data.
     * If multiple OCI buckets are configured, then it returns the bucket in round-robin fashion.
     * @param provider storage provider
     * @return bucket name
     */
    String findBucket(RemoteStorageProvider provider, RemoteLogSegmentId segmentId) {
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

    /**
     * Returns the bucket name for the given metadata.
     * @param metadata metadata
     * @return bucket name
     */
    private String getBucket(RemoteLogSegmentMetadata metadata) {
        Optional<RemoteLogSegmentMetadata.CustomMetadata> customMetadataOpt = metadata.customMetadata();
        return customMetadataOpt.map(cm -> new String(cm.value(), StandardCharsets.UTF_8))
                .orElseGet(() -> hdfsBucket);
    }

    Set<String> getBuckets(List<RemoteLogSegmentMetadata> metadataList) {
        return metadataList.stream()
                .map(this::getBucket)
                .collect(Collectors.toSet());
    }

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata metadata, LogSegmentData segmentData) throws RemoteStorageException {
        final RemoteStorageProvider provider = segmentData.storageProvider();
        final Path dirPath = new Path(getSegmentRemoteDir(metadata.remoteLogSegmentId()));
        final String bucket = findBucket(provider, metadata.remoteLogSegmentId());
        try (final FSDataOutputStream fsOut = getFS(bucket).create(dirPath)) {
            openOutputStreamCount.incrementAndGet();
            final LogSegmentDataHeader header = LogSegmentDataHeader.create(segmentData);
            byte[] serializedHeader = LogSegmentDataHeader.serialize(header);
            fsOut.write(serializedHeader, 0, serializedHeader.length);
            uploadFile(segmentData.offsetIndex(), fsOut);
            uploadFile(segmentData.timeIndex(), fsOut);
            uploadData(segmentData.leaderEpochIndex(), fsOut);
            uploadFile(segmentData.producerSnapshotIndex(), fsOut);
            if (segmentData.transactionIndex().isPresent()) {
                uploadFile(segmentData.transactionIndex().get(), fsOut);
            }
            uploadFile(segmentData.logSegment(), fsOut);
            fsOut.flush();
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to copy log segment to remote storage", e);
        } finally {
            openOutputStreamCount.decrementAndGet();
        }
        return Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(bucket.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata,
                                       int startPosition) throws RemoteStorageException {
        return fetchSegmentData(metadata, DEFAULT_READ_CONTEXT, startPosition, Integer.MAX_VALUE);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata,
                                       int startPosition,
                                       int endPosition) throws RemoteStorageException {
        return fetchSegmentData(metadata, DEFAULT_READ_CONTEXT, startPosition, endPosition);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata,
                                       RemoteReadContext readContext,
                                       int startPosition) throws RemoteStorageException {
        return fetchSegmentData(metadata, readContext, startPosition, Integer.MAX_VALUE);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata,
                                       RemoteReadContext readContext,
                                       int startPosition,
                                       int endPosition) throws RemoteStorageException {
        return fetchSegmentData(metadata, readContext, startPosition, endPosition);
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata metadata, IndexType indexType) throws RemoteStorageException {
        switch (indexType) {
            case OFFSET:
                return fetchAuxFile(metadata, OFFSET_INDEX);
            case TIMESTAMP:
                return fetchAuxFile(metadata, TIMESTAMP_INDEX);
            case TRANSACTION:
                return fetchAuxFile(metadata, TRANSACTION_INDEX);
            case PRODUCER_SNAPSHOT:
                return fetchAuxFile(metadata, PRODUCER_SNAPSHOT);
            case LEADER_EPOCH:
                return fetchAuxFile(metadata, LEADER_EPOCH_CHECKPOINT);
            default:
                throw new KafkaException("Unknown index type :" + indexType);
        }
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata segmentMetadata) throws RemoteStorageException {
        boolean delete;
        try {
            segmentHeaderHolderCache.invalidate(segmentMetadata.remoteLogSegmentId());
            Path path = new Path(getSegmentRemoteDir(segmentMetadata.remoteLogSegmentId()));
            String bucket = getBucket(segmentMetadata);
            FileSystem fs = getFS(bucket);
            if (fs.exists(path)) {
                delete = fs.delete(path, true);
            } else {
                delete = true;
                LOGGER.warn("Skipping the call to delete log segment data: {} as the segment file doesn't exists",
                        segmentMetadata);
            }
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to delete remote log segment with id:" +
                    segmentMetadata.remoteLogSegmentId(), e);
        }
        if (!delete) {
            throw new RemoteStorageException("Failed to delete remote log segment with id: " +
                    segmentMetadata.remoteLogSegmentId());
        }
    }

    @Override
    public void deletePartition(TopicIdPartition partition,
                                List<RemoteLogSegmentMetadata> metadataList) throws RemoteStorageException {
        // Even-though the exact buckets are known to issue the delete-partition request, sending the request to all
        // buckets to ensure that the *empty* partition directories are also deleted.
        Set<String> allBuckets = getBuckets(metadataList);
        fileSystemByBucket.keySet().forEach(key -> allBuckets.add(key.bucket));
        boolean status = false;
        try {
            Path path = new Path(getPartitionRemoteDir(partition));
            for (String bucket : allBuckets) {
                FileSystem fs = getFS(bucket);
                if (fs.exists(path)) {
                    status = fs.delete(path, true);
                    if (status) {
                        LOGGER.info("Remote logs are deleted for {} partition. Bucket: {}", partition, bucket);
                    }
                }
            }
            if (!status) {
                LOGGER.warn("Skipping the call to delete partition: {} as the folder doesn't exists", partition);
            }
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to delete remote log partition:" + partition, e);
        }
    }

    @Override
    public void close() {
        fileSystemByBucket.forEach((fileSystemKey, fs) -> {
                if (fileSystemKey.bucket.equals(hdfsBucket)) {
                    Utils.closeQuietly(fs, "Closed FileSystem for bucket: " + fileSystemKey.bucket + " with hedged reads enabled: " + fileSystemKey.hedgedReadsEnabled);
                } else {
                    Utils.closeQuietly(fs, "Closed FileSystem for bucket: " + fileSystemKey.bucket);
                }
            }
        );
        ThreadUtils.shutdownExecutorServiceQuietly(executor, 5, TimeUnit.SECONDS);
    }

    void setLRUCache(final LRUCache cache) {
        this.readCache = cache;
    }

    void setDefaultHadoopConfiguration(final Configuration configuration) {
        this.defaultHadoopConf = configuration;
    }

    private void uploadFile(final java.nio.file.Path localSrc,
                            final FSDataOutputStream out) throws IOException {
        if (localSrc != null && localSrc.toFile().exists()) {
            final int bufferSize = defaultHadoopConf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
                    CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
            final byte[] buf = new byte[bufferSize];
            try (final FileInputStream fis = new FileInputStream(localSrc.toFile())) {
                int bytesRead = fis.read(buf);
                while (bytesRead >= 0) {
                    out.write(buf, 0, bytesRead);
                    bytesRead = fis.read(buf);
                }
            }
        }
    }

    private void uploadData(final ByteBuffer localSrc,
                            final FSDataOutputStream out) throws IOException {
        if (localSrc != null) {
            final int bufferSize = defaultHadoopConf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
                                                     CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_DEFAULT);

            final byte[] buf = new byte[bufferSize];
            try (final ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(localSrc)) {
                int bytesRead = byteBufferInputStream.read(buf);
                while (bytesRead >= 0) {
                    out.write(buf, 0, bytesRead);
                    bytesRead = byteBufferInputStream.read(buf);
                }
            }
        }
    }

    private InputStream fetchAuxFile(RemoteLogSegmentMetadata metadata,
                                     LogSegmentDataHeader.FileType fileType) throws RemoteStorageException {
        try {
            String bucket = getBucket(metadata);
            return new AuxiliaryDataInputStream(metadata.remoteLogSegmentId(), bucket, fileType);
        } catch (Exception e) {
            throw new RemoteStorageException(
                    String.format("Failed to fetch %s file from remote storage. Metadata: %s", fileType, metadata), e);
        }
    }

    private InputStream fetchSegmentData(RemoteLogSegmentMetadata metadata,
                                         RemoteReadContext readContext,
                                         int startPosition,
                                         int endPosition) throws RemoteStorageException {
        try {
            String bucket = getBucket(metadata);
            if (readContext.isPrefetchEnabled()) {
                boolean enableHedgedReads = readContext.isHedgedReadsEnabled();
                return new CachedInputStream(metadata.remoteLogSegmentId(), bucket, startPosition, endPosition, enableHedgedReads);
            } else {
                return new SimpleInputStream(metadata.remoteLogSegmentId(), bucket, startPosition, endPosition);
            }
        } catch (Exception e) {
            throw new RemoteStorageException(
                    String.format("Failed to fetch SEGMENT file from remote storage. Metadata: %s", metadata), e);
        }
    }

    FileSystem getFS(String bucket) {
        return getFS(bucket, false);
    }

    FileSystem getFS(String bucket, boolean enableHedgedReads) {
        // Only use hedged reads for the HDFS bucket when explicitly enabled
        boolean useHedgedReads = bucket.equals(hdfsBucket) && enableHedgedReads;
        Configuration conf = useHedgedReads ? hedgedReadsHadoopConf : defaultHadoopConf;
        FileSystemKey key = new FileSystemKey(bucket, useHedgedReads);
        return fileSystemByBucket.computeIfAbsent(key, k -> createFileSystem(bucket, conf));
    }

    private FileSystem createFileSystem(String bucket, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(new URI(bucket), conf);
            LOGGER.info("FileSystem created for uri: {}", bucket);
            return fs;
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException("Unable to create file system instance for uri: " + bucket, e);
        }
    }

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

    long bytesReadFromRemote() {
        return auxBytesReadFromRemote.get();
    }

    long segmentFileReadOpenCounter() {
        return metrics.getFileSystemOpenCount();
    }

    String baseDir() {
        return baseDir;
    }

    List<String> ociBuckets() {
        return ociBuckets;
    }

    private String getSegmentRemoteDir(RemoteLogSegmentId remoteLogSegmentId) {
        return getSegmentRemoteDir(baseDir, remoteLogSegmentId);
    }

    private String getPartitionRemoteDir(TopicIdPartition partition) {
        return getPartitionRemoteDir(baseDir, partition);
    }

    static String getSegmentRemoteDir(final String baseDir, final RemoteLogSegmentId segmentId) {
        return getPartitionRemoteDir(baseDir, segmentId.topicIdPartition()) + Path.SEPARATOR + segmentId.id();
    }

    static String getPartitionRemoteDir(final String baseDir, final TopicIdPartition partition) {
        return baseDir + Path.SEPARATOR + partition.topicPartition() + "-" + partition.topicId();
    }

    /**
     * Auxiliary Data Input Stream is used to fetch the offset-index, time-index, producer-snapshot, leader-epoch-checkpoint,
     * and transaction-index files from the remote storage. This stream reads the data in chunks from the remote storage
     * to reduce the number of remote calls. Note that there is no need to caches these data as the RemoteIndexCache
     * already caches them in disk.
     */
    class AuxiliaryDataInputStream extends InputStream {
        private static final int MAX_AUX_BUFFER_SIZE = 2 * 1024 * 1024; // 2 MB
        private final RemoteLogSegmentId segmentId;
        private final String bucket;
        private final LogSegmentDataHeader.FileType fileType;
        private FSDataInputStream inputStream;
        private final LogSegmentDataHeader.DataPosition dataPosition;
        private final byte[] bufferedData;
        private int position; // current position in the data

        AuxiliaryDataInputStream(RemoteLogSegmentId segmentId,
                                 String bucket,
                                 LogSegmentDataHeader.FileType fileType) throws IOException {
            this.segmentId = segmentId;
            this.bucket = bucket;
            this.fileType = fileType;

            Path dataPath = new Path(getSegmentRemoteDir(segmentId));
            long currentTimeMs = time.milliseconds();
            try {
                inputStream = getFS(bucket).open(dataPath);
                openInputStreamCount.incrementAndGet();
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Opened file stream for {} in {} ms", getString(segmentId), time.milliseconds() - currentTimeMs);
                }
                SegmentHeaderHolder headerHolder = segmentHeaderHolderCache.getIfPresent(segmentId);
                if (headerHolder == null) {
                    headerHolder = fetchSegmentHeaderHolder(dataPath);
                    segmentHeaderHolderCache.put(segmentId, headerHolder);
                }
                dataPosition = headerHolder.header().getDataPosition(fileType);
                if (headerHolder.fileLength() < dataPosition.getPos() + dataPosition.getLength()) {
                    throw new IOException(String.format("File length: %d is less than the expected length: %d for %s file.",
                            headerHolder.fileLength(), dataPosition.getPos() + dataPosition.getLength(), getString(segmentId)));
                }
                bufferedData = new byte[Math.min(MAX_AUX_BUFFER_SIZE, dataPosition.getLength())];
                inputStream.seek(dataPosition.getPos());
            } catch (Exception e) {
                if (inputStream != null) {
                    Utils.closeAll(inputStream);
                    inputStream = null;
                    openInputStreamCount.decrementAndGet();
                }
                throw new IOException(String.format("Failed to open file stream for %s", getString(segmentId)), e);
            }
        }

        private SegmentHeaderHolder fetchSegmentHeaderHolder(Path dataPath) throws IOException {
            // Sends a remote fetch to read the file header.
            long currentTimeMs = time.milliseconds();
            byte[] buffer = new byte[LogSegmentDataHeader.LENGTH];
            inputStream.readFully(0, buffer);
            LogSegmentDataHeader header = LogSegmentDataHeader.deserialize(ByteBuffer.wrap(buffer));
            long actualFileLength = getFS(bucket).getFileStatus(dataPath).getLen();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Time taken to fetch header for {} in {} ms",
                        getString(segmentId), time.milliseconds() - currentTimeMs);
            }
            return new SegmentHeaderHolder(header, actualFileLength);
        }

        @Override
        public int read() throws IOException {
            if (position >= dataPosition.getLength()) {
                return -1;
            }
            if (position % MAX_AUX_BUFFER_SIZE == 0) {
                long currentTimeMs = time.milliseconds();
                int readLen = Math.min(MAX_AUX_BUFFER_SIZE, dataPosition.getLength() - position);
                inputStream.readFully(bufferedData, 0, readLen);
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Time taken to fetch {} bytes from {} {} in {} ms", readLen, getString(segmentId),
                            fileType.toString().toLowerCase(Locale.ROOT), time.milliseconds() - currentTimeMs);
                }
                auxBytesReadFromRemote.addAndGet(readLen);
            }
            return bufferedData[position++ % MAX_AUX_BUFFER_SIZE] & 0xFF;
        }

        @Override
        public int available() {
            return dataPosition.getLength() - position;
        }

        @Override
        public void close() throws IOException {
            if (inputStream != null) {
                Utils.closeAll(inputStream);
                inputStream = null;
                openInputStreamCount.decrementAndGet();
            }
        }
    }

    private class CachedInputStream extends InputStream {
        private final RemoteLogSegmentId segmentId;
        private final String bucket;
        private final boolean enableHedgedReads;
        private final Path dataPath;
        private final LogSegmentDataHeader.DataPosition dataPosition;
        // Represents the length of the segment file that is readable
        private final long readableSegmentLen;
        // realFileLen is the length of both the LogSegmentDataHeader and the Segment file.
        private final long realFileLen;
        // Type of currentPos is kept as `long` to avoid overflow error when the realFileLen is higher than 2 GB.
        private long currentPos;
        private FSDataInputStream inputStream;

        /**
         * Input Stream which caches the SEGMENT data to serve them locally on repeated reads.
         * @param segmentId  remote log segment id
         * @param bucket     bucket name
         * @param currentPos current position to read from the stream, inclusive.
         * @param endPos     to read upto the end position, inclusive.
         * @throws IOException IO problems
         */
        CachedInputStream(RemoteLogSegmentId segmentId,
                          String bucket,
                          int currentPos,
                          int endPos,
                          boolean enableHedgedReads) throws IOException {
            this.segmentId = segmentId;
            this.bucket = bucket;
            this.enableHedgedReads = enableHedgedReads;
            this.dataPath = new Path(getSegmentRemoteDir(segmentId));
            try {
                SegmentHeaderHolder headerHolder = segmentHeaderHolderCache.getIfPresent(segmentId);
                if (headerHolder == null) {
                    openFileStream();
                    headerHolder = fetchSegmentHeaderHolder();
                    segmentHeaderHolderCache.put(segmentId, headerHolder);
                }
                this.dataPosition = headerHolder.header().getDataPosition(SEGMENT);
                this.currentPos = currentPos;
                this.realFileLen = headerHolder.fileLength();

                if (endPos == Integer.MAX_VALUE) {
                    readableSegmentLen = realFileLen - dataPosition.getPos();
                } else {
                    // Note that the endPos is inclusive.
                    readableSegmentLen = Math.min(endPos + 1, realFileLen - dataPosition.getPos());
                }
            } catch (Exception e) {
                if (inputStream != null) {
                    Utils.closeAll(inputStream);
                    inputStream = null;
                    openInputStreamCount.decrementAndGet();
                }
                throw new IOException(String.format("Failed to open file stream for %s", getString(segmentId)), e);
            }
        }

        private void openFileStream() throws IOException {
            long currentTimeMs = time.milliseconds();
            FileSystem fileSystem = getFS(bucket, enableHedgedReads);
            metrics.timeFileSystemOpen(() -> inputStream = fileSystem.open(dataPath));
            openInputStreamCount.incrementAndGet();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Opened file stream for segment {} in {} ms (hedged reads enabled: {})", getString(segmentId),
                        time.milliseconds() - currentTimeMs, enableHedgedReads);
            }
        }

        private SegmentHeaderHolder fetchSegmentHeaderHolder() throws IOException {
            // Sends a remote fetch to read the file header.
            long currentTimeMs = time.milliseconds();
            byte[] buffer = new byte[LogSegmentDataHeader.LENGTH];
            metrics.timeSegmentHeaderRead(() -> inputStream.readFully(0, buffer));
            LogSegmentDataHeader header = LogSegmentDataHeader.deserialize(ByteBuffer.wrap(buffer));

            FileSystem fileSystem = getFS(bucket);
            FileStatus[] fileStatusHolder = new FileStatus[1];
            metrics.timeFileSystemStatus(() -> fileStatusHolder[0] = fileSystem.getFileStatus(dataPath));
            long actualFileLength = fileStatusHolder[0].getLen();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Time taken to fetch header for {} in {} ms",
                        getString(segmentId), time.milliseconds() - currentTimeMs);
            }
            return new SegmentHeaderHolder(header, actualFileLength);
        }

        private <T> T getCachedDataAndApply(long position, Function<ByteBuffer, T> func) throws IOException {
            ByteBufferWrapper wrapper = null;
            try {
                wrapper = getCachedData(position);
                return func.apply(wrapper.getByteBuffer());
            } finally {
                if (wrapper != null) {
                    wrapper.release();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Released ByteBufferWrapper for {} at position {}", getString(segmentId), position);
                    }
                }
            }
        }

        @Override
        public int read() throws IOException {
            if (currentPos >= readableSegmentLen)
                return -1;

            return getCachedDataAndApply(currentPos,
                byteBuffer -> byteBuffer.get((int) ((currentPos++) % cacheLineSize)) & 0xFF);
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            AtomicInteger pos = new AtomicInteger();
            if (len > readableSegmentLen - currentPos)
                len = (int) (readableSegmentLen - currentPos);

            if (len <= 0)
                return -1;

            int finalLen = len;
            while (pos.get() < len) {
                getCachedDataAndApply(currentPos + pos.get(), byteBuffer -> {
                    int srcPos = (int) ((currentPos + pos.get()) % cacheLineSize);
                    int length = Math.min(finalLen - pos.get(), byteBuffer.remaining() - srcPos);

                    // Read the bytes into the destination buffer.
                    byteBuffer.position(srcPos);
                    byteBuffer.get(buf, pos.get() + off, length);

                    pos.addAndGet(length);
                    return null;
                });
            }
            currentPos += pos.get();
            return pos.get();
        }

        @Override
        public int available() {
            long available = readableSegmentLen - currentPos;
            if (available > Integer.MAX_VALUE)
                return Integer.MAX_VALUE;

            return (int) available;
        }

        /**
         * Fetches the data from the cache or reads from the remote storage and caches the data.
         * Callers must release the returned ByteBufferWrapper after using it or else it will lead to memory leaks.
         * They can also instead use getCachedDataAndApply() which automatically releases the ByteBufferWrapper.
         */
        private ByteBufferWrapper getCachedData(long position) throws IOException {
            // Discarding the bytes before the `dataPosition.getPos` to maintain better cache hit ratio.
            // Each file comprised of offset-index, time-index, leader-epoch-checkpoint, producer-snapshot, and
            // transaction-index. Discarding the bytes before the `dataPosition.getPos` will help to cache only
            // the segment data and we can choose `cacheLineSize` to be inline with the `max.partition.fetch.bytes`
            // config.
            long actualPosition = ((position / cacheLineSize) * cacheLineSize) + dataPosition.getPos();

            ByteBufferWrapper wrapper = readCache.get(dataPath.toString(), actualPosition);
            if (wrapper != null) {
                return wrapper;
            } else if (position + dataPosition.getPos() != actualPosition) {
                // When the data is not present in the cache:
                // 1. If the requested position doesn't match with actual-position, then the previously fetched data
                //    was thrashed.
                // 2. If the requested position matches with the actual-position, then the cache didn't fetch the data
                //    previously.
                // 3. There can be few false-positive cache thrash hits, this happens only for the first FETCH request
                //    from the consumer where the `fetchOffset` does not match with the actual-position.
                //    This small error rate should be OK.
                //
                // Note that the lookup happens for the previous entry in the cache due to the below reason:
                // 1. offset-index is used to find the file-position for a given offset. Assume that the offset-index
                //    is in the format of (offset, position): {{0, 0}, {5, 50}, {10, 1000}, {30, 4000}, {60, 8000}}
                // 2. offset-index is a sparse-index and does not have entries for all the offsets. It returns the
                //    file-position of the previous entry. (eg)
                //      a) Assume that the consumer read the data from offset 0-39 and it's corresponding file-position
                //         is 0-5000 in the first FETCH request.
                //      b) In the subsequent/next FETCH request, when the consumer asks for data from fetch-offset: 40.
                //      c) The offset index might return file-position: 4000 for offset: 40, the data from
                //         file-position: 4000-5000 was already read/processed by the consumer in the previous FETCH request.
                //      d) To serve the data from file-position: 4000, we do two fetches:
                //          a) 1st fetch: 0-5000 (already cached but got thrashed, so re-fetch from HDFS)
                //          b) 2nd fetch: 5000-10000 (not cached, so fetch from HDFS)
                //      e) This is aggravated by the fact that the consumer rotates the partition in the FETCH request,
                //         so if the consumer is reading for 50 partitions, and few partition leaders are co-located
                //         in the same broker. Assume 4/50 partition leaders are co-located in the same broker then the
                //         next FETCH for the same partition will happen in the 5th FETCH request. By that time, the
                //         previous entry stored in the cache might get evicted.
                //
                // See: https://docs.google.com/document/d/1ztTbLo0GVpOCq35oMLJQLyOHV2ZKo2Ny_vUg8EaE-6I
                metrics.markCacheThrashing();
            }

            if (inputStream == null) {
                openFileStream();
            }

            long currentTimeMs = time.milliseconds();
            long dataLength = Math.min(cacheLineSize, realFileLen - actualPosition);

            // Borrow a buffer from the pooled allocator of cacheLineSize although the actual data length may be lesser
            // in some cases - e.g. when we are reading the end of the segment file
            wrapper = byteBufferPool.acquire().retain();

            ByteBuffer byteBuffer = wrapper.getByteBuffer();
            metrics.timeSegmentRead(() -> inputStream.readFully(actualPosition, byteBuffer.array(), byteBuffer.arrayOffset(), (int) dataLength));
            // Explicitly set the position to 0 since we wrote to the buffer from the beginning.
            byteBuffer.position(0);
            // We have to explicitly set the limit to the dataLength as the buffer is borrowed from the pool. The
            // readFully operation will not set it since it has no knowledge of the buffer, it is just using the
            // supplied byte array.
            byteBuffer.limit((int) dataLength);

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Time taken to fetch {} bytes from {} segment in {} ms",
                        dataLength, getString(segmentId), time.milliseconds() - currentTimeMs);
            }
            readCache.put(dataPath.toString(), actualPosition, wrapper.duplicate());
            return wrapper;
        }

        @Override
        public void close() throws IOException {
            if (inputStream != null) {
                Utils.closeAll(inputStream);
                inputStream = null;
                openInputStreamCount.decrementAndGet();
            }
        }
    }

    private static String getString(RemoteLogSegmentId segmentId) {
        if (segmentId != null) {
            TopicPartition tp = segmentId.topicIdPartition().topicPartition();
            return tp + "-" + segmentId.topicIdPartition().topicId() + "/" + segmentId.id();
        }
        return null;
    }

    private class SimpleInputStream extends InputStream {
        private final RemoteLogSegmentId segmentId;
        private final String bucket;
        private final Path dataPath;
        // Represents the length of the segment file that is readable
        private final long readableSegmentLen;
        // Datatype of `position` is kept as Long to avoid overflow error when the realFileLen is higher than 2 GB.
        private long position;
        private FSDataInputStream inputStream;

        private ByteBufferWrapper bufferWrapper = byteBufferPool.acquire().retain();
        private byte[] cache;
        private int cacheIndex = 0;
        private int cacheLimit = 0;
        private boolean cacheLoaded = false;

        /**
         * Input Stream that caches the {@link HDFSRemoteStorageManager#cacheLineSize} amount of data on the first byte
         * read. It serves the initial data from cached data and then reads from the underlying input stream.
         * The first cache-line size of data gets cached so that the reader can skip the initial RecordBatch's and
         * read the actual data. This is required due to the offset-index which is sparse and does not contain entry
         * for all the offsets.
         *
         * @param segmentId  remote log segment id
         * @param bucket     bucket name
         * @param startPos   starting position to read from the stream, inclusive.
         * @param endPos     to read upto the end position, inclusive.
         * @throws IOException IO problems
         */
        SimpleInputStream(RemoteLogSegmentId segmentId,
                          String bucket,
                          int startPos,
                          int endPos) throws IOException {
            this.segmentId = segmentId;
            this.bucket = bucket;
            this.dataPath = new Path(getSegmentRemoteDir(segmentId));
            try {
                SegmentHeaderHolder headerHolder = segmentHeaderHolderCache.getIfPresent(segmentId);
                openFileStream();
                if (headerHolder == null) {
                    headerHolder = fetchSegmentHeaderHolder();
                    segmentHeaderHolderCache.put(segmentId, headerHolder);
                }
                LogSegmentDataHeader.DataPosition dataPosition = headerHolder.header().getDataPosition(SEGMENT);
                // realFileLen is the length of both the LogSegmentDataHeader and the Segment file.
                long realFileLen = headerHolder.fileLength();

                long validSegmentLen = realFileLen - dataPosition.getPos();
                if (endPos != Integer.MAX_VALUE) {
                    // Note that the endPos is inclusive.
                    validSegmentLen = Math.min(endPos + 1, validSegmentLen);
                }
                readableSegmentLen = Math.max(0, validSegmentLen - startPos);
                inputStream.seek(dataPosition.getPos() + startPos);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("SimpleInputStream started with segmentId: {}, startPos: {}, endPos: {}, " +
                                    "readableSegmentLen: {}, realFileLen: {}, cacheLineSize: {}",
                            getString(segmentId), startPos, endPos, readableSegmentLen, realFileLen, cacheLineSize);
                }
            } catch (Exception e) {
                if (inputStream != null) {
                    Utils.closeAll(inputStream);
                    inputStream = null;
                    openInputStreamCount.decrementAndGet();
                }
                throw new IOException(String.format("Failed to open file stream for %s", getString(segmentId)), e);
            }
        }

        @Override
        public int read() throws IOException {
            if (position >= readableSegmentLen)
                return -1;

            // On first read, load cache
            if (!cacheLoaded) {
                loadCache();
            }

            // Serve from cache first
            position++;
            if (cacheIndex < cacheLimit) {
                return cache[cacheIndex++] & 0xFF;
            }

            // Then fallback to source
            return inputStream.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len > readableSegmentLen - position) {
                len = (int) (readableSegmentLen - position);
            }

            if (len <= 0)
                return -1;

            if (!cacheLoaded) {
                loadCache();
            }

            int bytesRead = 0;
            // Read from cache if any left
            while (cacheIndex < cacheLimit && len > 0) {
                b[off++] = cache[cacheIndex++];
                len--;
                bytesRead++;
            }

            // If cache exhausted, read from underlying stream
            if (len > 0) {
                inputStream.readFully(b, off, len);
                bytesRead += len;
            }
            position += bytesRead;
            return bytesRead == 0 ? -1 : bytesRead;
        }

        @Override
        public int available() {
            long available = readableSegmentLen - position;
            if (available > Integer.MAX_VALUE)
                return Integer.MAX_VALUE;

            return (int) available;
        }

        @Override
        public void close() throws IOException {
            if (inputStream != null) {
                Utils.closeAll(inputStream);
                inputStream = null;
                openInputStreamCount.decrementAndGet();
            }
            if (bufferWrapper != null) {
                bufferWrapper.release();
                bufferWrapper = null;
            }
        }

        private void loadCache() throws IOException {
            cache = bufferWrapper.getByteBuffer().array();
            cacheLimit = 0;
            int total = 0;
            int readLen = Math.min(cacheLineSize, (int) (readableSegmentLen - position));
            while (total < readLen) {
                inputStream.readFully(cache, 0, readLen);
                total += readLen;
            }
            cacheLimit = total;
            cacheLoaded = true;
        }

        private void openFileStream() throws IOException {
            long currentTimeMs = time.milliseconds();
            FileSystem fileSystem = getFS(bucket);
            metrics.timeFileSystemOpen(() -> inputStream = fileSystem.open(dataPath));
            openInputStreamCount.incrementAndGet();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Opened file stream for {} in {} ms", getString(segmentId), time.milliseconds() - currentTimeMs);
            }
        }

        private SegmentHeaderHolder fetchSegmentHeaderHolder() throws IOException {
            // Sends a remote fetch to read the file header.
            long currentTimeMs = time.milliseconds();
            byte[] buffer = new byte[LogSegmentDataHeader.LENGTH];
            metrics.timeSegmentHeaderRead(() -> inputStream.readFully(0, buffer));
            LogSegmentDataHeader header = LogSegmentDataHeader.deserialize(ByteBuffer.wrap(buffer));

            FileSystem fileSystem = getFS(bucket);
            FileStatus[] fileStatusHolder = new FileStatus[1];
            metrics.timeFileSystemStatus(() -> fileStatusHolder[0] = fileSystem.getFileStatus(dataPath));
            long actualFileLength = fileStatusHolder[0].getLen();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Time taken to fetch header for {} in {} ms", getString(segmentId), time.milliseconds() - currentTimeMs);
            }
            return new SegmentHeaderHolder(header, actualFileLength);
        }
    }

    static class FileSystemKey {
        final String bucket;
        final boolean hedgedReadsEnabled;

        public FileSystemKey(String bucket, boolean hedgedReadsEnabled) {
            this.bucket = bucket;
            this.hedgedReadsEnabled = hedgedReadsEnabled;
        }

        @Override
        public String toString() {
            return "FileSystemKey{" +
                    "bucket='" + bucket + '\'' +
                    ", hedgedReadsEnabled=" + hedgedReadsEnabled +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileSystemKey that = (FileSystemKey) o;
            return hedgedReadsEnabled == that.hedgedReadsEnabled && Objects.equals(bucket, that.bucket);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucket, hedgedReadsEnabled);
        }
    }
}
