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
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.rsm.hdfs.generated.ConnectorCustomMetadata;
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
import org.apache.kafka.server.log.remote.storage.RetriableRemoteStorageException;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.core.IntervalFunction;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.ALLOWED_CIRCUIT_BREAKER_VALUES;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_COPY_CIRCUIT_BREAKER_STATE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DELETE_CIRCUIT_BREAKER_STATE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_BUFFER_POOL_MAX_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_BYTES_PROP;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.LEADER_EPOCH_CHECKPOINT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.OFFSET_INDEX;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.SEGMENT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.TIMESTAMP_INDEX;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.TRANSACTION_INDEX;
import static org.apache.kafka.rsm.hdfs.RSMUtils.KLOAK_USER;

public class HDFSRemoteStorageManager implements RemoteStorageManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRemoteStorageManager.class);
    private static final RemoteReadContext DEFAULT_READ_CONTEXT = RemoteReadContext.builder()
            .withBlockPrefetchEnabled(true)
            .build();
    private static final int ERROR_BACKOFF_EXP_BASE = 2;
    private static final double ERROR_BACKOFF_JITTER = 0.2;

    private final AtomicLong auxBytesReadFromRemote = new AtomicLong(0);
    private String baseDir;
    private int cacheLineSize;
    private LRUCache readCache;
    private ByteBufferPool byteBufferPool;
    private Time time = Time.SYSTEM;
    private final Cache<RemoteLogSegmentId, SegmentHeaderHolder> segmentHeaderHolderCache =
            Caffeine.newBuilder()
                    .maximumSize(20_000)
                    .expireAfterWrite(Duration.ofMinutes(10))
                    .build();
    private final HDFSRemoteStorageManagerMetrics metrics;
    private final AtomicInteger openInputStreamCount = new AtomicInteger();
    private final AtomicInteger openOutputStreamCount = new AtomicInteger();
    private final FileSystemManager fileSystemManager;

    private volatile ExponentialBackoff fetchErrorBackoff;
    private volatile long fetchErrorMaxBackoffWaitMs;
    private final ReadErrorHandler fetchErrorHandler = new ReadErrorHandler(Duration.ofMinutes(5));
    private final CircuitBreaker copyErrorBreaker = CircuitBreaker.of("copy-circuit-breaker", circuitBreakerConfig());
    private final CircuitBreaker deleteErrorBreaker = CircuitBreaker.of("delete-circuit-breaker", circuitBreakerConfig());

    public HDFSRemoteStorageManager() {
        this(new HDFSRemoteStorageManagerMetrics(), new FileSystemManager());
    }

    public HDFSRemoteStorageManager(HDFSRemoteStorageManagerMetrics metrics, FileSystemManager fileSystemManager) {
        this.metrics = metrics;
        this.fileSystemManager = fileSystemManager;
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

        // Configure the FileSystemManager
        fileSystemManager.configure(configs);

        long fetchErrorBackoffWaitMs = conf.getLong(HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP);
        fetchErrorMaxBackoffWaitMs = conf.getLong(HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP);
        fetchErrorBackoff = new ExponentialBackoff(fetchErrorBackoffWaitMs, ERROR_BACKOFF_EXP_BASE, fetchErrorMaxBackoffWaitMs, ERROR_BACKOFF_JITTER);

        registerMetrics(readCache);
        registerBufferPoolMetrics();
        registerHedgedReadMetrics();
        registerHDFSReadMetrics();
        registerStreamMetrics();

        LOGGER.info("Configured with baseDir: {}, cacheLineSize: {}, cacheSize: {}, defaultFsUri: {}, " +
                "ociBuckets: {}, fetchErrorBackoffWaitMs: {}, fetchErrorMaxBackoffWaitMs: {}", baseDir, cacheLineSize, cacheSize,
                fileSystemManager.getHdfsBucket(), fileSystemManager.getOciBuckets(), fetchErrorBackoffWaitMs, fetchErrorMaxBackoffWaitMs);
    }


    @Override
    public Set<String> reconfigurableConfigs() {
        Set<String> reconfigurableConfigs = new HashSet<>();
        reconfigurableConfigs.add(HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP);
        reconfigurableConfigs.add(HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP);
        reconfigurableConfigs.add(HDFS_COPY_CIRCUIT_BREAKER_STATE_PROP);
        reconfigurableConfigs.add(HDFS_DELETE_CIRCUIT_BREAKER_STATE_PROP);
        reconfigurableConfigs.addAll(fileSystemManager.reconfigurableConfigs());
        return reconfigurableConfigs;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        fileSystemManager.validateReconfiguration(configs);
        String backoffMs = (String) configs.get(HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP);
        if (backoffMs != null && Long.parseLong(backoffMs) < 0) {
            throw new ConfigException(HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP, backoffMs, "value should be at least 0");
        }
        String maxBackoffMs = (String) configs.get(HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP);
        if (maxBackoffMs != null && Long.parseLong(maxBackoffMs) < 0) {
            throw new ConfigException(HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP, maxBackoffMs, "value should be at least 0");
        }
        for (String breakerProp : Arrays.asList(HDFS_COPY_CIRCUIT_BREAKER_STATE_PROP, HDFS_DELETE_CIRCUIT_BREAKER_STATE_PROP)) {
            String breakerState = (String) configs.get(breakerProp);
            if (breakerState != null && !ALLOWED_CIRCUIT_BREAKER_VALUES.contains(breakerState)) {
                throw new ConfigException(breakerProp, breakerState, "Valid values are: " + ALLOWED_CIRCUIT_BREAKER_VALUES);
            }
        }
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        fileSystemManager.reconfigure(configs);
        reconfigureErrorBackoff(configs);
        reconfigureCircuitBreakerState(configs);
    }

    private void reconfigureErrorBackoff(Map<String, ?> configs) {
        String backoffMs = (String) configs.get(HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP);
        String maxBackoffMs = (String) configs.get(HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP);
        long updatedErrorBackoffWaitMs = backoffMs != null ? Long.parseLong(backoffMs) : fetchErrorBackoffWaitMs();
        long updatedErrorMaxBackoffWaitMs = maxBackoffMs != null ? Long.parseLong(maxBackoffMs) : fetchErrorMaxBackoffWaitMs;
        if (updatedErrorBackoffWaitMs != fetchErrorBackoffWaitMs() || updatedErrorMaxBackoffWaitMs != fetchErrorMaxBackoffWaitMs) {
            fetchErrorMaxBackoffWaitMs = updatedErrorMaxBackoffWaitMs;
            fetchErrorBackoff = new ExponentialBackoff(updatedErrorBackoffWaitMs, ERROR_BACKOFF_EXP_BASE,
                    fetchErrorMaxBackoffWaitMs, ERROR_BACKOFF_JITTER);
            LOGGER.info("Reconfigured with fetchErrorBackoffWaitMs: {}, fetchErrorMaxBackoffWaitMs: {}", updatedErrorBackoffWaitMs, fetchErrorMaxBackoffWaitMs);
        }
    }

    void reconfigureCircuitBreakerState(Map<String, ?> configs) {
        String copyCircuitBreakerState = (String) configs.get(HDFS_COPY_CIRCUIT_BREAKER_STATE_PROP);
        if (copyCircuitBreakerState != null) {
            transitionState(copyErrorBreaker, copyCircuitBreakerState);
        }
        String deleteCircuitBreakerState = (String) configs.get(HDFS_DELETE_CIRCUIT_BREAKER_STATE_PROP);
        if (deleteCircuitBreakerState != null) {
            transitionState(deleteErrorBreaker, deleteCircuitBreakerState);
        }
    }

    private void transitionState(CircuitBreaker breaker, String newState) {
        CircuitBreaker.State state = CircuitBreaker.State.valueOf(newState);
        switch (state) {
            case FORCED_OPEN:
                breaker.transitionToForcedOpenState();
                break;
            case CLOSED:
                breaker.transitionToClosedState();
                break;
            case DISABLED:
                breaker.transitionToDisabledState();
                break;
            default:
                throw new IllegalArgumentException("Invalid state: " + newState);
        }
        LOGGER.info("Transitioned to {} state for breaker: {}", state, breaker.getName());
    }

    FileSystemManager fileSystemManager() {
        return fileSystemManager;
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
            return getFS(fileSystemManager.getHdfsBucket());
        });
    }

    void registerHDFSReadMetrics() {
        metrics.registerHDFSReadMetrics();
    }

    void registerStreamMetrics() {
        metrics.registerStreamMetrics(openInputStreamCount, openOutputStreamCount);
    }

    Set<String> getBuckets(List<RemoteLogSegmentMetadata> metadataList) {
        return metadataList.stream()
                .map(fileSystemManager::getBucket)
                .collect(Collectors.toSet());
    }

    public static RemoteLogSegmentMetadata.CustomMetadata createCustomMetadata(String bucket) {
        ConnectorCustomMetadata connectorCustomMetadata = new ConnectorCustomMetadata();
        connectorCustomMetadata.setUri(bucket);
        ByteBuffer byteBuffer = MessageUtil.toByteBuffer(connectorCustomMetadata, ConnectorCustomMetadata.LOWEST_SUPPORTED_VERSION);
        return new RemoteLogSegmentMetadata.CustomMetadata(byteBuffer.array());
    }

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata metadata, LogSegmentData segmentData) throws RemoteStorageException {
        if (!copyErrorBreaker.tryAcquirePermission()) {
            throw new RetriableRemoteStorageException("Remote copy circuit is open. Skipping the current call");
        }
        final long start = time.milliseconds();
        final RemoteStorageProvider provider = segmentData.storageProvider();
        final String bucket = fileSystemManager.findBucket(provider, metadata.remoteLogSegmentId());
        final Path path = new Path(bucket + getSegmentRemoteDir(metadata.remoteLogSegmentId()));
        metrics.timeSegmentWrite(provider, () -> {
            openOutputStreamCount.incrementAndGet();
            try (final FSDataOutputStream fsOut = getFS(bucket).create(path)) {
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
                copyErrorBreaker.onSuccess(time.milliseconds() - start, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                copyErrorBreaker.onError(time.milliseconds() - start, TimeUnit.MILLISECONDS, e);
                throw new RemoteStorageException("Failed to copy log segment to remote storage", e);
            } finally {
                openOutputStreamCount.decrementAndGet();
            }
        });
        metrics.recordSegmentWriteSize(provider, metadata.segmentSizeInBytes());
        return Optional.of(createCustomMetadata(bucket));
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
        if (!deleteErrorBreaker.tryAcquirePermission()) {
            throw new RetriableRemoteStorageException("Remote deletion circuit is open. Skipping the current call");
        }
        long start = time.milliseconds();
        boolean delete;
        try {
            segmentHeaderHolderCache.invalidate(segmentMetadata.remoteLogSegmentId());
            String bucket = fileSystemManager.getBucket(segmentMetadata);
            Path path = new Path(bucket + getSegmentRemoteDir(segmentMetadata.remoteLogSegmentId()));
            FileSystem fs = getFS(bucket);
            if (fs.exists(path)) {
                delete = fs.delete(path, true);
            } else {
                delete = true;
                LOGGER.warn("Skipping the call to delete log segment data: {} as the segment file doesn't exists",
                        segmentMetadata);
            }
            deleteErrorBreaker.onSuccess(time.milliseconds() - start, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            deleteErrorBreaker.onError(time.milliseconds() - start, TimeUnit.MILLISECONDS, e);
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
        allBuckets.addAll(fileSystemManager.getAllBucketNames());
        boolean status = false;
        try {
            String partitionRemoteDir = getPartitionRemoteDir(partition);
            for (String bucket : allBuckets) {
                FileSystem fs = getFS(bucket);
                Path path = new Path(bucket + partitionRemoteDir);
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
        fileSystemManager.close();
    }

    void setLRUCache(final LRUCache cache) {
        this.readCache = cache;
    }

    void setDefaultHadoopConfiguration(final Configuration configuration) {
        fileSystemManager.setDefaultHadoopConfiguration(configuration);
    }

    void setTime(Time time) {
        this.time = time;
        fetchErrorHandler.setTime(time);
    }

    private void uploadFile(final java.nio.file.Path localSrc,
                            final FSDataOutputStream out) throws IOException {
        if (localSrc != null && localSrc.toFile().exists()) {
            Configuration defaultHadoopConf = fileSystemManager.getDefaultHadoopConf();
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
            Configuration defaultHadoopConf = fileSystemManager.getDefaultHadoopConf();
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
            String bucket = fileSystemManager.getBucket(metadata);
            InputStream stream = new AuxiliaryDataInputStream(metadata.remoteLogSegmentId(), bucket, fileType);
            return new SafeInputStream(stream, fetchErrorHandler);
        } catch (IOException e) {
            fetchErrorHandler.accept(e);
            throw new RemoteStorageException("Failed to fetch " + fileType + " file from remote storage. Metadata: " + metadata, e);
        }
    }

    private InputStream fetchSegmentData(RemoteLogSegmentMetadata metadata,
                                         RemoteReadContext readContext,
                                         int startPosition,
                                         int endPosition) throws RemoteStorageException {
        try {
            String bucket = fileSystemManager.getBucket(metadata);
            RemoteStorageProvider storageProvider = fileSystemManager.getRemoteStorageProvider(bucket);
            boolean isHedgedReadsEnabled = storageProvider == RemoteStorageProvider.HDFS && readContext.isHedgedReadsEnabled();
            InputStream stream;
            if (readContext.isBlockPrefetchEnabled() || isHedgedReadsEnabled) {
                stream = new CachedInputStream(metadata.remoteLogSegmentId(), bucket, storageProvider,
                        startPosition, endPosition, isHedgedReadsEnabled);
            } else {
                stream = new SimpleInputStream(metadata.remoteLogSegmentId(), bucket, storageProvider, startPosition, endPosition);
            }
            return new SafeInputStream(stream, fetchErrorHandler);
        } catch (IOException e) {
            fetchErrorHandler.accept(e);
            throw new RemoteStorageException("Failed to fetch SEGMENT file from remote storage. Metadata: " + metadata, e);
        }
    }

    FileSystem getFS(String bucket) {
        FileSystemOptions options = new FileSystemOptions(bucket);
        return fileSystemManager.getFS(options);
    }

    FileSystem getFS(String bucket, boolean enableHedgedReads) {
        FileSystemOptions options = new FileSystemOptions(bucket, enableHedgedReads);
        return fileSystemManager.getFS(options);
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
        return fileSystemManager.getOciBuckets();
    }

    long fetchErrorBackoffWaitMs() {
        return fetchErrorBackoff.initialInterval();
    }

    long errorMaxBackoffWaitMs() {
        return fetchErrorMaxBackoffWaitMs;
    }

    private String getSegmentRemoteDir(RemoteLogSegmentId remoteLogSegmentId) {
        return RSMUtils.getSegmentRemoteDir(baseDir, remoteLogSegmentId);
    }

    private String getPartitionRemoteDir(TopicIdPartition partition) {
        return RSMUtils.getPartitionRemoteDir(baseDir, partition);
    }

    CircuitBreaker copyErrorBreaker() {
        return copyErrorBreaker;
    }

    CircuitBreaker deleteErrorBreaker() {
        return deleteErrorBreaker;
    }

    private CircuitBreakerConfig circuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .waitIntervalFunctionInOpenState(IntervalFunction.ofRandomized(Duration.ofMinutes(5), 0.8))
                .build();
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

            Path dataPath = new Path(bucket + getSegmentRemoteDir(segmentId));
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

    class CachedInputStream extends InputStream {
        private final RemoteLogSegmentId segmentId;
        private final String bucket;
        private final RemoteStorageProvider storageProvider;
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
         * @param storageProvider remote storage provider
         * @param currentPos current position to read from the stream, inclusive.
         * @param endPos     to read upto the end position, inclusive.
         * @throws IOException IO problems
         */
        CachedInputStream(RemoteLogSegmentId segmentId,
                          String bucket,
                          RemoteStorageProvider storageProvider,
                          int currentPos,
                          int endPos,
                          boolean enableHedgedReads) throws IOException {
            this.segmentId = segmentId;
            this.bucket = bucket;
            this.storageProvider = storageProvider;
            this.enableHedgedReads = enableHedgedReads;
            this.dataPath = new Path(bucket + getSegmentRemoteDir(segmentId));
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
            metrics.timeSegmentHeaderRead(storageProvider, () -> inputStream.readFully(0, buffer));
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
            metrics.timeSegmentRead(storageProvider,
                () -> inputStream.readFully(actualPosition, byteBuffer.array(), byteBuffer.arrayOffset(), (int) dataLength));
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
        private final RemoteStorageProvider storageProvider;
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
         * @param storageProvider remote storage provider
         * @param bucket     bucket name
         * @param startPos   starting position to read from the stream, inclusive.
         * @param endPos     to read upto the end position, inclusive.
         * @throws IOException IO problems
         */
        SimpleInputStream(RemoteLogSegmentId segmentId,
                          String bucket,
                          RemoteStorageProvider storageProvider,
                          int startPos,
                          int endPos) throws IOException {
            this.segmentId = segmentId;
            this.bucket = bucket;
            this.storageProvider = storageProvider;
            this.dataPath = new Path(bucket + getSegmentRemoteDir(segmentId));
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
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("SimpleInputStream started with segmentId: {}, startPos: {}, endPos: {}, " +
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
            return metrics.timeSegmentRead(storageProvider, () -> inputStream.read());
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
                final int finalOff = off;
                final int finalLen = len;
                metrics.timeSegmentRead(storageProvider, () -> inputStream.readFully(b, finalOff, finalLen));
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
            int readLen = Math.min(cacheLineSize, (int) (readableSegmentLen - position));
            metrics.timeSegmentRead(storageProvider, () -> inputStream.readFully(cache, 0, readLen));
            cacheLimit = readLen;
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
            metrics.timeSegmentHeaderRead(storageProvider, () -> inputStream.readFully(0, buffer));
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

    private class ReadErrorHandler implements Consumer<IOException> {
        private final AtomicLong lastErrorAttemptTimestampMs = new AtomicLong(0);
        private final AtomicLong errorAttempts = new AtomicLong(0);
        private final long waitDurationInOpenStateMs;
        private Time time = Time.SYSTEM;

        private ReadErrorHandler(Duration waitDurationInOpenState) {
            this.waitDurationInOpenStateMs = waitDurationInOpenState.toMillis();
        }

        @Override
        public void accept(IOException e) {
            // reset the error attempt counter if there is no error observed in the last waitDurationInOpenStateMs
            if (lastErrorAttemptTimestampMs.get() + waitDurationInOpenStateMs <= time.milliseconds()) {
                errorAttempts.set(0);
            }
            lastErrorAttemptTimestampMs.set(time.milliseconds());
            long backoffMs = fetchErrorBackoff.backoff(errorAttempts.getAndIncrement());
            if (backoffMs > 0) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Error backoff for {} ms", backoffMs);
                }
                time.sleep(backoffMs);
            }
        }

        void setTime(Time time) {
            this.time = time;
        }
    }
}
