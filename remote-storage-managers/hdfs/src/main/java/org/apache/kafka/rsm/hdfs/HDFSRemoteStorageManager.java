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
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.rsm.hdfs.pool.ByteBufferPool;
import org.apache.kafka.rsm.hdfs.pool.ByteBufferPoolImpl;
import org.apache.kafka.rsm.hdfs.pool.ByteBufferWrapper;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DEFAULT_FS_URI_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_KEYTAB_PATH_PROP;
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
    private static final String KLOAK_USER = Path.SEPARATOR + "user" + Path.SEPARATOR + "kloak" + Path.SEPARATOR;
    private static final List<String> ALLOWED_SCHEMES = Arrays.asList("hdfs://", "oci://", "cfs://");

    private final AtomicLong auxBytesReadFromRemote = new AtomicLong(0);
    private String baseDir;
    private Configuration hadoopConf;
    private int cacheLineSize;
    private LRUCache readCache;
    private ByteBufferPool byteBufferPool;
    private final ThreadLocal<FileSystem> fs = new ThreadLocal<>();
    private final Time time = Time.SYSTEM;
    private final Cache<RemoteLogSegmentId, SegmentHeaderHolder> segmentHeaderHolderCache =
            Caffeine.newBuilder()
                    .maximumSize(20_000)
                    .expireAfterWrite(Duration.ofMinutes(10))
                    .build();
    private final HDFSRemoteStorageManagerMetrics metrics;
    private final AtomicInteger openInputStreamCount = new AtomicInteger();
    private final AtomicInteger openOutputStreamCount = new AtomicInteger();

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
        cacheLineSize = conf.getInt(HDFS_REMOTE_READ_BYTES_PROP);
        long cacheSize = conf.getLong(HDFS_REMOTE_READ_CACHE_BYTES_PROP);
        if (cacheSize < cacheLineSize) {
            throw new IllegalArgumentException(String.format("%s is larger than %s", HDFS_REMOTE_READ_BYTES_PROP, HDFS_REMOTE_READ_CACHE_BYTES_PROP));
        }
        readCache = new LRUCache(cacheSize);
        int bufferPoolMaxSize = conf.getInt(HDFS_REMOTE_READ_CACHE_BUFFER_POOL_MAX_SIZE_PROP);
        byteBufferPool = new ByteBufferPoolImpl(cacheLineSize, bufferPoolMaxSize);

        if (hadoopConf == null) {
            // Loads configuration from hadoop configuration files in class path
            hadoopConf = new Configuration();
        }
        String defaultFsUri = getDefaultFsUri(conf);
        hadoopConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, defaultFsUri);
        baseDir = defaultFsUri + KLOAK_USER + conf.getString(HDFS_BASE_DIR_PROP);

        String authentication = hadoopConf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
        if (authentication.equalsIgnoreCase("kerberos")) {
            String user = conf.getString(HDFS_USER_PROP);
            String keytabPath = conf.getString(HDFS_KEYTAB_PATH_PROP);
            try {
                UserGroupInformation.setConfiguration(hadoopConf);
                UserGroupInformation.loginUserFromKeytab(user, keytabPath);
            } catch (final Exception ex) {
                throw new RuntimeException(String.format("Unable to login as user: %s", user), ex);
            }
        }
        registerMetrics(readCache);
        registerBufferPoolMetrics();
        registerHDFSReadMetrics();
        registerStreamMetrics();
        LOGGER.info("HDFSRemoteStorageManager is configured with baseDir: {}, cacheLineSize: {}, cacheSize: {}, " +
                        "defaultFsUri: {}", baseDir, cacheLineSize, cacheSize, defaultFsUri);
    }

    @VisibleForTesting
    void registerMetrics(LRUCache cache) {
        metrics.registerCacheMetrics(cache);
    }

    void registerBufferPoolMetrics() {
        metrics.registerBufferPoolMetrics(byteBufferPool);
    }

    void registerHDFSReadMetrics() {
        metrics.registerHDFSReadMetrics();
    }

    @VisibleForTesting
    void registerStreamMetrics() {
        metrics.registerStreamMetrics(openInputStreamCount, openOutputStreamCount);
    }

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata metadata, LogSegmentData segmentData) throws RemoteStorageException {
        final Path dirPath = new Path(getSegmentRemoteDir(metadata.remoteLogSegmentId()));
        try (final FSDataOutputStream fsOut = getFS().create(dirPath)) {
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
        //DKAFC-4132: Return custom metadata
        return Optional.empty();
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata,
                                       int startPosition) throws RemoteStorageException {
        return fetchSegmentData(metadata, startPosition, Integer.MAX_VALUE);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata metadata,
                                       int startPosition,
                                       int endPosition) throws RemoteStorageException {
        return fetchSegmentData(metadata, startPosition, endPosition);
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
            FileSystem fs = getFS();
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

    // @Override
    public void deletePartition(TopicIdPartition partition) throws RemoteStorageException {
        boolean status;
        try {
            Path path = new Path(getPartitionRemoteDir(partition));
            FileSystem fs = getFS();
            if (fs.exists(path)) {
                status = fs.delete(path, true);
                if (status) {
                    LOGGER.info("Remote logs are deleted for {} partition", partition);
                }
            } else {
                status = true;
                LOGGER.warn("Skipping the call to delete partition: {} as the folder doesn't exists", partition);
            }
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to delete remote log partition:" + partition, e);
        }
        if (!status) {
            throw new RemoteStorageException("Failed to delete remote log partition: " + partition);
        }
    }

    @Override
    public void close() {
        Utils.closeQuietly(fs.get(), "Hadoop file system");
    }

    @VisibleForTesting
    void setLRUCache(final LRUCache cache) {
        this.readCache = cache;
    }

    @VisibleForTesting
    void setHadoopConfiguration(final Configuration configuration) {
        this.hadoopConf = configuration;
    }

    @VisibleForTesting
    static String getDefaultFsUri(HDFSRemoteStorageManagerConfig conf) {
        String defaultFsUri = conf.getString(HDFS_DEFAULT_FS_URI_PROP);
        // NOTE: If defaultFsUri is not set, then it can be taken from the `hadoopConf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY)`
        // But, we want to enforce that the value should be set in the Kafka DSC config.
        if (Utils.isBlank(defaultFsUri)) {
            throw new IllegalArgumentException(String.format("Default file system URI is not set. " +
                    "Please set %s in the configuration", HDFS_DEFAULT_FS_URI_PROP));
        }
        boolean isValidScheme = ALLOWED_SCHEMES.stream().anyMatch(defaultFsUri::startsWith);
        if (!isValidScheme) {
            throw new IllegalArgumentException(String.format("Invalid default file system URI: %s. It should start with %s",
                    defaultFsUri, ALLOWED_SCHEMES));
        }
        defaultFsUri = defaultFsUri.trim();
        if (defaultFsUri.endsWith(Path.SEPARATOR)) {
            defaultFsUri = defaultFsUri.substring(0, defaultFsUri.length() - 1);
        }
        return defaultFsUri;
    }

    private void uploadFile(final java.nio.file.Path localSrc,
                            final FSDataOutputStream out) throws IOException {
        if (localSrc != null && localSrc.toFile().exists()) {
            final int bufferSize = hadoopConf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
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
            final int bufferSize = hadoopConf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY,
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
            return new AuxiliaryDataInputStream(metadata.remoteLogSegmentId(), fileType);
        } catch (Exception e) {
            throw new RemoteStorageException(
                    String.format("Failed to fetch %s file from remote storage. Metadata: %s", fileType, metadata), e);
        }
    }

    private InputStream fetchSegmentData(RemoteLogSegmentMetadata metadata,
                                         int startPosition,
                                         int endPosition) throws RemoteStorageException {
        try {
            return new CachedInputStream(metadata.remoteLogSegmentId(), startPosition, endPosition);
        } catch (Exception e) {
            throw new RemoteStorageException(
                    String.format("Failed to fetch SEGMENT file from remote storage. Metadata: %s", metadata), e);
        }
    }

    @VisibleForTesting
    FileSystem getFS() throws IOException {
        if (fs.get() == null) {
            fs.set(FileSystem.newInstance(hadoopConf));
        }
        return fs.get();
    }

    @VisibleForTesting
    long bytesReadFromRemote() {
        return auxBytesReadFromRemote.get();
    }

    @VisibleForTesting
    long segmentFileReadOpenCounter() {
        return metrics.getFileSystemOpenCount();
    }

    @VisibleForTesting
    String baseDir() {
        return baseDir;
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
        private final LogSegmentDataHeader.FileType fileType;
        private FSDataInputStream inputStream;
        private final LogSegmentDataHeader.DataPosition dataPosition;
        private final byte[] bufferedData;
        private int position; // current position in the data

        AuxiliaryDataInputStream(RemoteLogSegmentId segmentId,
                                 LogSegmentDataHeader.FileType fileType) throws IOException {
            this.segmentId = segmentId;
            this.fileType = fileType;

            Path dataPath = new Path(getSegmentRemoteDir(segmentId));
            long currentTimeMs = time.milliseconds();
            try {
                inputStream = getFS().open(dataPath);
                openInputStreamCount.incrementAndGet();
                LOGGER.trace("Opened file stream for {} in {} ms", getString(segmentId), time.milliseconds() - currentTimeMs);
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
                    openInputStreamCount.decrementAndGet();
                    inputStream = null;
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
            long actualFileLength = getFS().getFileStatus(dataPath).getLen();
            LOGGER.trace("Time taken to fetch header for {} in {} ms", getString(segmentId), time.milliseconds() - currentTimeMs);
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
                LOGGER.trace("Time taken to fetch {} bytes from {} {} in {} ms", readLen, getString(segmentId),
                        fileType.toString().toLowerCase(Locale.ROOT), time.milliseconds() - currentTimeMs);
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
                openInputStreamCount.decrementAndGet();
            }
        }
    }

    private class CachedInputStream extends InputStream {
        private final RemoteLogSegmentId segmentId;
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
         * @param currentPos current position to read from the stream, inclusive.
         * @param endPos     to read upto the end position, inclusive.
         * @throws IOException IO problems
         */
        CachedInputStream(RemoteLogSegmentId segmentId,
                          int currentPos,
                          int endPos) throws IOException {
            this.segmentId = segmentId;
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
                    openInputStreamCount.decrementAndGet();
                    inputStream = null;
                }
                throw new IOException(String.format("Failed to open file stream for %s", getString(segmentId)), e);
            }
        }

        private void openFileStream() throws IOException {
            long currentTimeMs = time.milliseconds();
            FileSystem fileSystem = getFS();
            metrics.timeFileSystemOpen(() -> inputStream = fileSystem.open(dataPath));
            openInputStreamCount.incrementAndGet();
            LOGGER.trace("Opened file stream for {} in {} ms", getString(segmentId), time.milliseconds() - currentTimeMs);
        }

        private SegmentHeaderHolder fetchSegmentHeaderHolder() throws IOException {
            // Sends a remote fetch to read the file header.
            long currentTimeMs = time.milliseconds();
            byte[] buffer = new byte[LogSegmentDataHeader.LENGTH];
            metrics.timeSegmentHeaderRead(() -> inputStream.readFully(0, buffer));
            LogSegmentDataHeader header = LogSegmentDataHeader.deserialize(ByteBuffer.wrap(buffer));

            FileSystem fileSystem = getFS();
            FileStatus[] fileStatusHolder = new FileStatus[1];
            metrics.timeFileSystemStatus(() -> fileStatusHolder[0] = fileSystem.getFileStatus(dataPath));
            long actualFileLength = fileStatusHolder[0].getLen();
            LOGGER.trace("Time taken to fetch header for {} in {} ms", getString(segmentId), time.milliseconds() - currentTimeMs);
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

            LOGGER.trace("Time taken to fetch {} bytes from {} segment in {} ms",
                    dataLength, getString(segmentId), time.milliseconds() - currentTimeMs);
            readCache.put(dataPath.toString(), actualPosition, wrapper.duplicate());
            return wrapper;
        }

        @Override
        public void close() throws IOException {
            if (inputStream != null) {
                Utils.closeAll(inputStream);
                openInputStreamCount.decrementAndGet();
                inputStream = null;
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
}
