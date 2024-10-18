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
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DEFAULT_FS_URI_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_KEYTAB_PATH_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP;
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
    private final AtomicLong auxBytesReadFromRemote = new AtomicLong(0);
    private final AtomicInteger segmentFileReadOpenCounter = new AtomicInteger(0);
    private String baseDir;
    private Configuration hadoopConf;
    private int cacheLineSize;
    private LRUCache readCache;
    private final ThreadLocal<FileSystem> fs = new ThreadLocal<>();
    private final Time time = Time.SYSTEM;
    private final Cache<RemoteLogSegmentId, SegmentHeaderHolder> segmentHeaderHolderCache =
            Caffeine.newBuilder()
                    .maximumSize(20_000)
                    .expireAfterWrite(Duration.ofMinutes(10))
                    .build();
    private Meter cacheThrashMeter;

    public HDFSRemoteStorageManager() {
    }

    /**
     * Initialize this instance with the given configs
     *
     * @param configs Key-Value pairs of configuration parameters
     */
    @Override
    public void configure(Map<String, ?> configs) {
        HDFSRemoteStorageManagerConfig conf = new HDFSRemoteStorageManagerConfig(configs, true);

        baseDir = conf.getString(HDFS_BASE_DIR_PROP);
        cacheLineSize = conf.getInt(HDFS_REMOTE_READ_BYTES_PROP);
        long cacheSize = conf.getLong(HDFS_REMOTE_READ_CACHE_BYTES_PROP);
        String defaultFsUri = conf.getString(HDFS_DEFAULT_FS_URI_PROP);
        if (cacheSize < cacheLineSize) {
            throw new IllegalArgumentException(String.format("%s is larger than %s", HDFS_REMOTE_READ_BYTES_PROP, HDFS_REMOTE_READ_CACHE_BYTES_PROP));
        }
        readCache = new LRUCache(cacheSize);

        if (hadoopConf == null) {
            // Loads configuration from hadoop configuration files in class path
            hadoopConf = new Configuration();
        }
        if (defaultFsUri != null && !defaultFsUri.trim().isEmpty()) {
            hadoopConf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, defaultFsUri.trim());
        }
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
        LOGGER.info("HDFSRemoteStorageManager is configured with baseDir: {}, cacheLineSize: {}, cacheSize: {}, " +
                        "defaultFsUri: {}", baseDir, cacheLineSize, cacheSize, defaultFsUri);
    }

    @VisibleForTesting
    void registerMetrics(LRUCache cache) {
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("requestCount"), new Gauge<Long>() {
            @Override
            public Long value() {
                return cache.stats().getRequestCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("hitCount"), new Gauge<Long>() {
            @Override
            public Long value() {
                return cache.stats().getHitCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("hitRate"), new Gauge<Double>() {
            @Override
            public Double value() {
                return cache.stats().getHitRate();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("missCount"), new Gauge<Long>() {
            @Override
            public Long value() {
                return cache.stats().getMissCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("missRate"), new Gauge<Double>() {
            @Override
            public Double value() {
                return cache.stats().getMissRate();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("loadCount"), new Gauge<Long>() {
            @Override
            public Long value() {
                return cache.stats().getLoadCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("evictionCount"), new Gauge<Long>() {
            @Override
            public Long value() {
                return cache.stats().getEvictionCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("size"), new Gauge<Integer>() {
            @Override
            public Integer value() {
                return cache.stats().getSize();
            }
        });
        cacheThrashMeter = KafkaYammerMetrics.defaultRegistry().newMeter(
                metricName("HDFSCacheThrashRequestPerSec"), "requests", TimeUnit.SECONDS);
    }

    private MetricName metricName(String name) {
        Class<? extends HDFSRemoteStorageManager> klass = this.getClass();
        String group = klass.getPackage() == null ? "" : klass.getPackage().getName();
        String typeName = klass.getSimpleName().replaceAll("\\$$", "");
        return new MetricName(group, typeName, name, null, group + ":type=" + typeName + ",name=" + name);
    }

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata metadata, LogSegmentData segmentData) throws RemoteStorageException {
        try {
            final Path dirPath = new Path(getSegmentRemoteDir(metadata.remoteLogSegmentId()));
            final FSDataOutputStream fsOut = getFS().create(dirPath);

            final LogSegmentDataHeader header = LogSegmentDataHeader.create(segmentData);
            byte[] serializedHeader = LogSegmentDataHeader.serialize(header);
            fsOut.write(serializedHeader, 0, serializedHeader.length);
            uploadFile(segmentData.offsetIndex(), fsOut, false);
            uploadFile(segmentData.timeIndex(), fsOut, false);
            uploadData(segmentData.leaderEpochIndex(), fsOut, false);
            uploadFile(segmentData.producerSnapshotIndex(), fsOut, false);
            if (segmentData.transactionIndex().isPresent()) {
                uploadFile(segmentData.transactionIndex().get(), fsOut, false);
            }
            uploadFile(segmentData.logSegment(), fsOut, true);
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to copy log segment to remote storage", e);
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

    private void uploadFile(final java.nio.file.Path localSrc,
                            final FSDataOutputStream out,
                            final boolean closeStream) throws IOException {
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
            if (closeStream && out != null) {
                out.flush();
                Utils.closeAll(out);
            }
        }
    }

    private void uploadData(final ByteBuffer localSrc,
                            final FSDataOutputStream out,
                            final boolean closeStream) throws IOException {
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
            if (closeStream && out != null) {
                out.flush();
                Utils.closeAll(out);
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
        return segmentFileReadOpenCounter.get();
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
                Utils.closeAll(inputStream);
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
            Utils.closeAll(inputStream);
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
                Utils.closeAll(inputStream);
                throw new IOException(String.format("Failed to open file stream for %s", getString(segmentId)), e);
            }
        }

        private void openFileStream() throws IOException {
            long currentTimeMs = time.milliseconds();
            inputStream = getFS().open(dataPath);
            segmentFileReadOpenCounter.incrementAndGet();
            LOGGER.trace("Opened file stream for {} in {} ms", getString(segmentId), time.milliseconds() - currentTimeMs);
        }

        private SegmentHeaderHolder fetchSegmentHeaderHolder() throws IOException {
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
            if (currentPos >= readableSegmentLen)
                return -1;
            byte[] data = getCachedData(currentPos);
            return data[(int) ((currentPos++) % cacheLineSize)] & 0xFF;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            int pos = 0;
            if (len > readableSegmentLen - currentPos)
                len = (int) (readableSegmentLen - currentPos);

            if (len <= 0)
                return -1;

            while (pos < len) {
                byte[] data = getCachedData(currentPos + pos);
                int srcPos = (int) ((currentPos + pos) % cacheLineSize);
                int length = Math.min(len - pos, data.length - srcPos);
                System.arraycopy(data, srcPos, buf, pos + off, length);
                pos += length;
            }
            currentPos += pos;
            return pos;
        }

        @Override
        public int available() {
            long available = readableSegmentLen - currentPos;
            if (available > Integer.MAX_VALUE)
                return Integer.MAX_VALUE;

            return (int) available;
        }

        private byte[] getCachedData(long position) throws IOException {
            // Discarding the bytes before the `dataPosition.getPos` to maintain better cache hit ratio.
            // Each file comprised of offset-index, time-index, leader-epoch-checkpoint, producer-snapshot, and
            // transaction-index. Discarding the bytes before the `dataPosition.getPos` will help to cache only
            // the segment data and we can choose `cacheLineSize` to be inline with the `max.partition.fetch.bytes`
            // config.
            long actualPosition = ((position / cacheLineSize) * cacheLineSize) + dataPosition.getPos();

            byte[] data = readCache.get(dataPath.toString(), actualPosition);
            if (data != null) {
                return data;
            } else if (position + dataPosition.getPos() != actualPosition) {
                // When the data is not present in the cache:
                // 1. If the requested position doesn't match with actual-position, then the previously fetched data
                //    was thrashed.
                // 2. If the requested position matches with the actual-position, then the cache didn't fetch the data
                //    previously.
                // 3. There can be few false-positive cache thrash hits, this happens only for the first FETCH request
                //    from the consumer where the `fetchOffset` does not match with the actual-position.
                //    This small error rate should be OK.
                cacheThrashMeter.mark();
            }

            if (inputStream == null) {
                openFileStream();
            }

            long currentTimeMs = time.milliseconds();
            long dataLength = Math.min(cacheLineSize, realFileLen - actualPosition);
            data = new byte[(int) dataLength];
            inputStream.readFully(actualPosition, data);
            LOGGER.trace("Time taken to fetch {} bytes from {} segment in {} ms",
                    dataLength, getString(segmentId), time.milliseconds() - currentTimeMs);
            readCache.put(dataPath.toString(), actualPosition, data);
            return data;
        }

        @Override
        public void close() throws IOException {
            Utils.closeAll(inputStream);
            inputStream = null;
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
