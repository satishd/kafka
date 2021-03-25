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


import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.log.remote.storage.LogSegmentData;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.log.remote.storage.RemoteStorageException;
import org.apache.kafka.common.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.common.utils.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.LEADER_EPOCH_CHECKPOINT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.OFFSET_INDEX;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.PRODUCER_SNAPSHOT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.SEGMENT;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.TIMESTAMP_INDEX;
import static org.apache.kafka.rsm.hdfs.LogSegmentDataHeader.FileType.TRANSACTION_INDEX;

public class HDFSRemoteStorageManager implements RemoteStorageManager {

    private URI fsURI;
    private String baseDir;
    private Configuration hadoopConf;
    private int cacheLineSize;
    private LRUCache readCache;
    private final ThreadLocal<FileSystem> fs = new ThreadLocal<>();

    /**
     * Initialize this instance with the given configs
     *
     * @param configs Key-Value pairs of configuration parameters
     */
    @Override
    public void configure(Map<String, ?> configs) {
        HDFSRemoteStorageManagerConfig conf = new HDFSRemoteStorageManagerConfig(configs, true);

        fsURI = URI.create(conf.getString(HDFSRemoteStorageManagerConfig.HDFS_URI_PROP));
        baseDir = conf.getString(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP);
        cacheLineSize = conf.getInt(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_MB_PROP) * 1048576;
        long cacheSize = conf.getInt(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_MB_PROP) * 1048576L;
        if (cacheSize < cacheLineSize) {
            throw new IllegalArgumentException(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_MB_PROP +
                    " is larger than " + HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_MB_PROP);
        }
        readCache = new LRUCache(cacheSize);

        // Loads configuration from hadoop configuration files in class path
        hadoopConf = new Configuration();
    }

    @Override
    public void copyLogSegment(RemoteLogSegmentMetadata metadata, LogSegmentData segmentData) throws RemoteStorageException {
        try {
            final Path dirPath = new Path(getSegmentRemoteDir(metadata.remoteLogSegmentId()));
            final FSDataOutputStream fsOut = getFS().create(dirPath);

            final LogSegmentDataHeader header = LogSegmentDataHeader.create(segmentData);
            byte[] serializedHeader = LogSegmentDataHeader.serialize(header);
            fsOut.write(serializedHeader, 0, serializedHeader.length);
            uploadFile(segmentData.offsetIndex(), fsOut, false);
            uploadFile(segmentData.timeIndex(), fsOut, false);
            uploadFile(segmentData.leaderEpochCheckpoint(), fsOut, false);
            uploadFile(segmentData.producerIdSnapshotIndex(), fsOut, false);
            uploadFile(segmentData.txnIndex(), fsOut, false);
            uploadFile(segmentData.logSegment(), fsOut, true);
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to copy log segment to remote storage", e);
        }
    }

    @Override
    public InputStream fetchLogSegmentData(RemoteLogSegmentMetadata metadata,
                                           Long startPosition,
                                           Long endPosition) throws RemoteStorageException {
        return fetchData(metadata, SEGMENT, startPosition, endPosition);
    }

    @Override
    public InputStream fetchOffsetIndex(RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        return fetchData(metadata, OFFSET_INDEX, 0, Long.MAX_VALUE);
    }

    @Override
    public InputStream fetchTimestampIndex(RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        return fetchData(metadata, TIMESTAMP_INDEX, 0, Long.MAX_VALUE);
    }

    @Override
    public InputStream fetchTransactionIndex(RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        return fetchData(metadata, TRANSACTION_INDEX, 0, Long.MAX_VALUE);
    }

    @Override
    public InputStream fetchProducerSnapshotIndex(RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        return fetchData(metadata, PRODUCER_SNAPSHOT, 0, Long.MAX_VALUE);
    }

    @Override
    public InputStream fetchLeaderEpochIndex(RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        return fetchData(metadata, LEADER_EPOCH_CHECKPOINT, 0, Long.MAX_VALUE);
    }

    @Override
    public void deleteLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        boolean delete;
        try {
            String path = getSegmentRemoteDir(remoteLogSegmentMetadata.remoteLogSegmentId());
            delete = getFS().delete(new Path(path), true);
        } catch (Exception e) {
            throw new RemoteStorageException("Failed to delete remote log segment with id:" +
                    remoteLogSegmentMetadata.remoteLogSegmentId(), e);
        }
        if (!delete) {
            throw new RemoteStorageException("Failed to delete remote log segment with id: " +
                    remoteLogSegmentMetadata.remoteLogSegmentId());
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

    private void uploadFile(final File localSrc,
                            final FSDataOutputStream out,
                            final boolean closeStream) throws IOException {
        if (localSrc != null && localSrc.exists()) {
            final int bufferSize = hadoopConf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT);
            final byte[] buf = new byte[bufferSize];
            try (final FileInputStream fis = new FileInputStream(localSrc)) {
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

    private InputStream fetchData(RemoteLogSegmentMetadata metadata,
                                  LogSegmentDataHeader.FileType fileType,
                                  long startPosition,
                                  long endPosition) throws RemoteStorageException {
        try {
            Path dataFilePath = new Path(getSegmentRemoteDir(metadata.remoteLogSegmentId()));
            return new CachedInputStream(dataFilePath, fileType, startPosition, addExact(endPosition, 1L));
        } catch (Exception e) {
            throw new RemoteStorageException(
                    String.format("Failed to fetch %s file from remote storage", fileType), e);
        }
    }

    private FileSystem getFS() throws IOException {
        if (fs.get() == null) {
            fs.set(FileSystem.newInstance(fsURI, hadoopConf));
        }
        return fs.get();
    }

    private String getSegmentRemoteDir(RemoteLogSegmentId remoteLogSegmentId) {
        return baseDir + "/" + remoteLogSegmentId.topicPartition() + "/" + remoteLogSegmentId.id();
    }

    private long addExact(long base, long increment) {
        try {
            base = Math.addExact(base, increment);
        } catch (final ArithmeticException swallow) {
            base = Long.MAX_VALUE;
        }
        return base;
    }

    private class CachedInputStream extends InputStream {
        private final Path dataPath;
        private final long fileLen;
        private long currentPos;
        private FSDataInputStream inputStream;
        private final boolean fullFetch;

        /**
         * Input Stream which caches the data to serve them locally on repeated reads.
         * @param dataPath   path of the data file.
         * @param fileType   type of the file.
         * @param currentPos current position to read from the stream, inclusive.
         * @param endPos     to read upto the end position, exclusive.
         * @throws IOException IO problems
         */
        CachedInputStream(Path dataPath,
                          LogSegmentDataHeader.FileType fileType,
                          long currentPos,
                          long endPos) throws IOException {
            this.dataPath = dataPath;

            FileSystem fs = getFS();
            // Sends a remote fetch to read the file header.
            inputStream = fs.open(dataPath);
            byte[] buffer = new byte[LogSegmentDataHeader.LENGTH];
            inputStream.readFully(0, buffer);

            LogSegmentDataHeader.DataPosition dataPosition = LogSegmentDataHeader
                    .deserialize(ByteBuffer.wrap(buffer))
                    .getDataPosition(fileType);
            this.currentPos = addExact(currentPos, dataPosition.getPos());
            endPos = addExact(endPos, dataPosition.getPos());

            long actualFileLength = fs.getFileStatus(dataPath).getLen();
            fileLen = (dataPosition.getLength() == Integer.MAX_VALUE) ?
                    Math.min(endPos, actualFileLength) :
                    Math.min(endPos, dataPosition.getPos() + dataPosition.getLength());
            fullFetch = (fileLen == actualFileLength);
        }

        @Override
        public int read() throws IOException {
            if (currentPos >= fileLen)
                return -1;
            byte[] data = getCachedData(currentPos);
            return data[(int) ((currentPos++) % cacheLineSize)];
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            int pos = 0;
            if (len > fileLen - currentPos)
                len = (int) (fileLen - currentPos);

            if (len <= 0)
                return -1;

            while (pos < len) {
                byte[] data = getCachedData(currentPos + pos);
                int length = (int) Math.min(len - pos, data.length - (currentPos + pos) % cacheLineSize);
                System.arraycopy(data, (int) (currentPos + pos) % cacheLineSize,
                    buf, pos + off,
                    length);
                pos += length;
            }
            currentPos += pos;
            return pos;
        }

        @Override
        public int available() {
            long available = fileLen - currentPos;
            if (available > Integer.MAX_VALUE)
                return Integer.MAX_VALUE;

            return (int) available;
        }

        private byte[] getCachedData(long position) throws IOException {
            long pos = (position / cacheLineSize) * cacheLineSize;

            byte[] data = readCache.get(dataPath.toString(), pos);
            if (data != null)
                return data;

            long dataLength = Math.min(cacheLineSize, fileLen - pos);
            data = new byte[(int) dataLength];
            inputStream.readFully(pos, data);
            // To avoid intermediate results being stored in cache, save only full fetch request results.
            if (fullFetch || dataLength == cacheLineSize) {
                readCache.put(dataPath.toString(), pos, data);
            }
            return data;
        }

        @Override
        public void close() throws IOException {
            Utils.closeAll(inputStream);
            inputStream = null;
        }
    }
}
