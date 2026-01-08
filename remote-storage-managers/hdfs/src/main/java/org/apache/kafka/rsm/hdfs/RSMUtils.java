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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class RSMUtils {

    public static final String KLOAK_USER = Path.SEPARATOR + "user" + Path.SEPARATOR + "kloak" + Path.SEPARATOR;
    static final String DELIMITER = "_";

    public static String segmentPrefetchPath(String downloadDirectory, RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        RemoteLogSegmentId segmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
        return downloadDirectory + Path.SEPARATOR + segmentId.topicIdPartition() + DELIMITER + segmentId.id() + DELIMITER + remoteLogSegmentMetadata.startOffset();
    }

    public static String getSegmentRemoteDir(final String baseDir, final RemoteLogSegmentId segmentId) {
        return getPartitionRemoteDir(baseDir, segmentId.topicIdPartition()) + Path.SEPARATOR + segmentId.id();
    }

    public static String getPartitionRemoteDir(final String baseDir, final TopicIdPartition partition) {
        return baseDir + Path.SEPARATOR + partition.topicPartition() + "-" + partition.topicId();
    }

    public static InputStream getInputStreamFromChannel(FileChannel channel, RemoteStorageManager.IndexType indexType)
            throws IOException {
        LogSegmentDataHeader header = getLogSegmentDataHeader(channel);
        LogSegmentDataHeader.FileType fileType = getFileTypeFromIndexType(indexType);
        LogSegmentDataHeader.DataPosition dataPosition = header.getDataPosition(fileType);
        return readInputStreamFromChannel(channel, dataPosition, 0, dataPosition.getLength() - 1);
    }

    public static InputStream getInputStreamFromChannel(FileChannel channel, int startPosition, int endPosition) throws IOException {
        // Read and parse the header first
        LogSegmentDataHeader header = getLogSegmentDataHeader(channel);
        // Get the data position from the header
        LogSegmentDataHeader.DataPosition dataPosition = header.getDataPosition(LogSegmentDataHeader.FileType.SEGMENT);
        return readInputStreamFromChannel(channel, dataPosition, startPosition, endPosition);
    }

    static LogSegmentDataHeader getLogSegmentDataHeader(FileChannel channel) throws IOException {
        // Read and parse the header first
        ByteBuffer headerBuffer = ByteBuffer.allocate(LogSegmentDataHeader.LENGTH);
        int byteReads = channel.read(headerBuffer, 0);
        if (byteReads != LogSegmentDataHeader.LENGTH) {
            throw new IOException("Failed to read LogSegmentDataHeader from channel");
        }
        headerBuffer.flip();
        return LogSegmentDataHeader.deserialize(headerBuffer);
    }

    private static InputStream readInputStreamFromChannel(FileChannel channel,
                                                          LogSegmentDataHeader.DataPosition dataPosition,
                                                          int startPosition,
                                                          int endPosition) throws IOException {
        // fileLength is the length of both the LogSegmentDataHeader and the Segment file.
        long fileLength = channel.size();
        long segmentLength = fileLength - dataPosition.getPos();
        final long readableLength = endPosition != Integer.MAX_VALUE ? Math.min(endPosition + 1, segmentLength) : segmentLength;

        return new InputStream() {
            private long pos = dataPosition.getPos() + startPosition;
            private long totalBytesRead = 0;

            @Override
            public int available() {
                long readableSegmentLen = Math.max(0, readableLength - startPosition);
                long available = readableSegmentLen - totalBytesRead;
                if (available > Integer.MAX_VALUE) {
                    return Integer.MAX_VALUE;
                }
                return (int) available;
            }

            @Override
            public int read() throws IOException {
                if (pos >= dataPosition.getPos() + readableLength) return -1;
                ByteBuffer buf = ByteBuffer.allocate(1);
                int bytesRead = channel.read(buf, pos);
                if (bytesRead <= 0) return -1;
                pos++;
                totalBytesRead++;
                buf.flip();
                return buf.get() & 0xFF;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (pos >= dataPosition.getPos() + readableLength) return -1;
                int adjustedLen = (int) Math.min(len, readableLength - (pos - dataPosition.getPos()));
                ByteBuffer buf = ByteBuffer.wrap(b, off, adjustedLen);
                int bytesRead = channel.read(buf, pos);
                if (bytesRead > 0) {
                    pos += bytesRead;
                    totalBytesRead += bytesRead;
                }
                return bytesRead;
            }

            @Override
            public void close() throws IOException {
                // Do nothing
            }
        };
    }

    static LogSegmentDataHeader.FileType getFileTypeFromIndexType(RemoteStorageManager.IndexType indexType) {
        switch (indexType) {
            case OFFSET:
                return LogSegmentDataHeader.FileType.OFFSET_INDEX;
            case TIMESTAMP:
                return LogSegmentDataHeader.FileType.TIMESTAMP_INDEX;
            case TRANSACTION:
                return LogSegmentDataHeader.FileType.TRANSACTION_INDEX;
            case LEADER_EPOCH:
                return LogSegmentDataHeader.FileType.LEADER_EPOCH_CHECKPOINT;
            case PRODUCER_SNAPSHOT:
                return LogSegmentDataHeader.FileType.PRODUCER_SNAPSHOT;
            default:
                throw new IllegalArgumentException("Unsupported index type: " + indexType);
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
    public static void validateConfigValueRange(String prop, long currentValue, long newValue) {
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
}
