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
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class RSMUtils {

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

    public static InputStream getInputStreamFromChannel(FileChannel channel, int startPosition, int endPosition) throws IOException {
        // Read and parse the header first
        ByteBuffer headerBuffer = ByteBuffer.allocate(LogSegmentDataHeader.LENGTH);
        int byteReads = channel.read(headerBuffer, 0);
        if (byteReads != LogSegmentDataHeader.LENGTH) {
            throw new IOException("Failed to read LogSegmentDataHeader from channel");
        }
        headerBuffer.flip();

        LogSegmentDataHeader header = LogSegmentDataHeader.deserialize(headerBuffer);
        // Get the data position from the header
        LogSegmentDataHeader.DataPosition dataPosition = header.getDataPosition(LogSegmentDataHeader.FileType.SEGMENT);

        // fileLength is the length of both the LogSegmentDataHeader and the Segment file.
        long fileLength = channel.size();
        long segmentLength = fileLength - dataPosition.getPos();
        final long readableLength = endPosition != Integer.MAX_VALUE ? Math.min(endPosition + 1, segmentLength) : segmentLength;

        return new InputStream() {
            private long pos = dataPosition.getPos() + startPosition;

            @Override
            public int read() throws IOException {
                if (pos >= dataPosition.getPos() + readableLength) return -1;
                ByteBuffer buf = ByteBuffer.allocate(1);
                int bytesRead = channel.read(buf, pos);
                if (bytesRead <= 0) return -1;
                pos++;
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
                }
                return bytesRead;
            }

            @Override
            public void close() throws IOException {
                // Do nothing
            }
        };
    }
}
