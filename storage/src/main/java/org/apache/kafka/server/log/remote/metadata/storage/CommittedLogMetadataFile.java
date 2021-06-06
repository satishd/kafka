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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CommittedLogMetadataFile {

    public static final String COMMITTED_LOG_METADATA_SNAPSHOT_FILE_NAME = "remote_log_snapshot";

    // header: <version:short><topicId:2 longs><metadata-partition:int><metadata-partition-offset:long>
    // size: 2 + (8+8) + 4 + 8 = 30
    private static final int HEADER_SIZE = 30;

    private final File metadataStoreFile;

    CommittedLogMetadataFile(TopicIdPartition topicIdPartition,
                             Path metadataStoreDir) {
        this.metadataStoreFile = new File(metadataStoreDir.toFile(), COMMITTED_LOG_METADATA_SNAPSHOT_FILE_NAME);

        if (!metadataStoreFile.exists()) {
            try {
                metadataStoreFile.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void write(Snapshot snapshot) throws IOException {
        File newMetadataStoreFile = new File(metadataStoreFile.getAbsolutePath() + ".new");
        try (FileOutputStream fileOutputStream = new FileOutputStream(newMetadataStoreFile);
             BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream)) {

            ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);

            // Write version
            headerBuffer.putShort(snapshot.version);
            bufferedOutputStream.write(headerBuffer.array());

            // Write topic-id
            headerBuffer.putLong(snapshot.topicId.getMostSignificantBits());
            headerBuffer.putLong(snapshot.topicId.getLeastSignificantBits());

            // Write metadata partition and metadata partition offset
            headerBuffer.putInt(snapshot.metadataPartition);
            headerBuffer.putLong(snapshot.metadataPartitionOffset);

            // Write header
            bufferedOutputStream.write(headerBuffer.array());

            // Write each entry
            ByteBuffer lenBuffer = ByteBuffer.allocate(8);
            RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
            for (RemoteLogSegmentMetadata remoteLogSegmentMetadata : snapshot.remoteLogMetadatas) {
                final byte[] serializedBytes = serde.serialize(remoteLogSegmentMetadata);
                // Write length
                lenBuffer.putInt(serializedBytes.length);
                bufferedOutputStream.write(lenBuffer.array());
                lenBuffer.flip();

                // Write data
                bufferedOutputStream.write(serializedBytes);
            }

            fileOutputStream.getFD().sync();
        }

        Utils.atomicMoveWithFallback(newMetadataStoreFile.toPath(), metadataStoreFile.toPath());
    }

    @SuppressWarnings("unchecked")
    public synchronized Snapshot read() throws IOException {

        // Checking for empty files.
        if (metadataStoreFile.length() == 0) {
            throw new IOException("Empty snapshot file.");
        }

        try (FileInputStream fis = new FileInputStream(metadataStoreFile)) {

            // Read header
            ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
            fis.read(headerBuffer.array());

            Short version = headerBuffer.getShort();
            Uuid topicId = new Uuid(headerBuffer.getLong(), headerBuffer.getLong());
            int metadataPartition = headerBuffer.getInt();
            long metadataPartitionOffset = headerBuffer.getLong();

            RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();

            List<RemoteLogSegmentMetadata> result = new ArrayList<>();

            ByteBuffer lenBuffer = ByteBuffer.allocate(4);
            while (fis.read(lenBuffer.array()) != -1) {
                // Read the length of each entry
                final int len = lenBuffer.getInt();
                lenBuffer.flip();

                // Read the entry
                byte[] data = new byte[len];
                final int read = fis.read(data);
                if (read != len) {
                    throw new IOException("Invalid amount of data read, file may have been corrupted.");
                }

                // We are always adding RemoteLogSegmentMetadata only as you can see in #write() method.
                // Did not add a specific serde for RemoteLogSegmentMetadata and reusing RemoteLogMetadataSerde
                final RemoteLogSegmentMetadata remoteLogSegmentMetadata = (RemoteLogSegmentMetadata) serde.deserialize(data);
                result.add(remoteLogSegmentMetadata);
            }

            return new Snapshot(version, topicId, metadataPartition, metadataPartitionOffset, result);
        }
    }

    public static final class Snapshot {
        private static final short CURRENT_VERSION = 0;

        private final short version;
        private final Uuid topicId;
        private final int metadataPartition;
        private final long metadataPartitionOffset;
        private final Collection<RemoteLogSegmentMetadata> remoteLogMetadatas;

        public Snapshot(Uuid topicId,
                        int metadataPartition,
                        long metadataPartitionOffset,
                        Collection<RemoteLogSegmentMetadata> remoteLogMetadatas) {
            this(CURRENT_VERSION, topicId, metadataPartition, metadataPartitionOffset, remoteLogMetadatas);
        }

        public Snapshot(short version,
                        Uuid topicId,
                        int metadataPartition,
                        long metadataPartitionOffset,
                        Collection<RemoteLogSegmentMetadata> remoteLogMetadatas) {
            this.version = version;
            this.topicId = topicId;
            this.metadataPartition = metadataPartition;
            this.metadataPartitionOffset = metadataPartitionOffset;
            this.remoteLogMetadatas = remoteLogMetadatas;
        }

        public short version() {
            return version;
        }

        public Uuid topicId() {
            return topicId;
        }

        public int metadataPartition() {
            return metadataPartition;
        }

        public long metadataPartitionOffset() {
            return metadataPartitionOffset;
        }

        public Collection<RemoteLogSegmentMetadata> remoteLogMetadatas() {
            return remoteLogMetadatas;
        }
    }
}