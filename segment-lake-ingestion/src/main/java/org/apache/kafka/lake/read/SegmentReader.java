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
package org.apache.kafka.lake.read;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Fetches a remote log segment through the {@link RemoteStorageManager} and parses it into Kafka
 * record batches.
 *
 * <p>The Uber RSM packs all indexes and the log into a single remote object; {@code fetchLogSegment}
 * hides that layout and returns the {@code .log} bytes, which are Kafka's binary record-batch format
 * (not Avro — record values are decoded in a later commit).
 */
public class SegmentReader {

    private static final int READ_BUFFER_SIZE = 64 * 1024;

    private final RemoteStorageManager remoteStorageManager;

    public SegmentReader(RemoteStorageManager remoteStorageManager) {
        this.remoteStorageManager = remoteStorageManager;
    }

    /**
     * Fetch a segment in full and expose it as in-memory records. The returned {@link MemoryRecords}
     * is backed by a heap buffer and holds no external resources.
     *
     * @param metadata metadata of the segment to fetch.
     * @return the segment's records.
     */
    public MemoryRecords fetch(RemoteLogSegmentMetadata metadata) throws RemoteStorageException, IOException {
        try (InputStream in = remoteStorageManager.fetchLogSegment(metadata, 0)) {
            return MemoryRecords.readableRecords(ByteBuffer.wrap(readFully(in)));
        }
    }

    private static byte[] readFully(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(READ_BUFFER_SIZE);
        byte[] buffer = new byte[READ_BUFFER_SIZE];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    /**
     * Count data records in a segment, skipping control batches. Used to validate the end-to-end
     * fetch/parse path before decoding is added.
     *
     * @param metadata metadata of the segment to read.
     * @return number of data records.
     */
    public long countDataRecords(RemoteLogSegmentMetadata metadata) throws RemoteStorageException, IOException {
        long count = 0;
        for (RecordBatch batch : fetch(metadata).batches()) {
            if (batch.isControlBatch()) {
                continue;
            }
            Iterator<Record> records = batch.iterator();
            while (records.hasNext()) {
                records.next();
                count++;
            }
        }
        return count;
    }
}
