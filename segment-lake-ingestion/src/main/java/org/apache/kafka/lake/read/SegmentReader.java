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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RemoteLogInputStream;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * Fetches a remote log segment through the {@link RemoteStorageManager} and parses it into Kafka
 * record batches.
 *
 * <p>The Uber RSM packs all indexes and the log into a single remote object; {@code fetchLogSegment}
 * hides that layout and returns the {@code .log} bytes, which are Kafka's binary record-batch format
 * (not Avro — record values are decoded in a later commit).
 *
 * <p>Segments can be up to the broker's configured segment size (often hundreds of MB), so they are
 * never loaded whole. {@link #batches(RemoteLogSegmentMetadata)} streams the remote object in blocks
 * of {@code blockSize} bytes and materializes a single {@link RecordBatch} at a time; peak heap per
 * in-flight segment is bounded by the block size plus the largest batch, not the segment size.
 */
public class SegmentReader {

    /** Default block size for the streaming read buffer: 4 MiB. */
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024;

    private final RemoteStorageManager remoteStorageManager;
    private final int blockSize;

    public SegmentReader(RemoteStorageManager remoteStorageManager) {
        this(remoteStorageManager, DEFAULT_BLOCK_SIZE);
    }

    /**
     * @param remoteStorageManager RSM used to open the segment's remote object.
     * @param blockSize number of bytes read from the remote object per underlying read; larger
     *                  values trade memory for fewer round trips. Must be positive.
     */
    public SegmentReader(RemoteStorageManager remoteStorageManager, int blockSize) {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive but was " + blockSize);
        }
        this.remoteStorageManager = remoteStorageManager;
        this.blockSize = blockSize;
    }

    /**
     * Open the segment and stream its record batches one at a time. The segment is read from the
     * remote object in {@code blockSize} blocks and only one batch is held in memory at any point, so
     * this never allocates a segment-sized buffer.
     *
     * <p>The returned iterator owns the underlying remote input stream; the caller <b>must</b> close
     * it (ideally via try-with-resources) even if iteration is stopped early.
     *
     * @param metadata metadata of the segment to read.
     * @return a closeable iterator over the segment's record batches.
     */
    public CloseableIterator<RecordBatch> batches(RemoteLogSegmentMetadata metadata)
            throws RemoteStorageException, IOException {
        InputStream in = remoteStorageManager.fetchLogSegment(metadata, 0);
        try {
            RemoteLogInputStream logInputStream = new RemoteLogInputStream(new BufferedInputStream(in, blockSize));
            return new RecordBatchStream(in, logInputStream);
        } catch (RuntimeException e) {
            in.close();
            throw e;
        }
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
        try (CloseableIterator<RecordBatch> batches = batches(metadata)) {
            while (batches.hasNext()) {
                RecordBatch batch = batches.next();
                if (batch.isControlBatch()) {
                    continue;
                }
                for (Record record : batch) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Lazily pulls batches from a {@link RemoteLogInputStream}, keeping the source stream open until
     * closed. Reimplements the batch-at-a-time logic of the package-private
     * {@code RecordBatchIterator} because that class is not visible outside {@code common.record}.
     */
    private static final class RecordBatchStream extends AbstractIterator<RecordBatch>
            implements CloseableIterator<RecordBatch> {

        private final Closeable resource;
        private final RemoteLogInputStream logInputStream;

        RecordBatchStream(Closeable resource, RemoteLogInputStream logInputStream) {
            this.resource = resource;
            this.logInputStream = logInputStream;
        }

        @Override
        protected RecordBatch makeNext() {
            try {
                RecordBatch batch = logInputStream.nextBatch();
                return batch == null ? allDone() : batch;
            } catch (IOException e) {
                throw new KafkaException("Failed to read the next record batch from the segment stream", e);
            }
        }

        @Override
        public void close() {
            try {
                resource.close();
            } catch (IOException e) {
                throw new KafkaException("Failed to close the segment input stream", e);
            }
        }
    }
}
