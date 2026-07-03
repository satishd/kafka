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
 * Streams a segment straight from the {@link RemoteStorageManager}, parsing one batch at a time.
 *
 * <p>The remote object is read through a {@link BufferedInputStream} of {@code blockSize} bytes into
 * Kafka's {@link RemoteLogInputStream}, so peak heap per in-flight segment is bounded by the block
 * size plus the largest batch, never the segment size. No local disk is used, but the network stream
 * is held open until the returned iterator is closed.
 */
final class StreamingSegmentSource implements SegmentBatchSource {

    private final RemoteStorageManager remoteStorageManager;
    private final int blockSize;

    StreamingSegmentSource(RemoteStorageManager remoteStorageManager, int blockSize) {
        this.remoteStorageManager = remoteStorageManager;
        this.blockSize = blockSize;
    }

    @Override
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
