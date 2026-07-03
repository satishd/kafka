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

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Fetches a remote log segment through the {@link RemoteStorageManager} and parses it into Kafka
 * record batches.
 *
 * <p>The Uber RSM packs all indexes and the log into a single remote object; {@code fetchLogSegment}
 * hides that layout and returns the {@code .log} bytes, which are Kafka's binary record-batch format
 * (not Avro — record values are decoded downstream).
 *
 * <p>Segments can be up to the broker's configured segment size (often hundreds of MB), so they are
 * never loaded whole onto the heap. {@link #batches(RemoteLogSegmentMetadata)} always yields one
 * {@link RecordBatch} at a time; how the bytes are obtained is chosen by {@link ReadMode} and
 * delegated to a {@link SegmentBatchSource}:
 * <ul>
 *   <li>{@link ReadMode#STREAM} reads directly from the remote object (no local disk);</li>
 *   <li>{@link ReadMode#CACHE} prefetches the segment to a local file and reads it back.</li>
 * </ul>
 */
public class SegmentReader implements Closeable {

    /** Default block size for the read/download buffer: 4 MiB. */
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024;

    private final SegmentBatchSource source;

    SegmentReader(SegmentBatchSource source) {
        this.source = source;
    }

    /**
     * Streaming reader with the {@link #DEFAULT_BLOCK_SIZE default block size}.
     */
    public SegmentReader(RemoteStorageManager remoteStorageManager) {
        this(remoteStorageManager, DEFAULT_BLOCK_SIZE);
    }

    /**
     * Streaming reader with a custom block size.
     *
     * @param blockSize bytes read from the remote object per underlying read; must be positive.
     */
    public SegmentReader(RemoteStorageManager remoteStorageManager, int blockSize) {
        this(new StreamingSegmentSource(remoteStorageManager, requirePositive(blockSize)));
    }

    /**
     * Build a reader for the given {@link ReadMode}.
     *
     * @param remoteStorageManager RSM used to open the segment's remote object.
     * @param mode strategy used to obtain the segment bytes.
     * @param blockSize bytes read from the remote object per underlying read (and the download buffer
     *                  size in {@link ReadMode#CACHE}); must be positive.
     * @param cacheDir directory for prefetched segment files; used only in {@link ReadMode#CACHE}.
     */
    public static SegmentReader create(RemoteStorageManager remoteStorageManager, ReadMode mode,
                                       int blockSize, Path cacheDir) {
        requirePositive(blockSize);
        switch (mode) {
            case STREAM:
                return new SegmentReader(new StreamingSegmentSource(remoteStorageManager, blockSize));
            case CACHE:
                return new SegmentReader(new CachingSegmentSource(remoteStorageManager, blockSize, cacheDir));
            default:
                throw new IllegalArgumentException("Unsupported read mode: " + mode);
        }
    }

    /**
     * Open the segment and stream its record batches one at a time. Only one batch is held on heap at
     * any point, so this never allocates a segment-sized buffer.
     *
     * <p>The returned iterator owns the underlying resources (a remote stream, or a cached file); the
     * caller <b>must</b> close it (ideally via try-with-resources) even if iteration stops early.
     *
     * @param metadata metadata of the segment to read.
     * @return a closeable iterator over the segment's record batches.
     */
    public CloseableIterator<RecordBatch> batches(RemoteLogSegmentMetadata metadata)
            throws RemoteStorageException, IOException {
        return source.batches(metadata);
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

    @Override
    public void close() throws IOException {
        source.close();
    }

    private static int requirePositive(int blockSize) {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive but was " + blockSize);
        }
        return blockSize;
    }
}
