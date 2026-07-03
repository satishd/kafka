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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentReaderTest {

    @TempDir
    Path cacheDir;

    @ParameterizedTest
    @EnumSource(ReadMode.class)
    public void countsDataRecordsFromFetchedSegment(ReadMode mode) throws Exception {
        byte[] bytes = toByteArray(MemoryRecords.withRecords(0L, Compression.NONE,
                new SimpleRecord("v1".getBytes()),
                new SimpleRecord("v2".getBytes()),
                new SimpleRecord("v3".getBytes())).buffer());

        SegmentReader reader = reader(mode, bytes, SegmentReader.DEFAULT_BLOCK_SIZE);
        assertEquals(3L, reader.countDataRecords(mock(RemoteLogSegmentMetadata.class)));
    }

    @ParameterizedTest
    @EnumSource(ReadMode.class)
    public void emptySegmentHasNoRecords(ReadMode mode) throws Exception {
        SegmentReader reader = reader(mode, toByteArray(MemoryRecords.EMPTY.buffer()),
                SegmentReader.DEFAULT_BLOCK_SIZE);
        assertEquals(0L, reader.countDataRecords(mock(RemoteLogSegmentMetadata.class)));
    }

    @ParameterizedTest
    @EnumSource(ReadMode.class)
    public void streamsBatchesOneAtATimeAcrossBlockBoundaries(ReadMode mode) throws Exception {
        // Three separate batches concatenated into one segment.
        byte[] bytes = concat(
                MemoryRecords.withRecords(0L, Compression.NONE, new SimpleRecord("a".getBytes())),
                MemoryRecords.withRecords(1L, Compression.NONE,
                        new SimpleRecord("b".getBytes()), new SimpleRecord("c".getBytes())),
                MemoryRecords.withRecords(3L, Compression.NONE, new SimpleRecord("d".getBytes())));

        // A tiny block size forces many buffer refills (streaming) or a chunked download (cache),
        // exercising batches that straddle block boundaries.
        SegmentReader reader = reader(mode, bytes, 8);

        List<String> values = new ArrayList<>();
        int batchCount = 0;
        try (CloseableIterator<RecordBatch> batches = reader.batches(mock(RemoteLogSegmentMetadata.class))) {
            while (batches.hasNext()) {
                RecordBatch batch = batches.next();
                batchCount++;
                for (Record record : batch) {
                    values.add(StandardCharsets.UTF_8.decode(record.value().duplicate()).toString());
                }
            }
        }

        assertEquals(3, batchCount);
        assertEquals(Arrays.asList("a", "b", "c", "d"), values);
    }

    @ParameterizedTest
    @EnumSource(ReadMode.class)
    public void iteratorSignalsExhaustion(ReadMode mode) throws Exception {
        SegmentReader reader = reader(mode, toByteArray(MemoryRecords.EMPTY.buffer()),
                SegmentReader.DEFAULT_BLOCK_SIZE);
        try (CloseableIterator<RecordBatch> batches = reader.batches(mock(RemoteLogSegmentMetadata.class))) {
            assertFalse(batches.hasNext());
        }
    }

    @Test
    public void streamingModeClosesUnderlyingStream() throws Exception {
        byte[] bytes = toByteArray(MemoryRecords.withRecords(0L, Compression.NONE,
                new SimpleRecord("v1".getBytes())).buffer());
        AtomicInteger closes = new AtomicInteger();
        RemoteStorageManager rsm = mock(RemoteStorageManager.class);
        when(rsm.fetchLogSegment(any(), eq(0))).thenReturn(new CloseTrackingStream(bytes, closes));

        SegmentReader reader = SegmentReader.create(rsm, ReadMode.STREAM,
                SegmentReader.DEFAULT_BLOCK_SIZE, cacheDir);
        CloseableIterator<RecordBatch> batches = reader.batches(mock(RemoteLogSegmentMetadata.class));
        assertTrue(batches.hasNext());
        batches.close();

        assertEquals(1, closes.get());
    }

    @Test
    public void cacheModeClosesDownloadStreamAndDeletesFile() throws Exception {
        byte[] bytes = toByteArray(MemoryRecords.withRecords(0L, Compression.NONE,
                new SimpleRecord("v1".getBytes())).buffer());
        AtomicInteger closes = new AtomicInteger();
        RemoteStorageManager rsm = mock(RemoteStorageManager.class);
        when(rsm.fetchLogSegment(any(), eq(0))).thenReturn(new CloseTrackingStream(bytes, closes));

        SegmentReader reader = SegmentReader.create(rsm, ReadMode.CACHE,
                SegmentReader.DEFAULT_BLOCK_SIZE, cacheDir);
        CloseableIterator<RecordBatch> batches = reader.batches(mock(RemoteLogSegmentMetadata.class));

        // Download stream is closed as soon as the prefetch finishes, before iteration.
        assertEquals(1, closes.get());
        assertEquals(1, cachedFileCount(), "prefetched segment file should exist while open");
        assertTrue(batches.hasNext());

        batches.close();
        assertEquals(0, cachedFileCount(), "cached segment file should be deleted on close");
    }

    @Test
    public void cacheModeDeletesFileWhenDownloadFails() throws Exception {
        RemoteStorageManager rsm = mock(RemoteStorageManager.class);
        when(rsm.fetchLogSegment(any(), eq(0))).thenReturn(new FailingInputStream());

        SegmentReader reader = SegmentReader.create(rsm, ReadMode.CACHE,
                SegmentReader.DEFAULT_BLOCK_SIZE, cacheDir);
        assertThrows(java.io.IOException.class,
                () -> reader.batches(mock(RemoteLogSegmentMetadata.class)));
        assertEquals(0, cachedFileCount(), "failed prefetch must not leak a cache file");
    }

    @Test
    public void defaultBlockSizeIs4MiB() {
        assertEquals(4 * 1024 * 1024, SegmentReader.DEFAULT_BLOCK_SIZE);
    }

    @Test
    public void rejectsNonPositiveBlockSize() {
        RemoteStorageManager rsm = mock(RemoteStorageManager.class);
        assertThrows(IllegalArgumentException.class, () -> new SegmentReader(rsm, 0));
        assertThrows(IllegalArgumentException.class,
                () -> SegmentReader.create(rsm, ReadMode.CACHE, -1, cacheDir));
    }

    private SegmentReader reader(ReadMode mode, byte[] segment, int blockSize) throws Exception {
        RemoteStorageManager rsm = mock(RemoteStorageManager.class);
        when(rsm.fetchLogSegment(any(), eq(0))).thenReturn(new ByteArrayInputStream(segment));
        return SegmentReader.create(rsm, mode, blockSize, cacheDir);
    }

    private long cachedFileCount() throws Exception {
        try (java.util.stream.Stream<Path> files = java.nio.file.Files.list(cacheDir)) {
            return files.count();
        }
    }

    private static byte[] concat(MemoryRecords... records) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (MemoryRecords record : records) {
            byte[] bytes = toByteArray(record.buffer());
            out.write(bytes, 0, bytes.length);
        }
        return out.toByteArray();
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    /** Wraps a byte source and counts how many times {@link #close()} is called. */
    private static final class CloseTrackingStream extends FilterInputStream {
        private final AtomicInteger closes;

        CloseTrackingStream(byte[] bytes, AtomicInteger closes) {
            super(new ByteArrayInputStream(bytes));
            this.closes = closes;
        }

        @Override
        public void close() throws java.io.IOException {
            closes.incrementAndGet();
            super.close();
        }
    }

    /** Always throws on read, to simulate a mid-download failure. */
    private static final class FailingInputStream extends java.io.InputStream {
        @Override
        public int read() throws java.io.IOException {
            throw new java.io.IOException("boom");
        }

        @Override
        public int read(byte[] b, int off, int len) throws java.io.IOException {
            throw new java.io.IOException("boom");
        }
    }
}
