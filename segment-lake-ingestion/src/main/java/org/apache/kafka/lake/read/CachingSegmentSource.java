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
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * Prefetches a segment to a local file, then reads batches from it with {@link FileRecords}.
 *
 * <p>The remote object is downloaded once, sequentially, into a temp file under {@code cacheDir};
 * the network stream is closed as soon as the download finishes. Batches are then read from the file
 * channel, bringing each batch's record data into heap only when touched, so the segment is realized
 * on disk rather than on heap. The temp file is deleted when the returned iterator is closed (with a
 * JVM-exit hook as a backstop), so peak local disk is bounded by the sum of the segments in flight.
 *
 * <p>Prefer this over {@link StreamingSegmentSource} when the RSM stream is slow or unreliable, or
 * when a long decode should not hold a network stream open; it costs local disk in exchange.
 */
final class CachingSegmentSource implements SegmentBatchSource {

    private static final Logger LOG = LoggerFactory.getLogger(CachingSegmentSource.class);

    private final RemoteStorageManager remoteStorageManager;
    private final int blockSize;
    private final Path cacheDir;

    CachingSegmentSource(RemoteStorageManager remoteStorageManager, int blockSize, Path cacheDir) {
        this.remoteStorageManager = remoteStorageManager;
        this.blockSize = blockSize;
        this.cacheDir = cacheDir;
        try {
            Files.createDirectories(cacheDir);
        } catch (IOException e) {
            throw new KafkaException("Failed to create segment cache directory " + cacheDir, e);
        }
    }

    @Override
    public CloseableIterator<RecordBatch> batches(RemoteLogSegmentMetadata metadata)
            throws RemoteStorageException, IOException {
        Path file = Files.createTempFile(cacheDir, "segment-", ".log");
        file.toFile().deleteOnExit();
        try {
            download(metadata, file);
            FileRecords fileRecords = FileRecords.open(file.toFile(), false);
            return new FileRecordBatchStream(fileRecords, file);
        } catch (RemoteStorageException | IOException | RuntimeException e) {
            deleteQuietly(file);
            throw e;
        }
    }

    private void download(RemoteLogSegmentMetadata metadata, Path file) throws RemoteStorageException, IOException {
        byte[] buffer = new byte[blockSize];
        try (InputStream in = remoteStorageManager.fetchLogSegment(metadata, 0);
             OutputStream out = Files.newOutputStream(file)) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
        }
    }

    private static void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            LOG.warn("Failed to delete cached segment file {}", file, e);
        }
    }

    /**
     * Iterates the batches of an open {@link FileRecords}, closing the file and deleting the cached
     * copy on {@link #close()}. Record data is read lazily from the file channel, so callers must
     * finish reading each batch before closing.
     */
    private static final class FileRecordBatchStream implements CloseableIterator<RecordBatch> {

        private final FileRecords fileRecords;
        private final Path file;
        private final Iterator<? extends RecordBatch> batches;

        FileRecordBatchStream(FileRecords fileRecords, Path file) {
            this.fileRecords = fileRecords;
            this.file = file;
            this.batches = fileRecords.batches().iterator();
        }

        @Override
        public boolean hasNext() {
            return batches.hasNext();
        }

        @Override
        public RecordBatch next() {
            return batches.next();
        }

        @Override
        public void close() {
            try {
                fileRecords.close();
            } catch (IOException e) {
                throw new KafkaException("Failed to close cached segment file " + file, e);
            } finally {
                deleteQuietly(file);
            }
        }
    }
}
