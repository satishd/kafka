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

package org.apache.kafka.rsm.hdfs.prefetch;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.rsm.hdfs.DataFetcher;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics;
import org.apache.kafka.rsm.hdfs.RSMUtils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;

public class DownloadTask implements Callable<FileChannel> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadTask.class);
    // During performance testing, 3 MB was found to be the optimal buffer size that minimized garbage collection overhead while maximizing read throughput
    private static final int BUFFER_SIZE = 3 * 1024 * 1024;

    private final Time time;
    private final String downloadDirectory;
    private final DataFetcher dataFetcher;
    private final HDFSRemoteStorageManagerMetrics metrics;
    private final RemoteLogSegmentMetadata remoteLogSegmentMetadata;

    // Using a direct ByteBuffer to avoid an extra memory copy between user space buffers during data transfer from tempBuffer to fileChannel
    private final ThreadLocal<ByteBuffer> threadLocalBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(BUFFER_SIZE));
    private final ThreadLocal<byte[]> threadLocalTempBuffer = ThreadLocal.withInitial(() -> new byte[BUFFER_SIZE]);

    public DownloadTask(Time time, String downloadDirectory, DataFetcher dataFetcher, HDFSRemoteStorageManagerMetrics metrics, RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        this.time = time;
        this.downloadDirectory = downloadDirectory;
        this.dataFetcher = dataFetcher;
        this.metrics = metrics;
        this.remoteLogSegmentMetadata = remoteLogSegmentMetadata;
    }

    public RemoteLogSegmentMetadata getRemoteLogSegmentMetadata() {
        return remoteLogSegmentMetadata;
    }

    @Override
    public FileChannel call() throws IOException {
        return metrics.timeSegmentDownload(this::downloadSegment);
    }

    private FileChannel downloadSegment() throws IOException {
        LOGGER.debug("Reading segment data for segmentId: {}", remoteLogSegmentMetadata.remoteLogSegmentId());
        String filePath = RSMUtils.segmentPrefetchPath(downloadDirectory, remoteLogSegmentMetadata);
        RemoteLogSegmentId segmentId = remoteLogSegmentMetadata.remoteLogSegmentId();

        long startTimeMs = time.milliseconds();
        try (FSDataInputStream inputStream = dataFetcher.fetchSegmentData(remoteLogSegmentMetadata)) {
            FileChannel fileChannel = FileChannel.open(Paths.get(filePath),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ);

            long fileSize = dataFetcher.fileLength(remoteLogSegmentMetadata);
            LOGGER.debug("File size for segmentId: {} is: {}", segmentId, fileSize);

            if (fileSize <= 0) {
                throw new IOException("File size for segmentId: " + segmentId + " is not a positive number");
            }

            // Preallocate the size
            fileChannel.truncate(fileSize);

            ByteBuffer buffer = threadLocalBuffer.get();
            byte[] tempBuffer = threadLocalTempBuffer.get();

            // Read from InputStream and write to FileChannel
            int bytesRead;
            int totalBytesRead = 0;
            int pos = 0;
            
            // We are using positioned read API because hedged reads in HDFS are supported only for positioned read API
            while ((bytesRead = inputStream.read(pos, tempBuffer, 0, tempBuffer.length)) != -1) {
                buffer.put(tempBuffer, 0, bytesRead);
                buffer.flip();
                fileChannel.write(buffer);
                buffer.clear();
                // Force flush the current chunk to disk and immediately free page cache, as these chunks won't be
                // accessed until the complete file is downloaded
                fileChannel.force(false);
                totalBytesRead += bytesRead;
                pos += bytesRead;
            }
            // Force a final flush to ensure all file metadata and content are written to disk
            fileChannel.force(true);

            LOGGER.debug("Prefetched segment data for segmentId: {}, filePath: {}, size: {} in {} ms", segmentId,
                    filePath, totalBytesRead, time.milliseconds() - startTimeMs);
            return fileChannel;
        } catch (Exception e) {
            LOGGER.warn("Failed to prefetch segment data for segmentId: {}", segmentId, e);
            throw e;
        }
    }
}
