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

import kafka.log.remote.quota.RLMQuotaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.rsm.hdfs.DataFetcher;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics;
import org.apache.kafka.rsm.hdfs.LogSegmentDataHeader;
import org.apache.kafka.rsm.hdfs.PrefetchEnabledHDFSRemoteStorageManager;
import org.apache.kafka.rsm.hdfs.RSMUtils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageProvider;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS;
import static org.apache.kafka.rsm.hdfs.prefetch.RSMTestUtils.clearKafkaMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DownloadTaskTest {

    private Time mockTime;
    private DataFetcher mockDataFetcher;
    private RemoteLogSegmentMetadata metadata;
    private RemoteLogSegmentId segmentId;
    private FSDataInputStream mockInputStream;
    private HDFSRemoteStorageManagerMetrics metrics;
    private RLMQuotaManager rlmQuotaManager;
    private ReentrantLock lock;
    private Condition lockCondition;
    private long pos;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setup() throws IOException {
        mockTime = new MockTime();
        mockDataFetcher = mock(DataFetcher.class);

        // Create test metadata
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
        segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        Map<Integer, Long> segmentLeaderEpochs = Collections.singletonMap(5, 100L);

        metadata = new RemoteLogSegmentMetadata(
            segmentId,
            0L,
            100L,
            1000L,
            1,
            System.currentTimeMillis(),
            1024,
            Optional.empty(),
            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
            segmentLeaderEpochs
        );

        // Create directory structure for download
        String downloadPath = RSMUtils.segmentPrefetchPath(tempDir.toString(), metadata);
        File downloadDir = new File(downloadPath).getParentFile();
        downloadDir.mkdirs();

        // Mock FSDataInputStream
        mockInputStream = mock(FSDataInputStream.class);

        metrics = new HDFSRemoteStorageManagerMetrics();
        clearKafkaMetrics();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        Cache<RemoteLogSegmentId, CacheValue> cache = Caffeine.newBuilder().build();
        metrics.registerPrefetchMetrics(executor, cache, tempDir.toAbsolutePath().toString());

        rlmQuotaManager = mock(RLMQuotaManager.class);
        when(rlmQuotaManager.getThrottleTimeMs()).thenReturn(0L);

        lock = spy(new ReentrantLock());
        lockCondition = spy(lock.newCondition());
        pos = 0;
    }

    @ParameterizedTest
    @CsvSource({"hdfs", "oci"})
    public void testSuccessfulDownload(String provider) throws Exception {
        RemoteStorageProvider storageProvider = RemoteStorageProvider.fromName(provider);
        // Mock data content
        byte[] testData = new byte[1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }

        // Set up mock behavior
        when(mockDataFetcher.fetchSegmentData(metadata)).thenReturn(mockInputStream);
        when(mockDataFetcher.fileLength(metadata)).thenReturn((long) (LogSegmentDataHeader.LENGTH + testData.length));
        when(mockDataFetcher.storageProvider(metadata)).thenReturn(storageProvider);

        // Mock read behavior for data using positional reads
        when(mockInputStream.read(anyLong(), any(byte[].class), anyInt(), anyInt())).thenAnswer(invocation -> {
            long pos = invocation.getArgument(0);
            byte[] buffer = invocation.getArgument(1);
            int offset = invocation.getArgument(2);
            int length = invocation.getArgument(3);

            int bytesToCopy = Math.min(length, (int) (testData.length - pos));
            if (bytesToCopy <= 0) return -1; // EOF

            System.arraycopy(testData, (int) pos, buffer, offset, bytesToCopy);
            return bytesToCopy;
        });

        // Mock read behavior for data using non-positional reads
        when(mockInputStream.read(any(byte[].class), anyInt(), anyInt())).thenAnswer(invocation -> {
            byte[] buffer = invocation.getArgument(0);
            int offset = invocation.getArgument(1);
            int length = invocation.getArgument(2);

            int bytesToCopy = Math.min(length, (int) (testData.length - pos));
            if (bytesToCopy <= 0) return -1; // EOF

            System.arraycopy(testData, (int) pos, buffer, offset, bytesToCopy);
            pos += bytesToCopy;
            return bytesToCopy;
        });

        // Verify initial metric values
        verifyTimerCount(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0);
        verifyTimerQuantile(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0.5, value -> value == 0);

        // Create and execute the task
        DownloadTask task = createDownloadTask();
        FileChannel result = task.call();

        // Verify results
        assertNotNull(result, "FileChannel should not be null");
        assertTrue(result.isOpen(), "FileChannel should be open");
        assertEquals(testData.length, result.size(), "File size should match test data size");

        // Verify file content
        ByteBuffer readBuffer = ByteBuffer.allocate((int) result.size());
        result.position(0);
        result.read(readBuffer);
        readBuffer.flip();

        byte[] fileContent = new byte[readBuffer.remaining()];
        readBuffer.get(fileContent);

        // Compare content
        for (int i = 0; i < testData.length; i++) {
            assertEquals(testData[i], fileContent[i], "File content should match test data at position " + i);
        }

        // Verify the metrics
        verifyTimerCount(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 1);
        verifyTimerQuantile(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0.5, value -> value > 0);
    }

    @Test
    public void testQuotaReservationOnSuccess() throws Exception {
        byte[] testData = createSequentialTestData(1024);
        long fileSize = stubFetchAndFileLength(testData);
        when(mockInputStream.read(any(byte[].class), anyInt(), anyInt())).thenAnswer(invocation -> {
            // Get invocation parameters
            byte[] buffer = invocation.getArgument(0);
            int offset = invocation.getArgument(1);
            int length = invocation.getArgument(2);

            int bytesToCopy = Math.min(length, (int) (testData.length - pos));
            if (bytesToCopy <= 0) return -1;
            System.arraycopy(testData, (int) pos, buffer, offset, bytesToCopy);
            pos += bytesToCopy;
            return bytesToCopy;
        });

        DownloadTask task = createDownloadTask();
        FileChannel result = task.call();
        assertNotNull(result);
        assertEquals(testData.length, result.size());

        // Verify quota reservation was recorded once with full file size
        verify(rlmQuotaManager, times(1)).record(eq((double) fileSize));
    }

    @Test
    public void testQuotaReservationAndReleaseOnFailure() throws Exception {
        // Prepare data so that we can simulate partial read then exception
        byte[] testData = createSequentialTestData(1024);
        long fileSize = stubFetchAndFileLength(testData);

        // Simulate: first call reads 256 bytes, second call throws IOException
        when(mockInputStream.read(any(byte[].class), anyInt(), anyInt()))
            .thenAnswer(invocation -> {
                // Get invocation parameters
                byte[] buffer = invocation.getArgument(0);
                int offset = invocation.getArgument(1);
                int length = invocation.getArgument(2);

                int bytesToCopy = Math.min(length, 256);
                System.arraycopy(testData, (int) pos, buffer, offset, bytesToCopy);
                pos += bytesToCopy;
                return bytesToCopy;
            })
            .thenThrow(new IOException("Simulated read failure"));

        DownloadTask task = createDownloadTask();

        IOException thrown = assertThrows(IOException.class, task::call);
        assertTrue(thrown.getMessage().contains("Simulated read failure"));

        // Verify initial reservation and subsequent release of unused quota
        verify(rlmQuotaManager, times(1)).record(eq((double) fileSize));
        // 256 bytes were actually downloaded (not including header), so downloadSize=256
        // release adjustment = 256 - fileSize
        verify(rlmQuotaManager, times(1)).record(eq((double) (256 - fileSize)));
    }

    @Test
    public void testDownloadWithIOException() throws IOException {
        // Set up mock to throw IOException
        IOException testException = new IOException("Test exception");
        when(mockDataFetcher.fetchSegmentData(metadata)).thenThrow(testException);

        // Verify initial metric values
        verifyTimerCount(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0);
        verifyTimerQuantile(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0.5, value -> value == 0);

        // Create the task
        DownloadTask task = createDownloadTask();

        // Execute the task and verify that the exception is propagated
        IOException thrown = assertThrows(
            IOException.class,
            task::call,
            "DownloadTask should propagate IOException"
        );

        // Verify it's the same exception
        assertEquals(testException, thrown, "The thrown exception should be the same as the original exception");

        // Verify the metrics
        verifyTimerCount(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 1);
        verifyTimerQuantile(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0.5, value -> value > 0);
    }

    @Test
    public void testDownloadWithRuntimeException() throws IOException {
        // Set up mock to throw RuntimeException
        RuntimeException testException = new RuntimeException("Test exception");
        when(mockDataFetcher.fetchSegmentData(metadata)).thenThrow(testException);

        // Verify initial metric values
        verifyTimerCount(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0);
        verifyTimerQuantile(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0.5, value -> value == 0);

        // Create the task
        DownloadTask task = createDownloadTask();

        // Execute the task and verify that the exception is propagated
        RuntimeException thrown = assertThrows(
            RuntimeException.class,
            task::call,
            "DownloadTask should propagate RuntimeException"
        );

        // Verify it's the same exception
        assertEquals(testException, thrown, "The thrown exception should be the same as the original exception");

        // Verify the metrics
        verifyTimerCount(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 1);
        verifyTimerQuantile(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0.5, value -> value > 0);
    }

    @Test
    public void testDownloadWithZeroFileLength() throws IOException {
        // Set up mock behavior
        when(mockDataFetcher.fetchSegmentData(metadata)).thenReturn(mockInputStream);
        when(mockDataFetcher.fileLength(metadata)).thenReturn(0L);

        // Verify initial metric values
        verifyTimerCount(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0);
        verifyTimerQuantile(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0.5, value -> value == 0);

        // Create the task
        DownloadTask task = createDownloadTask();

        // Execute the task and verify that an IOException is thrown
        IOException thrown = assertThrows(
            IOException.class,
            task::call,
            "DownloadTask should throw IOException when file length is zero"
        );

        // Verify the exception message
        String expectedMessage = "File size for segmentId: " + segmentId + " is not a positive number";
        assertEquals(expectedMessage, thrown.getMessage(), "The exception message should indicate zero file length");

        // Verify the metrics
        verifyTimerCount(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 1);
        verifyTimerQuantile(PREFETCH_SEGMENT_DOWNLOAD_RATE_AND_TIME_MS, 0.5, value -> value > 0);
    }

    @Test
    public void testThrottlingWhenQuotaExceeded() throws Exception {
        // Setup the quota manager to return throttle time initially and then no throttle time
        when(rlmQuotaManager.getThrottleTimeMs())
            .thenReturn(1000L) // First call returns throttle time
            .thenReturn(500L)  // Second call still has throttle time
            .thenReturn(0L);   // Third call has no throttle time (proceed with download)

        // Setup the file size and successful download
        long fileSize = 1024L;
        when(mockDataFetcher.fileLength(metadata)).thenReturn(fileSize);

        // Setup a mock result
        FileChannel mockChannel = mock(FileChannel.class);
        DownloadTask task = setupSuccessfulDownload(mockChannel, fileSize);

        // Execute the task
        FileChannel result = task.call();

        // Verify throttling behavior
        verify(rlmQuotaManager, times(3)).getThrottleTimeMs();
        verify(lockCondition, times(2)).await(500, TimeUnit.MILLISECONDS);
        verify(lockCondition, times(1)).signalAll();

        // Verify quota was reserved
        verify(rlmQuotaManager).record(fileSize);

        // Verify the download occurred and returned the expected result
        assertEquals(mockChannel, result);
    }

    @Test
    public void testNoThrottlingWhenQuotaAvailable() throws Exception {
        // Setup the quota manager to return no throttle time
        when(rlmQuotaManager.getThrottleTimeMs()).thenReturn(0L);

        // Setup the file size and successful download
        long fileSize = 1024L;
        when(mockDataFetcher.fileLength(metadata)).thenReturn(fileSize);

        // Setup a mock result
        FileChannel mockChannel = mock(FileChannel.class);
        DownloadTask task = setupSuccessfulDownload(mockChannel, fileSize);

        // Execute the task
        FileChannel result = task.call();

        // Verify throttling behavior
        verify(rlmQuotaManager, times(1)).getThrottleTimeMs();
        verify(lockCondition, never()).await(anyLong(), any(TimeUnit.class));
        verify(lockCondition, times(1)).signalAll();

        // Verify quota was reserved
        verify(rlmQuotaManager).record(fileSize);

        // Verify the download occurred and returned the expected result
        assertEquals(mockChannel, result);
    }

    @Test
    public void testThrottlingWithInterruption() throws Exception {
        // Setup the quota manager to always return throttle time
        when(rlmQuotaManager.getThrottleTimeMs()).thenReturn(1000L);

        // Setup the lockCondition to throw InterruptedException
        doThrow(new InterruptedException()).when(lockCondition).await(anyLong(), any(TimeUnit.class));

        // Execute the task and expect an InterruptedException
        DownloadTask task = createDownloadTask();
        assertThrows(InterruptedException.class, task::call);

        // Verify throttling behavior
        verify(rlmQuotaManager, times(1)).getThrottleTimeMs();
        verify(lockCondition, times(1)).await(500, TimeUnit.MILLISECONDS);

        // Verify quota was not reserved
        verify(rlmQuotaManager, never()).record(anyLong());

        // Verify the lock was released
        verify(lock, times(1)).unlock();
    }

    private byte[] createSequentialTestData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    private long stubFetchAndFileLength(byte[] testData) throws IOException {
        when(mockDataFetcher.fetchSegmentData(metadata)).thenReturn(mockInputStream);
        long fileSize = LogSegmentDataHeader.LENGTH + testData.length;
        when(mockDataFetcher.fileLength(metadata)).thenReturn(fileSize);
        return fileSize;
    }

    // Helper method to set up a successful download
    private DownloadTask setupSuccessfulDownload(FileChannel mockChannel, long fileSize) throws Exception {
        DownloadTask.Result mockResult = DownloadTask.Result.success(mockChannel, fileSize);

        DownloadTask spyTask = spy(createDownloadTask());
        doReturn(mockResult).when(spyTask).downloadSegment();

        return spyTask;
    }

    // Helper method to create a DownloadTask
    private DownloadTask createDownloadTask() {
        return new DownloadTask(
            mockTime,
            tempDir.toString(),
            mockDataFetcher,
            metrics,
            metadata,
            rlmQuotaManager,
            lock,
            lockCondition
        );
    }

    private void verifyTimerCount(String name, long expectedValue) {
        RSMTestUtils.verifyTimerCount(PrefetchEnabledHDFSRemoteStorageManager.class, name, expectedValue);
    }

    private void verifyTimerQuantile(String name, double quantile, Predicate<Double> assertion) {
        RSMTestUtils.verifyTimerQuantile(PrefetchEnabledHDFSRemoteStorageManager.class, name, Collections.emptyMap(), quantile, assertion);
    }
}