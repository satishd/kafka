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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.rsm.hdfs.DataFetcher;
import org.apache.kafka.rsm.hdfs.FileSystemManager;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics;
import org.apache.kafka.rsm.hdfs.PrefetchEnabledHDFSRemoteStorageManager;
import org.apache.kafka.rsm.hdfs.RSMUtils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_DOWNLOAD_DIRECTORY_FILE_COUNT;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_DOWNLOAD_DIRECTORY_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_REQUESTS_PER_SEC;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_REQUEST_FAILURE_PER_SEC;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_REQUEST_SUCCESS_PER_SEC;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_THREADPOOL_EXECUTOR_REJECTION_PER_SEC;
import static org.apache.kafka.rsm.hdfs.prefetch.RSMTestUtils.clearKafkaMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrefetchSegmentManagerTest {

    static final String HDFS_BASE_DIR = "/user/kloak/";
    private PrefetchSegmentManager segmentManager;
    private DataFetcher mockDataFetcher;
    private RemoteLogSegmentMetadata metadata;
    private RemoteLogSegmentId segmentId;
    private Map<String, Object> configs;
    private HDFSRemoteStorageManagerMetrics metrics;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setup() {
        mockDataFetcher = mock(DataFetcher.class);
        metrics = new HDFSRemoteStorageManagerMetrics();
        segmentManager = new PrefetchSegmentManager(new FileSystemManager(), metrics);
        segmentManager.setDataFetcher(mockDataFetcher);

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

        // Create a common configuration
        configs = new HashMap<>();
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, HDFS_BASE_DIR);
        configs.put(HDFSRemoteStorageManagerConfig.PREFETCH_LOCAL_BASE_DIR_CONFIG, tempDir.toString());
        configs.put(HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_CORE_SIZE_CONFIG, 2);
        configs.put(HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_MAX_SIZE_CONFIG, 4);
        configs.put(HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_QUEUE_CAPACITY_CONFIG, 10);
        configs.put(HDFSRemoteStorageManagerConfig.PREFETCH_CACHE_MAX_SIZE_CONFIG, 100);
        configs.put(HDFSRemoteStorageManagerConfig.PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_CONFIG, 30);
    }

    @Test
    public void testConfigure() {
        segmentManager.configure(configs);

        // Verify that the local base directory was created
        File localBaseDir = new File(tempDir.toString());
        assertTrue(localBaseDir.exists());
        assertTrue(localBaseDir.isDirectory());
    }

    @Test
    public void testFetchLogSegmentWithCacheMiss() throws IOException {
        segmentManager.configure(configs);

        // Call the method under test with a segment ID that's not in the cache
        InputStream result = segmentManager.fetchLogSegment(segmentId, 0, Integer.MAX_VALUE);

        // Verify the result
        assertNull(result, "Should return null for cache miss");
    }

    @Test
    public void testFetchLogSegmentWithCacheHit() throws Exception {
        segmentManager.configure(configs);

        // Create a test file and add it to the cache
        String filePath = RSMUtils.segmentPrefetchPath(tempDir.toString(), metadata);
        File file = new File(filePath);
        file.getParentFile().mkdirs();
        FileChannel fileChannel = FileChannel.open(Paths.get(filePath),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ);

        byte[] data = {1, 2, 3, 4};
        RSMTestUtils.writeData(fileChannel, data);

        Cache<RemoteLogSegmentId, CacheValue> cache = Caffeine.newBuilder().build();
        cache.put(segmentId, new CacheValue(PrefetchStatus.SUCCESS, Paths.get(filePath), fileChannel));
        segmentManager.setSegmentCache(cache);

        // Call the method under test
        InputStream result = segmentManager.fetchLogSegment(segmentId, 0, Integer.MAX_VALUE);

        // Verify the result
        assertNotNull(result, "Should return an InputStream for cache hit");
        byte[] buffer = new byte[4];
        assertEquals(4, result.read(buffer));
        assertEquals(1, buffer[0]);
        assertEquals(2, buffer[1]);
        assertEquals(3, buffer[2]);
        assertEquals(4, buffer[3]);
    }

    @Test
    public void testDownloadSegment() throws Exception {
        clearKafkaMetrics();
        segmentManager.configure(configs);

        // Verify initial metrics
        verifyMeter(PREFETCH_REQUESTS_PER_SEC, 0);

        Cache<RemoteLogSegmentId, CacheValue> cache = Caffeine.newBuilder().build();
        segmentManager.setSegmentCache(cache);

        // Set up the mock data fetcher to return a test input stream
        FSDataInputStream mockFSDataInputStream = mock(FSDataInputStream.class);
        when(mockDataFetcher.fetchSegmentData(any())).thenReturn(mockFSDataInputStream);
        when(mockDataFetcher.fileLength(any())).thenReturn(4L);
        segmentManager.setDataFetcher(mockDataFetcher);

        // Call the method under test
        segmentManager.downloadSegment(metadata);

        // Verify that the segment was added to the cache with IN_PROGRESS status
        CacheValue cacheValue = cache.getIfPresent(segmentId);
        assertNotNull(cacheValue, "Segment should be added to cache");
        assertEquals(PrefetchStatus.IN_PROGRESS, cacheValue.status(), "Status should be IN_PROGRESS");

        // Verify the metrics
        verifyMeter(PREFETCH_REQUESTS_PER_SEC, 1);
    }

    @Test
    public void testCleanup() {
        segmentManager.configure(configs);

        // Call the method under test
        segmentManager.cleanup();

        // Verify that the cleanup does not throw any exception
        assertTrue(true, "Cleanup method executed without errors.");
    }

    @Test
    public void testCleanupWithActiveCache() {
        segmentManager.configure(configs);

        // Populate the cache with dummy data
        Cache<RemoteLogSegmentId, CacheValue> cache = Caffeine.newBuilder().build();
        RemoteLogSegmentId dummySegmentId = new RemoteLogSegmentId(new TopicIdPartition(Uuid.randomUuid(), 0, "dummy"), Uuid.randomUuid());
        cache.put(dummySegmentId, new CacheValue(PrefetchStatus.IN_PROGRESS, null, null));
        segmentManager.setSegmentCache(cache);

        assertNotNull(cache.getIfPresent(dummySegmentId), "Cache should contain the dummy segment.");

        // Call the cleanup method
        segmentManager.cleanup();

        // Verify that the cache is empty after cleanup
        assertNull(cache.getIfPresent(dummySegmentId), "Cache should be empty after cleanup.");
    }

    @Test
    public void testCleanupWithThreadPoolExecutor() {
        segmentManager.configure(configs);

        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        segmentManager.setThreadPoolExecutor(threadPoolExecutor);

        // Call the cleanup method
        segmentManager.cleanup();

        // Verify that the thread pool executor is shut down
        assertTrue(threadPoolExecutor.isShutdown(), "Thread pool executor should be shut down after cleanup.");
    }

    @Test
    public void testDownloadSegmentWithException() throws Exception {
        clearKafkaMetrics();
        segmentManager.configure(configs);

        // Verify initial metrics
        verifyMeter(PREFETCH_REQUESTS_PER_SEC, 0);
        verifyMeter(PREFETCH_REQUEST_SUCCESS_PER_SEC, 0);
        verifyMeter(PREFETCH_REQUEST_FAILURE_PER_SEC, 0);

        Cache<RemoteLogSegmentId, CacheValue> cache = Caffeine.newBuilder().build();
        segmentManager.setSegmentCache(cache);

        // Set up the mock data fetcher to throw an exception
        // First mock fileLength to return a positive value
        when(mockDataFetcher.fileLength(any())).thenReturn(1024L);

        // Then mock fetchSegmentData to throw an exception
        IOException testException = new IOException("Test download exception");
        CountDownLatch exceptionTriggerLatch = new CountDownLatch(1);
        when(mockDataFetcher.fetchSegmentData(any())).thenAnswer(invocation -> {
            exceptionTriggerLatch.await();
            throw testException;
        });
        segmentManager.setDataFetcher(mockDataFetcher);

        // Call the method under test
        segmentManager.downloadSegment(metadata);

        // Verify that the segment was initially added to the cache with IN_PROGRESS status
        CacheValue initialCacheValue = cache.getIfPresent(segmentId);
        assertNotNull(initialCacheValue, "Segment should be added to cache");
        assertEquals(PrefetchStatus.IN_PROGRESS, initialCacheValue.status(), "Status should be IN_PROGRESS");

        // Unblock the download task to trigger the exception
        exceptionTriggerLatch.countDown();

        // Wait for the segment to be removed from cache after exception
        assertTimeoutPreemptively(Duration.ofMillis(200), () -> {
            while (true) {
                if (cache.getIfPresent(segmentId) == null) {
                    // Segment entry is removed from the cache when download fails with exception
                    return;
                }
                Thread.sleep(10);
            }
        });

        verifyMeter(PREFETCH_REQUESTS_PER_SEC, 1);
        verifyMeter(PREFETCH_REQUEST_SUCCESS_PER_SEC, 0);
        verifyMeter(PREFETCH_REQUEST_FAILURE_PER_SEC, 1);
    }

    @Test
    public void testDownloadSegmentSuccess() throws Exception {
        clearKafkaMetrics();
        segmentManager.configure(configs);

        // Verify initial metrics
        verifyMeter(PREFETCH_REQUESTS_PER_SEC, 0);
        verifyMeter(PREFETCH_REQUEST_SUCCESS_PER_SEC, 0);
        verifyMeter(PREFETCH_REQUEST_FAILURE_PER_SEC, 0);

        Cache<RemoteLogSegmentId, CacheValue> cache = Caffeine.newBuilder().build();
        segmentManager.setSegmentCache(cache);

        // Set up the mock data fetcher to return a test input stream
        FSDataInputStream mockFSDataInputStream = mock(FSDataInputStream.class);
        when(mockDataFetcher.fetchSegmentData(any())).thenReturn(mockFSDataInputStream);
        when(mockDataFetcher.fileLength(any())).thenReturn(4L);

        // Mock read behavior to return EOF immediately
        when(mockFSDataInputStream.read(anyLong(), any(byte[].class), anyInt(), anyInt())).thenReturn(-1);

        // Create a custom ThreadPoolExecutor that will execute the task immediately in the current thread
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable command) {
                command.run();
            }
        };
        segmentManager.setThreadPoolExecutor(executor);
        segmentManager.setDataFetcher(mockDataFetcher);

        // Call the method under test
        segmentManager.downloadSegment(metadata);

        // Verify that the segment was added to the cache with SUCCESS status
        CacheValue cacheValue = cache.getIfPresent(segmentId);
        assertNotNull(cacheValue, "Segment should be added to cache");
        assertEquals(PrefetchStatus.SUCCESS, cacheValue.status(), "Status should be SUCCESS");

        // Verify the metrics
        verifyMeter(PREFETCH_REQUESTS_PER_SEC, 1);
        verifyMeter(PREFETCH_REQUEST_SUCCESS_PER_SEC, 1);
        verifyMeter(PREFETCH_REQUEST_FAILURE_PER_SEC, 0);
    }

    @Test
    public void testRejectedExecution() throws IOException, InterruptedException {
        clearKafkaMetrics();
        configs.put(HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_CORE_SIZE_CONFIG, 1);
        configs.put(HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_MAX_SIZE_CONFIG, 1);
        configs.put(HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_QUEUE_CAPACITY_CONFIG, 1);
        segmentManager.configure(configs);

        // Verify initial metrics
        verifyMeter(PREFETCH_THREADPOOL_EXECUTOR_REJECTION_PER_SEC, 0L);

        // Verify initial metrics
        verifyMeter(PREFETCH_REQUESTS_PER_SEC, 0);
        verifyMeter(PREFETCH_REQUEST_SUCCESS_PER_SEC, 0);
        verifyMeter(PREFETCH_REQUEST_FAILURE_PER_SEC, 0);

        Cache<RemoteLogSegmentId, CacheValue> cache = Caffeine.newBuilder().build();
        segmentManager.setSegmentCache(cache);

        // Set up the mock data fetcher to block indefinitely
        when(mockDataFetcher.fileLength(any())).thenReturn(1024L);

        // Create a latch to signal when the first task has started executing
        CountDownLatch taskStartedLatch = new CountDownLatch(1);
        
        // Submit a task that blocks the executor
        CountDownLatch blockingLatch = new CountDownLatch(1);
        FSDataInputStream mockFSDataInputStream = mock(FSDataInputStream.class);
        when(mockDataFetcher.fetchSegmentData(any())).thenAnswer(invocation -> {
            // Signal that the task has started executing
            taskStartedLatch.countDown();
            try {
                blockingLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return mockFSDataInputStream;
        });
        segmentManager.setDataFetcher(mockDataFetcher);

        // Call the method under test, this should use up the only thread of threadpool
        segmentManager.downloadSegment(metadata);

        // Wait for the first task to start executing
        assertTrue(taskStartedLatch.await(5, TimeUnit.SECONDS), "First task did not start executing within timeout");

        // Verify the metrics is still 0
        verifyMeter(PREFETCH_THREADPOOL_EXECUTOR_REJECTION_PER_SEC, 0L);

        // Submit a second task that will go into the queue
        RemoteLogSegmentId secondSegment = new RemoteLogSegmentId(segmentId.topicIdPartition(), Uuid.randomUuid());
        Map<Integer, Long> secondSegmentLeaderEpochs = Collections.singletonMap(5, 100L);
        RemoteLogSegmentMetadata secondMetadata = new RemoteLogSegmentMetadata(
            secondSegment,
            0L,
            100L,
            1000L,
            1,
            System.currentTimeMillis(),
            1024,
            Optional.empty(),
            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
            secondSegmentLeaderEpochs
        );
        segmentManager.downloadSegment(secondMetadata);

        // Verify the metrics is still 0 after the second task (it should be queued, not rejected)
        verifyMeter(PREFETCH_THREADPOOL_EXECUTOR_REJECTION_PER_SEC, 0L);

        // Submit a third task that should be rejected since both the thread and queue are full
        RemoteLogSegmentId thirdSegment = new RemoteLogSegmentId(segmentId.topicIdPartition(), Uuid.randomUuid());
        Map<Integer, Long> thirdSegmentLeaderEpochs = Collections.singletonMap(5, 100L);
        RemoteLogSegmentMetadata thirdMetadata = new RemoteLogSegmentMetadata(
            thirdSegment,
            0L,
            100L,
            1000L,
            1,
            System.currentTimeMillis(),
            1024,
            Optional.empty(),
            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
            thirdSegmentLeaderEpochs
        );
        segmentManager.downloadSegment(thirdMetadata);
        
        // Verify the rejection count metric is incremented after the third task
        verifyMeter(PREFETCH_THREADPOOL_EXECUTOR_REJECTION_PER_SEC, 1L);

        // Clean up
        blockingLatch.countDown();
    }

    @Test
    public void testDirectoryFileCountAndSizeMetrics() throws IOException {
        // Clear Kafka metrics
        clearKafkaMetrics();

        // Configure the segment manager
        segmentManager.configure(configs);

        // Verify initial metrics (should be 0 since the directory is empty)
        verifyGauge(PREFETCH_DOWNLOAD_DIRECTORY_FILE_COUNT, 0);
        verifyGauge(PREFETCH_DOWNLOAD_DIRECTORY_SIZE, 0L);

        // Create test files in the download directory
        File downloadDir = new File(tempDir.toString());

        // Create first test file with known content
        File file1 = new File(downloadDir, "test-file-1.txt");
        byte[] content1 = new byte[100]; // 100 bytes
        for (int i = 0; i < content1.length; i++) {
            content1[i] = (byte) i;
        }
        Files.write(file1.toPath(), content1);

        // Verify metrics after adding one file
        verifyGauge(PREFETCH_DOWNLOAD_DIRECTORY_FILE_COUNT, 1);
        verifyGauge(PREFETCH_DOWNLOAD_DIRECTORY_SIZE, 100L);

        // Create second test file with different content
        File file2 = new File(downloadDir, "test-file-2.txt");
        byte[] content2 = new byte[200]; // 200 bytes
        for (int i = 0; i < content2.length; i++) {
            content2[i] = (byte) (i % 256);
        }
        Files.write(file2.toPath(), content2);

        // Verify metrics after adding second file
        verifyGauge(PREFETCH_DOWNLOAD_DIRECTORY_FILE_COUNT, 2);
        verifyGauge(PREFETCH_DOWNLOAD_DIRECTORY_SIZE, 300L);

        // Delete first file
        file1.delete();

        // Verify metrics after deleting first file
        verifyGauge(PREFETCH_DOWNLOAD_DIRECTORY_FILE_COUNT, 1);
        verifyGauge(PREFETCH_DOWNLOAD_DIRECTORY_SIZE, 200L);
    }

    private void verifyMeter(String name, long expectedCount) {
        RSMTestUtils.verifyMeter(PrefetchEnabledHDFSRemoteStorageManager.class, name, Collections.emptyMap(), expectedCount);
    }

    private <T> void verifyGauge(String name, T expectedValue) {
        RSMTestUtils.verifyGauge(PrefetchEnabledHDFSRemoteStorageManager.class, name, expectedValue);
    }
}
