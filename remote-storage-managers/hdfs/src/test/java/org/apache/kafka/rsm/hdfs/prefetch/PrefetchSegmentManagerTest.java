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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.rsm.hdfs.DataFetcher;
import org.apache.kafka.rsm.hdfs.FileSystemManager;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
import java.util.stream.Stream;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_CACHE_MAX_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_LOCAL_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_CORE_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_MAX_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_QUEUE_CAPACITY_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_DOWNLOAD_DIRECTORY_FILE_COUNT;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_DOWNLOAD_DIRECTORY_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_REQUESTS_PER_SEC;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_REQUEST_FAILURE_PER_SEC;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_REQUEST_SUCCESS_PER_SEC;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_SEGMENT_READS_PER_SEC;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_THREADPOOL_EXECUTOR_REJECTION_PER_SEC;
import static org.apache.kafka.rsm.hdfs.prefetch.RSMTestUtils.clearKafkaMetrics;
import static org.apache.kafka.server.config.ServerLogConfigs.LOG_DIR_CONFIG;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManagerConfig.METRICS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        configs.put(HDFS_BASE_DIR_PROP, HDFS_BASE_DIR);
        configs.put(PREFETCH_LOCAL_BASE_DIR_PROP, tempDir.toString());
        configs.put(PREFETCH_THREAD_POOL_CORE_SIZE_PROP, 2);
        configs.put(PREFETCH_THREAD_POOL_MAX_SIZE_PROP, 4);
        configs.put(PREFETCH_THREAD_POOL_QUEUE_CAPACITY_PROP, 10);
        configs.put(PREFETCH_CACHE_MAX_SIZE_PROP, 100);
        configs.put(PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_PROP, 30);
        configs.put(METRICS, new Metrics());
        // Ensure log dir is set and different from local prefetch dir
        configs.put(LOG_DIR_CONFIG, tempDir.resolve("kafka-logs").toString());
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
        clearKafkaMetrics();
        segmentManager.configure(configs);

        // Verify initial metrics
        verifyMeter(PREFETCH_SEGMENT_READS_PER_SEC, 0);

        // Call the method under test with a segment ID that's not in the cache
        InputStream result = segmentManager.fetchLogSegment(segmentId, 0, Integer.MAX_VALUE);

        // Verify the result
        assertNull(result, "Should return null for cache miss");

        // Verify segments reads not recorded
        verifyMeter(PREFETCH_SEGMENT_READS_PER_SEC, 0);
    }

    @Test
    public void testFetchLogSegmentWithCacheHit() throws Exception {
        clearKafkaMetrics();
        segmentManager.configure(configs);

        // Verify initial metrics
        verifyMeter(PREFETCH_SEGMENT_READS_PER_SEC, 0);

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

        // Verify segment reads is recorded
        verifyMeter(PREFETCH_SEGMENT_READS_PER_SEC, 1);
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
        configs.put(PREFETCH_THREAD_POOL_CORE_SIZE_PROP, 1);
        configs.put(PREFETCH_THREAD_POOL_MAX_SIZE_PROP, 1);
        configs.put(PREFETCH_THREAD_POOL_QUEUE_CAPACITY_PROP, 1);
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

    /**
     * Tests the validation logic for reconfiguring thread pool properties in the segment manager.
     *
     * @param prop           the configuration property to be validated.
     * @param value          the value to set for the given property.
     * @param currentCoreSize the current core pool size to set up in the executor.
     * @param currentMaxSize  the current max pool size to set up in the executor.
     * @param passValidation indicates whether the provided configuration should pass validation.
     * @param failureReason   description of why validation fails, or empty if validation passes.
     */
    @ParameterizedTest
    @CsvSource({
        // Valid core size updates
        PREFETCH_THREAD_POOL_CORE_SIZE_PROP + ",2,2,4,true,''",
        PREFETCH_THREAD_POOL_CORE_SIZE_PROP + ",3,2,4,true,''",
        PREFETCH_THREAD_POOL_CORE_SIZE_PROP + ",4,2,4,true,''",

        // Invalid core size (less than half of current)
        PREFETCH_THREAD_POOL_CORE_SIZE_PROP + ",0,2,4,false,'value should be at least half the current value'",

        // Invalid core size (more than double of current)
        PREFETCH_THREAD_POOL_CORE_SIZE_PROP + ",5,2,4,false,'value should not be greater than double the current value'",

        // Invalid core size (greater than max)
        PREFETCH_THREAD_POOL_CORE_SIZE_PROP + ",6,3,4,false,'core pool size (6) cannot be greater than maximum pool size (4)'",

        // Valid max size updates
        PREFETCH_THREAD_POOL_MAX_SIZE_PROP + ",4,2,4,true,''",
        PREFETCH_THREAD_POOL_MAX_SIZE_PROP + ",6,2,4,true,''",
        PREFETCH_THREAD_POOL_MAX_SIZE_PROP + ",8,2,4,true,''",

        // Invalid max size (less than half of current)
        PREFETCH_THREAD_POOL_MAX_SIZE_PROP + ",1,1,4,false,'value should be at least half the current value'",

        // Invalid max size (more than double of current)
        PREFETCH_THREAD_POOL_MAX_SIZE_PROP + ",10,2,4,false,'value should not be greater than double the current value'",

        // Invalid max size (less than core)
        PREFETCH_THREAD_POOL_MAX_SIZE_PROP + ",2,3,4,false,'core pool size (3) cannot be greater than maximum pool size (2)'"
    })
    public void testValidateReconfiguration(String prop, String value, int currentCoreSize, int currentMaxSize, boolean passValidation, String failureReason) {
        segmentManager.configure(configs);

        // Set up a ThreadPoolExecutor with the specified core and max sizes
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
            currentCoreSize, currentMaxSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        segmentManager.setThreadPoolExecutor(threadPoolExecutor);

        Map<String, Object> configs = new HashMap<>();
        configs.put(prop, value);

        if (passValidation) {
            assertDoesNotThrow(() -> segmentManager.validateReconfiguration(configs));
        } else {
            ConfigException exception = assertThrows(ConfigException.class,
                () -> segmentManager.validateReconfiguration(configs));
            assertTrue(exception.getMessage().contains(failureReason),
                "Expected error message to contain '" + failureReason + "', but was: " + exception.getMessage());
        }
    }

    /**
     * Tests validation of core and max pool size when both are updated at the same time.
     */
    @ParameterizedTest
    @CsvSource({
        // Both valid and core <= max
        "3,6,2,4,true,''",
        // Both valid but core > max
        "4,3,2,4,false,'core pool size (4) cannot be greater than maximum pool size (3)'",
        // Core invalid range, max valid
        "0,6,2,4,false,'value should be at least half the current value'",
        // Core valid, max invalid range
        "3,10,2,4,false,'value should not be greater than double the current value'",
        // Both invalid range
        "0,10,2,4,false,'value should be at least half the current value'"
    })
    public void testValidateReconfigurationBothProperties(String coreValue, String maxValue,
                                                          int currentCoreSize, int currentMaxSize,
                                                          boolean passValidation, String failureReason) {
        segmentManager.configure(configs);

        // Set up a ThreadPoolExecutor with the specified core and max sizes
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
            currentCoreSize, currentMaxSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        segmentManager.setThreadPoolExecutor(threadPoolExecutor);

        Map<String, Object> configs = new HashMap<>();
        configs.put(PREFETCH_THREAD_POOL_CORE_SIZE_PROP, coreValue);
        configs.put(PREFETCH_THREAD_POOL_MAX_SIZE_PROP, maxValue);

        if (passValidation) {
            assertDoesNotThrow(() -> segmentManager.validateReconfiguration(configs));
        } else {
            ConfigException exception = assertThrows(ConfigException.class,
                () -> segmentManager.validateReconfiguration(configs));
            assertTrue(exception.getMessage().contains(failureReason),
                "Expected error message to contain '" + failureReason + "', but was: " + exception.getMessage());
        }
    }

    @Test
    public void testConfigureThrowsWhenLogDirMissing() {
        // Missing LOG_DIR_CONFIG should cause IllegalArgumentException
        Map<String, Object> badConfigs = new HashMap<>(configs);
        badConfigs.remove(LOG_DIR_CONFIG);

        assertThrows(IllegalArgumentException.class, () -> segmentManager.configure(badConfigs));
    }

    @Test
    public void testConfigureThrowsWhenLocalBaseDirSameAsLogDir(@TempDir Path tmp) {
        Map<String, Object> sameDirConfigs = new HashMap<>(configs);
        String dir = tmp.toString();
        sameDirConfigs.put(LOG_DIR_CONFIG, dir);
        sameDirConfigs.put(PREFETCH_LOCAL_BASE_DIR_PROP, dir);

        assertThrows(ConfigException.class, () -> segmentManager.configure(sameDirConfigs));
    }

    @Test
    public void testConfigureCreatesAndCleansLocalPrefetchDir(@TempDir Path tmp) throws IOException {
        // Create a sub-dir for prefetch and a stale file inside it
        Path prefetchDir = tmp.resolve("prefetch");
        Files.createDirectories(prefetchDir);
        Path stale = prefetchDir.resolve("stale.dat");
        Files.write(stale, new byte[]{1, 2, 3});
        // Sanity: file exists
        assertTrue(Files.exists(stale));
        assertTrue(fileCount(prefetchDir) > 0);

        Map<String, Object> cfg = new HashMap<>(configs);
        cfg.put(LOG_DIR_CONFIG, tmp.resolve("logdir").toString()); // ensure not equal to prefetch dir
        cfg.put(PREFETCH_LOCAL_BASE_DIR_PROP, prefetchDir.toString());

        // Should not throw and should delete stale files
        assertDoesNotThrow(() -> segmentManager.configure(cfg));
        assertTrue(Files.exists(prefetchDir));
        assertEquals(0, fileCount(prefetchDir));
    }

    private long fileCount(Path directory) throws IOException {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream.count();
        }
    }

    @Test
    public void testReconfigure() {
        segmentManager.configure(configs);

        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        segmentManager.setThreadPoolExecutor(threadPoolExecutor);

        assertEquals(2, threadPoolExecutor.getCorePoolSize());
        assertEquals(2, threadPoolExecutor.getMaximumPoolSize());

        Map<String, Object> newConfigs = new HashMap<>();
        newConfigs.put(PREFETCH_THREAD_POOL_CORE_SIZE_PROP, "1");
        newConfigs.put(PREFETCH_THREAD_POOL_MAX_SIZE_PROP, "4");

        segmentManager.reconfigure(newConfigs);
        assertEquals(1, threadPoolExecutor.getCorePoolSize());
        assertEquals(4, threadPoolExecutor.getMaximumPoolSize());
    }

    private void verifyMeter(String name, long expectedCount) {
        RSMTestUtils.verifyMeter(PrefetchEnabledHDFSRemoteStorageManager.class, name, Collections.emptyMap(), expectedCount);
    }

    private <T> void verifyGauge(String name, T expectedValue) {
        RSMTestUtils.verifyGauge(PrefetchEnabledHDFSRemoteStorageManager.class, name, expectedValue);
    }
}
