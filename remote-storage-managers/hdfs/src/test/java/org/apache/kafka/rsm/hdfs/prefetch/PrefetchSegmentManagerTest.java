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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.rsm.hdfs.DataFetcher;
import org.apache.kafka.rsm.hdfs.FileSystemManager;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig;
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
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrefetchSegmentManagerTest {

    static final String HDFS_BASE_DIR = "/user/kloak/";
    private PrefetchSegmentManager segmentManager;
    private DataFetcher mockDataFetcher;
    private RemoteLogSegmentMetadata metadata;
    private RemoteLogSegmentId segmentId;
    private Map<String, Object> configs;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setup() {
        Time mockTime = new MockTime();
        mockDataFetcher = mock(DataFetcher.class);
        segmentManager = new PrefetchSegmentManager(new FileSystemManager(), mockTime);
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
        segmentManager.configure(configs);

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
        segmentManager.configure(configs);

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
    }
}
