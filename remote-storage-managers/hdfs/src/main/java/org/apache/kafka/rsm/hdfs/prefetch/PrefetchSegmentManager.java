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

import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.rsm.hdfs.DataFetcher;
import org.apache.kafka.rsm.hdfs.FileSystemManager;
import org.apache.kafka.rsm.hdfs.HDFSDataFetcher;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig;
import org.apache.kafka.rsm.hdfs.RSMUtils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_CONFIG;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_CACHE_MAX_SIZE_CONFIG;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_LOCAL_BASE_DIR_CONFIG;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_CORE_SIZE_CONFIG;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_MAX_SIZE_CONFIG;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_QUEUE_CAPACITY_CONFIG;

public class PrefetchSegmentManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrefetchSegmentManager.class);

    private final Time time = Time.SYSTEM;
    private final FileSystemManager fileSystemManager;

    private DataFetcher dataFetcher;
    private String localBaseDir;
    private ThreadPoolExecutor threadPoolExecutor;
    private Cache<RemoteLogSegmentId, CacheValue> segmentCache;

    public PrefetchSegmentManager(FileSystemManager fileSystemManager) {
        this.fileSystemManager = fileSystemManager;
    }

    public void configure(Map<String, ?> configs) {
        HDFSRemoteStorageManagerConfig conf = new HDFSRemoteStorageManagerConfig(configs, true);

        String hadoopBaseDir = conf.getString(HDFS_BASE_DIR_PROP);
        this.dataFetcher = new HDFSDataFetcher(hadoopBaseDir, fileSystemManager);

        this.localBaseDir = conf.getString(PREFETCH_LOCAL_BASE_DIR_CONFIG);
        // Ensure the local base directory exists
        File baseDir = new File(localBaseDir);
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new RuntimeException("Unable to create directory: " + baseDir.getAbsolutePath());
        }

        int corePoolSize = conf.getInt(PREFETCH_THREAD_POOL_CORE_SIZE_CONFIG);
        int maxPoolSize = conf.getInt(PREFETCH_THREAD_POOL_MAX_SIZE_CONFIG);
        int queueCapacity = conf.getInt(PREFETCH_THREAD_POOL_QUEUE_CAPACITY_CONFIG);
        this.threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                ThreadUtils.createThreadFactory("remote-log-prefetch", false,
                        (t, e) -> LOGGER.error("Uncaught exception in thread '{}':", t.getName(), e)));

        int maxCacheSize = conf.getInt(PREFETCH_CACHE_MAX_SIZE_CONFIG);
        int expireAfterAccessMinutes = conf.getInt(PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_CONFIG);
        this.segmentCache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterAccess(expireAfterAccessMinutes, TimeUnit.MINUTES)
                .removalListener(new CacheRemovalListener())
                .build();
    }

    @VisibleForTesting
    void setDataFetcher(DataFetcher dataFetcher) {
        this.dataFetcher = dataFetcher;
    }

    @VisibleForTesting
    void setSegmentCache(Cache<RemoteLogSegmentId, CacheValue> segmentCache) {
        this.segmentCache = segmentCache;
    }

    @VisibleForTesting
    void setThreadPoolExecutor(ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    public InputStream fetchLogSegment(RemoteLogSegmentId segmentId, int startPosition, int endPosition) throws IOException {
        CacheValue cacheValue = segmentCache.getIfPresent(segmentId);
        if (cacheValue == null || cacheValue.status() != PrefetchStatus.SUCCESS) {
            return null; // Cache miss or segment not successfully downloaded
        }
        return RSMUtils.getInputStreamFromChannel(cacheValue.fileChannel(), startPosition, endPosition);
    }

    public void downloadSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
        CacheValue existingValue = segmentCache.asMap().putIfAbsent(
            remoteLogSegmentId,
            new CacheValue(PrefetchStatus.IN_PROGRESS, null, null)
        );

        if (existingValue == null) {
            // We successfully claimed this segment for download
            createDownloadTask(remoteLogSegmentMetadata);
        } else {
            LOGGER.debug("Segment {} is already downloaded or in progress, skipping download.", remoteLogSegmentId);
        }
    }

    public void cleanup() {
        segmentCache.invalidateAll();
        segmentCache.cleanUp();
        ThreadUtils.shutdownExecutorServiceQuietly(threadPoolExecutor, 5, TimeUnit.SECONDS);
    }

    private void createDownloadTask(RemoteLogSegmentMetadata segmentMetadata) {
        try {
            Task task = new Task(segmentMetadata);
            threadPoolExecutor.submit(task);
        } catch (Exception e) {
            LOGGER.error("Failed to submit download task for segment: {}", segmentMetadata.remoteLogSegmentId(), e);
        }
    }

    private void signalDownloadSuccess(RemoteLogSegmentMetadata segmentMetadata, FileChannel fileChannel) {
        Path filePath = Paths.get(RSMUtils.segmentPrefetchPath(localBaseDir, segmentMetadata));
        segmentCache.put(segmentMetadata.remoteLogSegmentId(), new CacheValue(PrefetchStatus.SUCCESS, filePath, fileChannel));
    }

    private void signalDownloadFailure(RemoteLogSegmentMetadata segmentMetadata) {
        segmentCache.invalidate(segmentMetadata.remoteLogSegmentId());
    }

    private class Task implements Runnable {
        private final RemoteLogSegmentMetadata remoteLogSegmentMetadata;
        private final DownloadTask downloadTask;

        public Task(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
            this.remoteLogSegmentMetadata = remoteLogSegmentMetadata;
            this.downloadTask = new DownloadTask(time, localBaseDir, dataFetcher, remoteLogSegmentMetadata);
        }

        @Override
        public void run() {
            try {
                FileChannel fileChannel = downloadTask.call();
                signalDownloadSuccess(remoteLogSegmentMetadata, fileChannel);
            } catch (Exception e) {
                LOGGER.error("Error while downloading segment: {}", downloadTask.getRemoteLogSegmentMetadata(), e);
                signalDownloadFailure(remoteLogSegmentMetadata);
            }
        }
    }
}
