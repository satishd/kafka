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
import kafka.log.remote.quota.RLMQuotaManagerConfig;
import kafka.server.QuotaType;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.rsm.hdfs.DataFetcher;
import org.apache.kafka.rsm.hdfs.FileSystemManager;
import org.apache.kafka.rsm.hdfs.HDFSDataFetcher;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics;
import org.apache.kafka.rsm.hdfs.RSMUtils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_CACHE_MAX_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_LOCAL_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_MAX_BYTES_PER_SECOND_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_QUOTA_WINDOW_NUM_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_QUOTA_WINDOW_SIZE_SECONDS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_CORE_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_MAX_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.PREFETCH_THREAD_POOL_QUEUE_CAPACITY_PROP;
import static org.apache.kafka.rsm.hdfs.RSMUtils.KLOAK_USER;
import static org.apache.kafka.server.config.ServerLogConfigs.LOG_DIR_CONFIG;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManagerConfig.METRICS;

public class PrefetchSegmentManager implements Reconfigurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrefetchSegmentManager.class);
    static final String PREFETCH_SUBDIRECTORY_NAME = "prefetch";

    private static final Set<String> DYNAMIC_CONFIGS = Utils.mkSet(
        PREFETCH_MAX_BYTES_PER_SECOND_PROP,
        PREFETCH_THREAD_POOL_CORE_SIZE_PROP,
        PREFETCH_THREAD_POOL_MAX_SIZE_PROP);

    private final Time time = Time.SYSTEM;
    private final FileSystemManager fileSystemManager;
    private final HDFSRemoteStorageManagerMetrics rsmMetrics;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition lockCondition = lock.newCondition();

    private RLMQuotaManager quotaManager;
    private DataFetcher dataFetcher;
    private String prefetchDir;
    private ThreadPoolExecutor threadPoolExecutor;
    private Cache<RemoteLogSegmentId, CacheValue> segmentCache;

    public PrefetchSegmentManager(FileSystemManager fileSystemManager, HDFSRemoteStorageManagerMetrics rsmMetrics) {
        this.fileSystemManager = fileSystemManager;
        this.rsmMetrics = rsmMetrics;
    }

    public void configure(Map<String, ?> configs) {
        HDFSRemoteStorageManagerConfig conf = new HDFSRemoteStorageManagerConfig(configs, true);

        String hadoopBaseDir = KLOAK_USER + conf.getString(HDFS_BASE_DIR_PROP);
        this.dataFetcher = new HDFSDataFetcher(hadoopBaseDir, fileSystemManager);

        String prefetchLocalBaseDir = conf.getString(PREFETCH_LOCAL_BASE_DIR_PROP);
        this.prefetchDir = localPrefetchDir(configs, prefetchLocalBaseDir);

        int corePoolSize = conf.getInt(PREFETCH_THREAD_POOL_CORE_SIZE_PROP);
        int maxPoolSize = conf.getInt(PREFETCH_THREAD_POOL_MAX_SIZE_PROP);
        int queueCapacity = conf.getInt(PREFETCH_THREAD_POOL_QUEUE_CAPACITY_PROP);
        this.threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(queueCapacity),
            ThreadUtils.createThreadFactory("remote-log-prefetch", false,
                (t, e) -> LOGGER.error("Uncaught exception in thread '{}':", t.getName(), e)));

        int maxCacheSize = conf.getInt(PREFETCH_CACHE_MAX_SIZE_PROP);
        int expireAfterAccessMinutes = conf.getInt(PREFETCH_CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES_PROP);
        this.segmentCache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterAccess(expireAfterAccessMinutes, TimeUnit.MINUTES)
                .removalListener(new CacheRemovalListener())
                .recordStats()
                .build();

        rsmMetrics.registerPrefetchMetrics(this.threadPoolExecutor, this.segmentCache, this.prefetchDir);

        RLMQuotaManagerConfig rlmQuotaManagerConfig = fetchQuotaManagerConfig(conf);
        Metrics metrics = (Metrics) configs.get(METRICS);
        this.quotaManager = new RLMQuotaManager(rlmQuotaManagerConfig, metrics, QuotaType.RLMPrefetch$.MODULE$,
            "Tracking prefetch byte-rate for Remote Log Manager", time);
    }

    String localPrefetchDir(Map<String, ?> configs, String localBaseDir) {
        String logDir = (String) configs.get(LOG_DIR_CONFIG);
        if (logDir == null || logDir.isEmpty()) {
            throw new IllegalArgumentException(String.format("Missing '%s' property", LOG_DIR_CONFIG));
        }

        if (localBaseDir.startsWith(logDir)) {
            throw new ConfigException("Local prefetch base directory: " + localBaseDir +
                " cannot be within the log directory: " + logDir);
        }

        Path prefetchDirPath = Paths.get(localBaseDir, PREFETCH_SUBDIRECTORY_NAME);

        // Clean up the existing directory if present
        if (Files.exists(prefetchDirPath)) {
            try {
                Utils.delete(prefetchDirPath.toFile());
            } catch (IOException e) {
                LOGGER.warn("Failed to delete stale prefetch directory: {}", prefetchDirPath, e);
            }
        }

        // Create the prefetch directory
        try {
            Files.createDirectories(prefetchDirPath);
        } catch (IOException e) {
            throw new RuntimeException("Unable to create directory: " + prefetchDirPath, e);
        }

        return prefetchDirPath.toString();
    }

    static RLMQuotaManagerConfig fetchQuotaManagerConfig(HDFSRemoteStorageManagerConfig config) {
        return new RLMQuotaManagerConfig(
            config.getLong(PREFETCH_MAX_BYTES_PER_SECOND_PROP),
            config.getInt(PREFETCH_QUOTA_WINDOW_NUM_PROP),
            config.getInt(PREFETCH_QUOTA_WINDOW_SIZE_SECONDS_PROP)
        );
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return DYNAMIC_CONFIGS;
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        int newCoreSize = this.threadPoolExecutor.getCorePoolSize();
        int newMaxSize = this.threadPoolExecutor.getMaximumPoolSize();

        // Update the new values if present in configs
        if (configs.containsKey(PREFETCH_THREAD_POOL_CORE_SIZE_PROP)) {
            newCoreSize = Integer.parseInt((String) configs.get(PREFETCH_THREAD_POOL_CORE_SIZE_PROP));
            RSMUtils.validateConfigValueRange(PREFETCH_THREAD_POOL_CORE_SIZE_PROP,
                this.threadPoolExecutor.getCorePoolSize(), newCoreSize);
        }

        if (configs.containsKey(PREFETCH_THREAD_POOL_MAX_SIZE_PROP)) {
            newMaxSize = Integer.parseInt((String) configs.get(PREFETCH_THREAD_POOL_MAX_SIZE_PROP));
            RSMUtils.validateConfigValueRange(PREFETCH_THREAD_POOL_MAX_SIZE_PROP,
                this.threadPoolExecutor.getMaximumPoolSize(), newMaxSize);
        }

        // Validate that core size is never greater than max size
        if (newCoreSize > newMaxSize) {
            throw new ConfigException(String.format(
                "Invalid thread pool configuration: core pool size (%d) cannot be greater than maximum pool size (%d)",
                newCoreSize, newMaxSize));
        }
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        String prefetchMaxBytesPerSecond = (String) configs.get(PREFETCH_MAX_BYTES_PER_SECOND_PROP);
        if (prefetchMaxBytesPerSecond != null) {
            this.quotaManager.updateQuota(new Quota(Long.parseLong(prefetchMaxBytesPerSecond), true));
        }
        String corePoolSize = (String) configs.get(PREFETCH_THREAD_POOL_CORE_SIZE_PROP);
        if (corePoolSize != null) {
            this.threadPoolExecutor.setCorePoolSize(Integer.parseInt(corePoolSize));
        }
        String maxPoolSize = (String) configs.get(PREFETCH_THREAD_POOL_MAX_SIZE_PROP);
        if (maxPoolSize != null) {
            this.threadPoolExecutor.setMaximumPoolSize(Integer.parseInt(maxPoolSize));
        }
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
        InputStream inputStream = RSMUtils.getInputStreamFromChannel(cacheValue.fileChannel(), startPosition, endPosition);
        rsmMetrics.markPrefetchSegmentRead();
        return inputStream;
    }

    public InputStream fetchIndex(RemoteLogSegmentId segmentId, RemoteStorageManager.IndexType indexType) throws IOException {
        CacheValue cacheValue = segmentCache.getIfPresent(segmentId);
        if (cacheValue == null || cacheValue.status() != PrefetchStatus.SUCCESS) {
            return null;
        }
        InputStream inputStream = RSMUtils.getInputStreamFromChannel(cacheValue.fileChannel(), indexType);
        rsmMetrics.markPrefetchSegmentRead();
        return inputStream;
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
        // Shutdown the executor first, before clearing the cache, so that no new files are added by new/in-progress
        // tasks after the cache was cleared
        ThreadUtils.shutdownExecutorServiceQuietly(threadPoolExecutor, 5, TimeUnit.SECONDS);
        segmentCache.invalidateAll();
        segmentCache.cleanUp();
    }

    private void createDownloadTask(RemoteLogSegmentMetadata segmentMetadata) {
        try {
            Task task = new Task(segmentMetadata);
            threadPoolExecutor.submit(task);
            rsmMetrics.markPrefetchRequests();
        } catch (RejectedExecutionException e) {
            rsmMetrics.markPrefetchThreadPoolExecutorRejection();
            LOGGER.error("Task rejected by thread pool for segment: {}", segmentMetadata.remoteLogSegmentId(), e);
            signalDownloadFailure(segmentMetadata);
        } catch (Exception e) {
            LOGGER.error("Failed to submit download task for segment: {}", segmentMetadata.remoteLogSegmentId(), e);
            signalDownloadFailure(segmentMetadata);
        }
    }

    private void signalDownloadSuccess(RemoteLogSegmentMetadata segmentMetadata, FileChannel fileChannel) {
        Path filePath = Paths.get(RSMUtils.segmentPrefetchPath(prefetchDir, segmentMetadata));
        segmentCache.put(segmentMetadata.remoteLogSegmentId(), new CacheValue(PrefetchStatus.SUCCESS, filePath, fileChannel));
        rsmMetrics.markPrefetchRequestSuccess();
    }

    private void signalDownloadFailure(RemoteLogSegmentMetadata segmentMetadata) {
        segmentCache.invalidate(segmentMetadata.remoteLogSegmentId());
        rsmMetrics.markPrefetchRequestFailure();
    }

    private class Task implements Runnable {
        private final RemoteLogSegmentMetadata remoteLogSegmentMetadata;
        private final DownloadTask downloadTask;

        public Task(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
            this.remoteLogSegmentMetadata = remoteLogSegmentMetadata;
            this.downloadTask = new DownloadTask(time, prefetchDir, dataFetcher, rsmMetrics, remoteLogSegmentMetadata, quotaManager, lock, lockCondition);
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
