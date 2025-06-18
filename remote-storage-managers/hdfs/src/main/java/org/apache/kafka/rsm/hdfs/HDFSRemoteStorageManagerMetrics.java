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
package org.apache.kafka.rsm.hdfs;

import org.apache.kafka.rsm.hdfs.pool.ByteBufferPool;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageProvider;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.google.common.annotations.VisibleForTesting;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class HDFSRemoteStorageManagerMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRemoteStorageManagerMetrics.class);
    static final String PROVIDER = "provider";

    // Buffer Pool metrics
    static final String BUFFER_POOL_ALLOC_COUNT = "buffer-pool-alloc-count";
    static final String BUFFER_POOL_RELEASE_COUNT = "buffer-pool-release-count";
    static final String BUFFER_POOL_REUSE_COUNT = "buffer-pool-reuse-count";
    static final String BUFFER_POOL_RECYCLE_COUNT = "buffer-pool-recycle-count";
    static final String BUFFER_POOL_DISCARD_COUNT = "buffer-pool-discard-count";
    static final String BUFFER_POOL_SIZE = "buffer-pool-size";

    // Hedged read metrics
    static final String HEDGED_READ_OPS = "hedged-read-ops";
    static final String HEDGED_READ_OPS_WIN = "hedged-read-ops-win";
    static final String READ_THREADPOOL_EXECUTOR_TASK_QUEUE_SIZE = "read-threadpool-executor-task-queue-size";
    static final String READ_THREADPOOL_EXECUTOR_REJECTION_COUNT = "read-threadpool-executor-rejection-count";
    static final String READ_THREADPOOL_EXECUTOR_AVG_IDLE_PERCENT = "read-threadpool-executor-avg-idle-percent";
    static final String READ_THREADPOOL_EXECUTOR_CORE_POOL_SIZE = "read-threadpool-executor-core-pool-size";
    static final String READ_THREADPOOL_EXECUTOR_MAX_POOL_SIZE = "read-threadpool-executor-max-pool-size";
    static final String READ_THREADPOOL_EXECUTOR_POOL_SIZE = "read-threadpool-executor-pool-size";

    // HDFS/OCI read metrics
    static final String FS_OPEN_RATE_AND_TIME_MS = "fs-open-rate-and-time-ms";
    static final String FS_STATUS_RATE_AND_TIME_MS = "fs-status-rate-and-time-ms";
    static final String SEGMENT_READ_RATE_AND_TIME_MS = "segment-read-rate-and-time-ms";
    static final String SEGMENT_HEADER_READ_RATE_AND_TIME_MS = "segment-header-read-rate-and-time-ms";

    // HDFS/OCI write metrics
    static final String SEGMENT_WRITE_RATE_AND_TIME_MS = "segment-write-rate-and-time-ms";
    static final String SEGMENT_WRITE_BYTES_PER_SEC = "segment-write-bytes-per-sec";

    // Tracks the number of open streams
    static final String FS_OPEN_INPUT_STREAM = "fs-open-input-stream";
    static final String FS_OPEN_OUTPUT_STREAM = "fs-open-output-stream";

    private final Map<RemoteStorageProvider, Timer> segmentReadTimerByProvider = new HashMap<>();
    private final Map<RemoteStorageProvider, Timer> segmentHeaderReadTimerByProvider = new HashMap<>();
    private final Map<RemoteStorageProvider, Timer> segmentWriteTimerByProvider = new HashMap<>();
    private final Map<RemoteStorageProvider, Meter> segmentWriteSizeMeterByProvider = new HashMap<>();

    private Meter cacheThrashMeter;
    private Timer fileSystemOpenTimer;
    private Timer fileSystemStatusTimer;

    private MetricName metricName(String name) {
        return metricName(name, null);
    }

    private MetricName metricName(String name, LinkedHashMap<String, String> tags) {
        Class<? extends HDFSRemoteStorageManager> klass = HDFSRemoteStorageManager.class;
        String group = klass.getPackage() == null ? "" : klass.getPackage().getName();
        String typeName = klass.getSimpleName().replaceAll("\\$$", "");
        return KafkaYammerMetrics.getMetricName(group, typeName, name, tags);
    }

    void registerCacheMetrics(LRUCache cache) {
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("requestCount"), new Gauge<Long>() {
            @Override
            public Long value() {
                return cache.stats().getRequestCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("hitCount"), new Gauge<Long>() {
            @Override
            public Long value() {
                return cache.stats().getHitCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("hitRate"), new Gauge<Double>() {
            @Override
            public Double value() {
                return cache.stats().getHitRate();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("missCount"), new Gauge<Long>() {
            @Override
            public Long value() {
                return cache.stats().getMissCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("missRate"), new Gauge<Double>() {
            @Override
            public Double value() {
                return cache.stats().getMissRate();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("loadCount"), new Gauge<Long>() {
            @Override
            public Long value() {
                return cache.stats().getLoadCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("evictionCount"), new Gauge<Long>() {
            @Override
            public Long value() {
                return cache.stats().getEvictionCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName("size"), new Gauge<Integer>() {
            @Override
            public Integer value() {
                return cache.stats().getSize();
            }
        });
        cacheThrashMeter = KafkaYammerMetrics.defaultRegistry().newMeter(
                metricName("HDFSCacheThrashRequestPerSec"), "requests", TimeUnit.SECONDS);
    }

    void registerBufferPoolMetrics(ByteBufferPool byteBufferPool) {
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(BUFFER_POOL_ALLOC_COUNT), new Gauge<Long>() {
            @Override
            public Long value() {
                return byteBufferPool.allocCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(BUFFER_POOL_REUSE_COUNT), new Gauge<Long>() {
            @Override
            public Long value() {
                return byteBufferPool.reuseCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(BUFFER_POOL_RELEASE_COUNT), new Gauge<Long>() {
            @Override
            public Long value() {
                return byteBufferPool.releaseCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(BUFFER_POOL_RECYCLE_COUNT), new Gauge<Long>() {
            @Override
            public Long value() {
                return byteBufferPool.recycleCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(BUFFER_POOL_DISCARD_COUNT), new Gauge<Long>() {
            @Override
            public Long value() {
                return byteBufferPool.discardCount();
            }
        });
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(BUFFER_POOL_SIZE), new Gauge<Integer>() {
            @Override
            public Integer value() {
                return byteBufferPool.poolSize();
            }
        });
    }

    /**
     * Executes the given supplier with the HDFSRemoteStorageManager's class loader as the
     * thread context class loader.
     *
     * <p>This is required for accessing Hadoop FileSystem metrics because Hadoop JARs are
     * loaded in a separate class loader (the RSM's class loader) rather than the application's
     * main class loader. When retrieving metrics from Hadoop FileSystem, the calls must be
     * made with the same class loader that loaded the Hadoop classes, otherwise it may result
     * in ClassNotFoundException or NoClassDefFoundError.
     *
     * <p>The original thread context class loader is always restored after execution,
     * even if the supplier throws an exception.
     *
     * @param rsmClassLoader the class loader that loaded the Hadoop JARs (RSM's class loader)
     * @param supplier the metrics retrieval operation to execute with the correct class loader context
     * @param <T> the return type of the supplier
     * @return the result of the supplier execution
     */
    private <T> T withClassLoader(ClassLoader rsmClassLoader, Supplier<T> supplier) {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(rsmClassLoader);
        try {
            return supplier.get();
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @VisibleForTesting
    void registerHedgedReadMetrics(ThrowingSupplier<FileSystem, IOException> fileSystemSupplier) {
        ClassLoader rsmClassLoader = Thread.currentThread().getContextClassLoader();
        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(HEDGED_READ_OPS), new Gauge<Long>() {
            @Override
            public Long value() {
                return withClassLoader(rsmClassLoader, () -> {
                    try {
                        return ((DistributedFileSystem) fileSystemSupplier.get()).getReadOpsInReadThread();
                    } catch (Exception e) {
                        LOGGER.error("Failed to get the number of hedged read ops", e);
                        return 0L;
                    }
                });
            }
        });

        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(HEDGED_READ_OPS_WIN), new Gauge<Long>() {
            @Override
            public Long value() {
                return withClassLoader(rsmClassLoader, () -> {
                    try {
                        return ((DistributedFileSystem) fileSystemSupplier.get()).getHedgedReadWinsInReadThread();
                    } catch (Exception e) {
                        LOGGER.error("Failed to get the number of hedged read wins", e);
                        return 0L;
                    }
                });
            }
        });

        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(READ_THREADPOOL_EXECUTOR_TASK_QUEUE_SIZE), new Gauge<Integer>() {
            @Override
            public Integer value() {
                return withClassLoader(rsmClassLoader, () -> {
                    try {
                        return ((DistributedFileSystem) fileSystemSupplier.get()).getNumberOfOperationsInReadThread();
                    } catch (Exception e) {
                        LOGGER.error("Failed to get the number of operations in read thread", e);
                        return 0;
                    }
                });
            }
        });

        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(READ_THREADPOOL_EXECUTOR_REJECTION_COUNT), new Gauge<Long>() {
            @Override
            public Long value() {
                return withClassLoader(rsmClassLoader, () -> {
                    try {
                        return ((DistributedFileSystem) fileSystemSupplier.get()).getReadThreadRejectionCount();
                    } catch (Exception e) {
                        LOGGER.error("Failed to get the number of read thread rejections", e);
                        return 0L;
                    }
                });
            }
        });

        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(READ_THREADPOOL_EXECUTOR_AVG_IDLE_PERCENT), new Gauge<Double>() {
            @Override
            public Double value() {
                return withClassLoader(rsmClassLoader, () -> {
                    try {
                        return ((DistributedFileSystem) fileSystemSupplier.get()).getReadThreadIdlePercentage();
                    } catch (Exception e) {
                        LOGGER.error("Failed to get the read thread idle percentage", e);
                        return 0.0;
                    }
                });
            }
        });

        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(READ_THREADPOOL_EXECUTOR_CORE_POOL_SIZE), new Gauge<Integer>() {
            @Override
            public Integer value() {
                return withClassLoader(rsmClassLoader, () -> {
                    try {
                        return ((DistributedFileSystem) fileSystemSupplier.get()).getDFSClientReaderThreadPoolSize();
                    } catch (Exception e) {
                        LOGGER.error("Failed to get the read thread pool size", e);
                        return 0;
                    }
                });
            }
        });

        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(READ_THREADPOOL_EXECUTOR_MAX_POOL_SIZE), new Gauge<Integer>() {
            @Override
            public Integer value() {
                return withClassLoader(rsmClassLoader, () -> {
                    try {
                        return ((DistributedFileSystem) fileSystemSupplier.get()).getDFSClientReaderThreadPoolMaxSize();
                    } catch (Exception e) {
                        LOGGER.error("Failed to get the read thread pool max size", e);
                        return 0;
                    }
                });
            }
        });

        KafkaYammerMetrics.defaultRegistry().newGauge(metricName(READ_THREADPOOL_EXECUTOR_POOL_SIZE), new Gauge<Integer>() {
            @Override
            public Integer value() {
                return withClassLoader(rsmClassLoader, () -> {
                    try {
                        return ((DistributedFileSystem) fileSystemSupplier.get()).getReaderThreadPoolSize();
                    } catch (Exception e) {
                        LOGGER.error("Failed to get the read thread pool size", e);
                        return 0;
                    }
                });
            }
        });
    }

    void registerHDFSReadMetrics() {
        fileSystemOpenTimer = KafkaYammerMetrics.defaultRegistry().newTimer(
                metricName(FS_OPEN_RATE_AND_TIME_MS), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        fileSystemStatusTimer = KafkaYammerMetrics.defaultRegistry().newTimer(
                metricName(FS_STATUS_RATE_AND_TIME_MS), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        for (RemoteStorageProvider provider : RemoteStorageProvider.values()) {
            LinkedHashMap<String, String> tags = new LinkedHashMap<>();
            tags.put(PROVIDER, provider.toString());
            segmentReadTimerByProvider.put(provider, KafkaYammerMetrics.defaultRegistry().newTimer(
                            metricName(SEGMENT_READ_RATE_AND_TIME_MS, tags), TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
            segmentHeaderReadTimerByProvider.put(provider, KafkaYammerMetrics.defaultRegistry().newTimer(
                            metricName(SEGMENT_HEADER_READ_RATE_AND_TIME_MS, tags), TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
            segmentWriteTimerByProvider.put(provider, KafkaYammerMetrics.defaultRegistry().newTimer(
                    metricName(SEGMENT_WRITE_RATE_AND_TIME_MS, tags), TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
            segmentWriteSizeMeterByProvider.put(provider, KafkaYammerMetrics.defaultRegistry().newMeter(
                    metricName(SEGMENT_WRITE_BYTES_PER_SEC, tags), "bytes", TimeUnit.SECONDS));
        }
    }

    void registerStreamMetrics(final AtomicInteger openInputStreamCount,
                               final AtomicInteger openOutputStreamCount) {
        KafkaYammerMetrics.defaultRegistry().newGauge(
                metricName(FS_OPEN_INPUT_STREAM), new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return openInputStreamCount.get();
                    }
                });
        KafkaYammerMetrics.defaultRegistry().newGauge(
                metricName(FS_OPEN_OUTPUT_STREAM), new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return openOutputStreamCount.get();
                    }
                });
    }

    void markCacheThrashing() {
        if (cacheThrashMeter != null) {
            cacheThrashMeter.mark();
        }
    }

    long getFileSystemOpenCount() {
        return fileSystemOpenTimer == null ? 0 : fileSystemOpenTimer.count();
    }

    void timeFileSystemOpen(ThrowingRunnable<IOException> operation) throws IOException {
        time(fileSystemOpenTimer, operation);
    }

    void timeFileSystemStatus(ThrowingRunnable<IOException> operation) throws IOException {
        time(fileSystemStatusTimer, operation);
    }

    void timeSegmentRead(RemoteStorageProvider provider, ThrowingRunnable<IOException> operation) throws IOException {
        time(segmentReadTimerByProvider.get(provider), operation);
    }

    int timeSegmentRead(RemoteStorageProvider provider, ThrowingSupplier<Integer, IOException> operation) throws IOException {
        Timer segmentReadTimer = segmentReadTimerByProvider.get(provider);
        if (segmentReadTimer == null) {
            return operation.get();
        }
        TimerContext context = segmentReadTimer.time();
        try {
            return operation.get();
        } finally {
            context.stop();
        }
    }

    void timeSegmentHeaderRead(RemoteStorageProvider provider, ThrowingRunnable<IOException> operation) throws IOException {
        time(segmentHeaderReadTimerByProvider.get(provider), operation);
    }

    void timeSegmentWrite(RemoteStorageProvider provider, ThrowingRunnable<RemoteStorageException> operation) throws RemoteStorageException {
        time(segmentWriteTimerByProvider.get(provider), operation);
    }

    void recordSegmentWriteSize(RemoteStorageProvider provider, long size) {
        Meter segmentWriteSizeMeter = segmentWriteSizeMeterByProvider.get(provider);
        if (segmentWriteSizeMeter != null) {
            segmentWriteSizeMeter.mark(size);
        }
    }

    private <E extends Exception> void time(Timer timer, ThrowingRunnable<E> operation) throws E {
        if (timer == null) {
            operation.run();
            return;
        }

        TimerContext context = timer.time();
        try {
            operation.run();
        } finally {
            context.stop();
        }
    }
}
