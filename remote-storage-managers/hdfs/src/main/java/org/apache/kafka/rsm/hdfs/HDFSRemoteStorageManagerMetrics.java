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
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HDFSRemoteStorageManagerMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRemoteStorageManagerMetrics.class);

    // Buffer Pool metrics
    static final String BUFFER_POOL_ALLOC_COUNT = "buffer-pool-alloc-count";
    static final String BUFFER_POOL_RELEASE_COUNT = "buffer-pool-release-count";
    static final String BUFFER_POOL_REUSE_COUNT = "buffer-pool-reuse-count";
    static final String BUFFER_POOL_RECYCLE_COUNT = "buffer-pool-recycle-count";
    static final String BUFFER_POOL_DISCARD_COUNT = "buffer-pool-discard-count";
    static final String BUFFER_POOL_SIZE = "buffer-pool-size";

    // HDFS read metrics
    static final String FS_OPEN_RATE_AND_TIME_MS = "fs-open-rate-and-time-ms";
    static final String FS_STATUS_RATE_AND_TIME_MS = "fs-status-rate-and-time-ms";
    static final String SEGMENT_READ_RATE_AND_TIME_MS = "segment-read-rate-and-time-ms";
    static final String SEGMENT_HEADER_READ_RATE_AND_TIME_MS = "segment-header-read-rate-and-time-ms";

    private Meter cacheThrashMeter;
    private Timer fileSystemOpenTimer;
    private Timer fileSystemStatusTimer;
    private Timer segmentReadTimer;
    private Timer segmentHeaderReadTimer;

    private MetricName metricName(String name) {
        Class<? extends HDFSRemoteStorageManager> klass = HDFSRemoteStorageManager.class;
        String group = klass.getPackage() == null ? "" : klass.getPackage().getName();
        String typeName = klass.getSimpleName().replaceAll("\\$$", "");
        return new MetricName(group, typeName, name, null, group + ":type=" + typeName + ",name=" + name);
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

    void registerHDFSReadMetrics() {
        fileSystemOpenTimer = KafkaYammerMetrics.defaultRegistry().newTimer(
                metricName(FS_OPEN_RATE_AND_TIME_MS), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        fileSystemStatusTimer = KafkaYammerMetrics.defaultRegistry().newTimer(
                metricName(FS_STATUS_RATE_AND_TIME_MS), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        segmentReadTimer = KafkaYammerMetrics.defaultRegistry().newTimer(
                metricName(SEGMENT_READ_RATE_AND_TIME_MS), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        segmentHeaderReadTimer = KafkaYammerMetrics.defaultRegistry().newTimer(
                metricName(SEGMENT_HEADER_READ_RATE_AND_TIME_MS), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
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

    void timeSegmentRead(ThrowingRunnable<IOException> operation) throws IOException {
        time(segmentReadTimer, operation);
    }

    void timeSegmentHeaderRead(ThrowingRunnable<IOException> operation) throws IOException {
        time(segmentHeaderReadTimer, operation);
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
