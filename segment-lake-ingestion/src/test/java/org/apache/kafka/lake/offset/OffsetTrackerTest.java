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
package org.apache.kafka.lake.offset;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.lake.write.HudiSegmentWriter;
import org.apache.kafka.lake.write.HudiWriterConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Builds real Hudi commits via {@link HudiSegmentWriter} (proven working in the write-path tests)
 * and verifies {@link OffsetTracker} reconstructs skip decisions from them correctly, including
 * across a fresh {@code load()} that simulates a worker restart.
 */
public class OffsetTrackerTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"}]}");

    private final TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), 0, "orders");

    @Test
    public void emptyTableProcessesEverything(@TempDir Path tempDir) {
        Configuration hadoopConf = new Configuration();
        OffsetTracker tracker = OffsetTracker.load(hadoopConf, tempDir.resolve("orders_table").toString());

        assertFalse(tracker.isProcessed(segment(0L, 99L)));
    }

    @Test
    public void skipsSameSegmentIdAfterRestart(@TempDir Path tempDir) {
        String tableBasePath = tempDir.resolve("orders_table").toString();
        Configuration hadoopConf = new Configuration();
        RemoteLogSegmentMetadata segment = segment(0L, 99L);

        commit(hadoopConf, tableBasePath, segment);

        // Simulate a restart: a fresh tracker loaded purely from the Hudi timeline.
        OffsetTracker restarted = OffsetTracker.load(hadoopConf, tableBasePath);
        assertTrue(restarted.isProcessed(segment));
    }

    @Test
    public void skipsDifferentSegmentCoveredByHighWaterOffset(@TempDir Path tempDir) {
        String tableBasePath = tempDir.resolve("orders_table").toString();
        Configuration hadoopConf = new Configuration();
        commit(hadoopConf, tableBasePath, segment(0L, 99L));

        OffsetTracker restarted = OffsetTracker.load(hadoopConf, tableBasePath);

        // A different segment id, but its offset range is already covered.
        assertTrue(restarted.isProcessed(segment(50L, 90L)));
        // Not covered: extends past the high-water offset.
        assertFalse(restarted.isProcessed(segment(90L, 150L)));
    }

    @Test
    public void markProcessedSkipsWithoutReload(@TempDir Path tempDir) {
        Configuration hadoopConf = new Configuration();
        OffsetTracker tracker = OffsetTracker.load(hadoopConf, tempDir.resolve("orders_table").toString());
        RemoteLogSegmentMetadata segment = segment(0L, 99L);

        assertFalse(tracker.isProcessed(segment));
        tracker.markProcessed(segment);
        assertTrue(tracker.isProcessed(segment));
    }

    @Test
    public void concurrentMarkProcessedDoesNotLoseUpdates(@TempDir Path tempDir) throws Exception {
        OffsetTracker tracker = OffsetTracker.load(new Configuration(),
                tempDir.resolve("orders_table").toString());

        int segmentCount = 2000;
        List<RemoteLogSegmentMetadata> segments = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            segments.add(segment(i, i));
        }

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int worker = t;
            futures.add(pool.submit(() -> {
                await(start);
                for (int i = worker; i < segmentCount; i += threads) {
                    RemoteLogSegmentMetadata segment = segments.get(i);
                    tracker.isProcessed(segment); // concurrent read alongside writes
                    tracker.markProcessed(segment);
                }
            }));
        }
        start.countDown();
        for (java.util.concurrent.Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        // Every mark must have survived (a non-thread-safe set would drop some), and the high-water
        // offset must equal the maximum end offset seen.
        for (RemoteLogSegmentMetadata segment : segments) {
            assertTrue(tracker.isProcessed(segment), "lost update for " + segment.remoteLogSegmentId());
        }
        assertTrue(tracker.isProcessed(segment(segmentCount - 1, segmentCount - 1)));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void commit(Configuration hadoopConf, String tableBasePath, RemoteLogSegmentMetadata segment) {
        HudiWriterConfig config = new HudiWriterConfig(tableBasePath, "orders_table", "id", "id");
        HudiSegmentWriter writer = new HudiSegmentWriter(config, hadoopConf,
                record -> record.get("id").toString(), record -> "all");
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("id", "order-1");
        writer.write(Collections.singletonList(record), SCHEMA, OffsetTracker.commitExtraMetadata(segment));
    }

    private RemoteLogSegmentMetadata segment(long startOffset, long endOffset) {
        return new RemoteLogSegmentMetadata(
                RemoteLogSegmentId.generateNew(tp), startOffset, endOffset, -1L, 1, 1000L, 1024,
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
                Collections.singletonMap(0, startOffset));
    }
}
