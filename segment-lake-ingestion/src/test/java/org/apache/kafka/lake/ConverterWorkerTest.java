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
package org.apache.kafka.lake;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.lake.config.ConverterConfig;
import org.apache.kafka.lake.decode.DeadLetterSink;
import org.apache.kafka.lake.discovery.MetadataSource;
import org.apache.kafka.lake.pipeline.Pipeline;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ConverterWorkerTest {

    private final TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), 0, "orders");

    @Test
    public void reclaimAbandonedAuditsDeletedBeforeIngestSegments() {
        DeadLetterSink sink = mock(DeadLetterSink.class);
        Pipeline pipeline = new Pipeline(null, sink, null, null, null, null);
        RemoteLogSegmentMetadata deleted = startedSegment();
        FakeSource source = new FakeSource(Collections.singletonList(deleted), Collections.emptyList());

        ConverterWorker.reclaimAbandoned(source, pipeline, config(6 * 60 * 60 * 1000L));

        assertEquals(1, pipeline.metrics().segmentsAbortedByDelete());
        assertEquals(0, pipeline.metrics().segmentsEvicted());
        verify(sink).recordAbandonedSegment(
                eq(deleted.topicIdPartition().toString()),
                eq(deleted.remoteLogSegmentId().id().toString()),
                eq(deleted.startOffset()),
                eq(deleted.endOffset()),
                contains("deleted before ingest"));
    }

    @Test
    public void reclaimAbandonedEvictsAndAuditsStaleSegments() {
        DeadLetterSink sink = mock(DeadLetterSink.class);
        Pipeline pipeline = new Pipeline(null, sink, null, null, null, null);
        RemoteLogSegmentMetadata evicted = startedSegment();
        FakeSource source = new FakeSource(Collections.emptyList(), Collections.singletonList(evicted));

        ConverterWorker.reclaimAbandoned(source, pipeline, config(6 * 60 * 60 * 1000L));

        assertEquals(6 * 60 * 60 * 1000L, source.evictRequestedWith);
        assertEquals(1, pipeline.metrics().segmentsEvicted());
        assertEquals(0, pipeline.metrics().segmentsAbortedByDelete());
        verify(sink).recordAbandonedSegment(any(), any(), anyLong(), anyLong(),
                contains("evicted after pending timeout"));
    }

    @Test
    public void reclaimAbandonedSkipsEvictionWhenTimeoutDisabled() {
        DeadLetterSink sink = mock(DeadLetterSink.class);
        Pipeline pipeline = new Pipeline(null, sink, null, null, null, null);
        FakeSource source = new FakeSource(Collections.emptyList(), Collections.singletonList(startedSegment()));

        ConverterWorker.reclaimAbandoned(source, pipeline, config(0L));

        assertEquals(-1L, source.evictRequestedWith, "evictStale must not be called when timeout is disabled");
        assertEquals(0, pipeline.metrics().segmentsEvicted());
        verify(sink, never()).recordAbandonedSegment(any(), any(), anyLong(), anyLong(), any());
    }

    @Test
    public void reclaimAbandonedIsNoOpWhenNothingAbandoned() {
        DeadLetterSink sink = mock(DeadLetterSink.class);
        Pipeline pipeline = new Pipeline(null, sink, null, null, null, null);
        FakeSource source = new FakeSource(Collections.emptyList(), Collections.emptyList());

        ConverterWorker.reclaimAbandoned(source, pipeline, config(6 * 60 * 60 * 1000L));

        assertEquals(0, pipeline.metrics().segmentsEvicted());
        assertEquals(0, pipeline.metrics().segmentsAbortedByDelete());
        verify(sink, never()).recordAbandonedSegment(any(), any(), anyLong(), anyLong(), any());
    }

    private RemoteLogSegmentMetadata startedSegment() {
        return new RemoteLogSegmentMetadata(
                RemoteLogSegmentId.generateNew(tp), 0L, 99L, -1L, 1, 1000L, 1024,
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                Collections.singletonMap(0, 0L));
    }

    private static ConverterConfig config(long pendingTimeoutMs) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConverterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConverterConfig.TOPICS_ALLOWLIST_CONFIG, "orders");
        props.put(ConverterConfig.PENDING_SEGMENT_TIMEOUT_MS_CONFIG, pendingTimeoutMs);
        return new ConverterConfig(props);
    }

    /** A {@link MetadataSource} that returns canned abandoned/evicted lists and records the timeout used. */
    private static final class FakeSource implements MetadataSource {
        private final List<RemoteLogSegmentMetadata> abandoned;
        private final List<RemoteLogSegmentMetadata> evicted;
        private long evictRequestedWith = -1L;

        FakeSource(List<RemoteLogSegmentMetadata> abandoned, List<RemoteLogSegmentMetadata> evicted) {
            this.abandoned = abandoned;
            this.evicted = evicted;
        }

        @Override
        public List<RemoteLogSegmentMetadata> poll() {
            return Collections.emptyList();
        }

        @Override
        public int pendingCount() {
            return 0;
        }

        @Override
        public List<RemoteLogSegmentMetadata> drainAbandoned() {
            return new ArrayList<>(abandoned);
        }

        @Override
        public List<RemoteLogSegmentMetadata> evictStale(long maxAgeMs) {
            this.evictRequestedWith = maxAgeMs;
            return new ArrayList<>(evicted);
        }

        @Override
        public void commit() {
        }

        @Override
        public void close() {
        }
    }
}
