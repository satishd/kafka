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
package org.apache.kafka.lake.discovery;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.lake.config.ConverterConfig;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicMetadataSourceTest {

    private static final String TOPIC = "__remote_log_metadata";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();

    @Test
    public void emitsFinishedSegmentsSkippingTombstonesAndUndeserializable() {
        MockConsumer<byte[], byte[]> consumer = mockConsumer();
        RemoteLogSegmentMetadata finished = finishedSegment("orders");
        TopicMetadataSource source = new TopicMetadataSource(consumer, config(Collections.singletonList("orders")));
        consumer.rebalance(Collections.singleton(PARTITION));

        long offset = 0;
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset++, null, serde.serialize(finished)));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset++, null, null));            // tombstone
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, null, new byte[]{0x01}));  // garbage

        List<RemoteLogSegmentMetadata> out = source.poll();

        assertEquals(1, out.size());
        assertEquals(finished.remoteLogSegmentId(), out.get(0).remoteLogSegmentId());
    }

    @Test
    public void filtersByAllowlist() {
        MockConsumer<byte[], byte[]> consumer = mockConsumer();
        TopicMetadataSource source = new TopicMetadataSource(consumer, config(Collections.singletonList("orders")));
        consumer.rebalance(Collections.singleton(PARTITION));

        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, null, serde.serialize(finishedSegment("orders"))));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, null, serde.serialize(finishedSegment("payments"))));

        List<RemoteLogSegmentMetadata> out = source.poll();

        assertEquals(1, out.size());
        assertEquals("orders", out.get(0).topicIdPartition().topic());
    }

    @Test
    public void nonAllowlistedTopicNeverEntersAssembler() {
        MockConsumer<byte[], byte[]> consumer = mockConsumer();
        TopicMetadataSource source = new TopicMetadataSource(consumer, config(Collections.singletonList("orders")));
        consumer.rebalance(Collections.singleton(PARTITION));

        // A started (but never finished) segment for a non-configured topic must not be tracked:
        // otherwise it would inflate pendingCount() and hold offset commits back forever.
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, null, serde.serialize(startedSegment("payments"))));

        assertTrue(source.poll().isEmpty());
        assertEquals(0, source.pendingCount());
    }

    @Test
    public void emptyPollReturnsNothing() {
        MockConsumer<byte[], byte[]> consumer = mockConsumer();
        TopicMetadataSource source = new TopicMetadataSource(consumer, config(Collections.singletonList("orders")));
        consumer.rebalance(Collections.singleton(PARTITION));

        assertTrue(source.poll().isEmpty());
    }

    @Test
    public void pendingCountTracksStartedButNotFinishedSegments() {
        MockConsumer<byte[], byte[]> consumer = mockConsumer();
        TopicMetadataSource source = new TopicMetadataSource(consumer, config(Collections.singletonList("orders")));
        consumer.rebalance(Collections.singleton(PARTITION));

        RemoteLogSegmentMetadata started = startedSegment("orders");
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, null, serde.serialize(started)));
        assertTrue(source.poll().isEmpty());
        assertEquals(1, source.pendingCount());

        RemoteLogSegmentMetadataUpdate finished = new RemoteLogSegmentMetadataUpdate(
                started.remoteLogSegmentId(), 2000L, Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 1);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, null, serde.serialize(finished)));

        List<RemoteLogSegmentMetadata> out = source.poll();
        assertEquals(1, out.size());
        assertEquals(0, source.pendingCount());
    }

    @Test
    public void commitAdvancesConsumerPosition() {
        MockConsumer<byte[], byte[]> consumer = mockConsumer();
        TopicMetadataSource source = new TopicMetadataSource(consumer, config(Collections.singletonList("orders")));
        consumer.rebalance(Collections.singleton(PARTITION));

        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, null, serde.serialize(finishedSegment("orders"))));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, null, serde.serialize(finishedSegment("orders"))));
        source.poll();

        assertTrue(consumer.committed(Collections.singleton(PARTITION)).isEmpty());
        source.commit();
        assertEquals(2L, consumer.committed(Collections.singleton(PARTITION)).get(PARTITION).offset());
    }

    @Test
    public void drainAbandonedReturnsSegmentsDeletedBeforeIngest() {
        MockConsumer<byte[], byte[]> consumer = mockConsumer();
        TopicMetadataSource source = new TopicMetadataSource(consumer, config(Collections.singletonList("orders")));
        consumer.rebalance(Collections.singleton(PARTITION));

        RemoteLogSegmentMetadata started = startedSegment("orders");
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, null, serde.serialize(started)));
        RemoteLogSegmentMetadataUpdate deleted = new RemoteLogSegmentMetadataUpdate(
                started.remoteLogSegmentId(), 3000L, Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_STARTED, 1);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, null, serde.serialize(deleted)));
        source.poll();

        List<RemoteLogSegmentMetadata> abandoned = source.drainAbandoned();
        assertEquals(1, abandoned.size());
        assertEquals(started.remoteLogSegmentId(), abandoned.get(0).remoteLogSegmentId());
        assertEquals(0, source.pendingCount());
    }

    @Test
    public void evictStaleDelegatesToAssembler() {
        AtomicLong now = new AtomicLong(0);
        MockConsumer<byte[], byte[]> consumer = mockConsumer();
        TopicMetadataSource source =
                new TopicMetadataSource(consumer, config(Collections.singletonList("orders")), now::get);
        consumer.rebalance(Collections.singleton(PARTITION));

        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, null, serde.serialize(startedSegment("orders"))));
        source.poll();
        assertEquals(1, source.pendingCount());

        assertTrue(source.evictStale(0L).isEmpty(), "Eviction disabled for non-positive timeout");
        assertEquals(1, source.pendingCount());

        now.addAndGet(5_000L);
        assertEquals(1, source.evictStale(1_000L).size());
        assertEquals(0, source.pendingCount());
    }

    private static MockConsumer<byte[], byte[]> mockConsumer() {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updateBeginningOffsets(Collections.singletonMap(PARTITION, 0L));
        return consumer;
    }

    private static ConverterConfig config(List<String> allowlist) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConverterConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConverterConfig.TOPICS_ALLOWLIST_CONFIG, String.join(",", allowlist));
        return new ConverterConfig(props);
    }

    private static RemoteLogSegmentMetadata finishedSegment(String topic) {
        return segment(topic, RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
    }

    private static RemoteLogSegmentMetadata startedSegment(String topic) {
        return segment(topic, RemoteLogSegmentState.COPY_SEGMENT_STARTED);
    }

    private static RemoteLogSegmentMetadata segment(String topic, RemoteLogSegmentState state) {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), 0, topic);
        return new RemoteLogSegmentMetadata(
                RemoteLogSegmentId.generateNew(tp), 0L, 99L, -1L, 1, 1000L, 1024,
                Optional.empty(), state,
                Collections.singletonMap(0, 0L));
    }
}
