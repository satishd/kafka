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

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.lake.config.ConverterConfig;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link MetadataSource} backed by a {@link KafkaConsumer} on the remote log metadata topic.
 *
 * <p>Each poll deserializes records with {@link RemoteLogMetadataSerde}, discards any whose topic is
 * not in the configured allowlist <em>before</em> assembly, feeds the rest through a
 * {@link SegmentAssembler} to merge started/finished events, and returns the finalized segments.
 * Filtering ahead of the assembler keeps un-ingested topics out of its pending map and out of
 * {@link #pendingCount()}, so they can never hold back offset commits.
 */
public class TopicMetadataSource implements MetadataSource {

    private static final Logger LOG = LoggerFactory.getLogger(TopicMetadataSource.class);

    private final Consumer<byte[], byte[]> consumer;
    private final RemoteLogMetadataSerde serde;
    private final SegmentAssembler assembler;
    private final Duration pollTimeout;
    private final Set<String> topicsAllowlist;

    public TopicMetadataSource(ConverterConfig config) {
        this(new KafkaConsumer<>(config.consumerProperties()), config);
    }

    // Visible for testing.
    TopicMetadataSource(Consumer<byte[], byte[]> consumer, ConverterConfig config) {
        this.consumer = consumer;
        this.serde = new RemoteLogMetadataSerde();
        this.assembler = new SegmentAssembler();
        this.pollTimeout = config.pollTimeout();
        this.topicsAllowlist = new HashSet<>(config.topicsAllowlist());
        this.consumer.subscribe(Collections.singletonList(config.metadataTopic()));
    }

    @Override
    public List<RemoteLogSegmentMetadata> poll() {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
        List<RemoteLogSegmentMetadata> finished = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (record.value() == null) {
                // Tombstone from log compaction; nothing to decode.
                continue;
            }
            RemoteLogMetadata metadata;
            try {
                metadata = serde.deserialize(record.value());
            } catch (RuntimeException e) {
                LOG.warn("Skipping undeserializable metadata record at {}-{}@{}",
                        record.topic(), record.partition(), record.offset(), e);
                continue;
            }
            if (!topicsAllowlist.contains(metadata.topicIdPartition().topic())) {
                // Not a configured topic: drop it before the assembler sees it, so it never enters
                // the pending map or counts toward pendingCount() (which would block offset commits).
                LOG.debug("Skipping metadata record for topic {} at {}-{}@{} because it is not in the allowlist",
                        metadata.topicIdPartition().topic(), record.topic(), record.partition(), record.offset());
                continue;
            }
            finished.addAll(assembler.accept(metadata));
        }
        return finished;
    }

    @Override
    public int pendingCount() {
        return assembler.pendingCount();
    }

    @Override
    public void commit() {
        try {
            consumer.commitSync();
        } catch (CommitFailedException e) {
            // Lost partition ownership (rebalance) before committing; the new owner will re-process
            // from the last durable checkpoint. Safe to skip and retry on the next quiescent point.
            LOG.warn("Offset commit failed; will retry at the next checkpoint", e);
        }
    }

    @Override
    public void close() {
        consumer.close();
    }
}
