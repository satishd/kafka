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
 * <p>Each poll deserializes records with {@link RemoteLogMetadataSerde}, feeds them through a
 * {@link SegmentAssembler} to merge started/finished events, and returns the finalized segments
 * (optionally filtered by a topics allowlist).
 */
public class TopicMetadataSource implements MetadataSource {

    private static final Logger LOG = LoggerFactory.getLogger(TopicMetadataSource.class);

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final RemoteLogMetadataSerde serde;
    private final SegmentAssembler assembler;
    private final Duration pollTimeout;
    private final Set<String> topicsAllowlist;

    public TopicMetadataSource(ConverterConfig config) {
        this(new KafkaConsumer<>(config.consumerProperties()), config);
    }

    // Visible for testing.
    TopicMetadataSource(KafkaConsumer<byte[], byte[]> consumer, ConverterConfig config) {
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
            for (RemoteLogSegmentMetadata segment : assembler.accept(metadata)) {
                if (allowed(segment)) {
                    finished.add(segment);
                }
            }
        }
        return finished;
    }

    private boolean allowed(RemoteLogSegmentMetadata segment) {
        return topicsAllowlist.isEmpty()
                || topicsAllowlist.contains(segment.topicIdPartition().topic());
    }

    @Override
    public void close() {
        consumer.close();
    }
}
