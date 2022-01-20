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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME;

/**
 * This class is responsible for consuming messages from remote log metadata topic ({@link TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_TOPIC_NAME})
 * partitions and maintain the state of the remote log segment metadata. It gives an API to add or remove
 * for what topic partition's metadata should be consumed by this instance using
 * {{@link #addAssignmentsForPartitions(Set)}} and {@link #removeAssignmentsForPartitions(Set)} respectively.
 * <p>
 * When a broker is started, controller sends topic partitions that this broker is leader or follower for and the
 * partitions to be deleted. This class receives those notifications with
 * {@link #addAssignmentsForPartitions(Set)} and {@link #removeAssignmentsForPartitions(Set)} assigns consumer for the
 * respective remote log metadata partitions by using {@link RemoteLogMetadataTopicPartitioner#metadataPartition(TopicIdPartition)}.
 * Any leadership changes later are called through the same API. We will remove the partitions that are deleted from
 * this broker which are received through {@link #removeAssignmentsForPartitions(Set)}.
 * <p>
 * After receiving these events it invokes {@link RemotePartitionMetadataEventHandler#handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata)},
 * which maintains in-memory representation of the state of {@link RemoteLogSegmentMetadata}.
 */
class PrimaryConsumerTask implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(PrimaryConsumerTask.class);
    private static final long POLL_INTERVAL_MS = 100L;

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final RemotePartitionMetadataEventHandler handler;
    private final RemoteLogMetadataTopicPartitioner partitioner;

    private volatile boolean isClosed = false;
    // It indicates whether the consumer needs to assign the partitions or not. This is set when it is
    // determined that the consumer needs to be assigned with the updated partitions.
    private volatile boolean isAssignmentChanged = true;
    private final Object assignmentLock = new Object();

    // Remote log metadata topic partitions that consumer is assigned to.
    private volatile Set<Integer> assignedMetaPartitions = Collections.emptySet();
    // User topic partitions that this broker is a leader/follower for.
    private volatile Set<TopicIdPartition> assignedUserTopicPartitions = Collections.emptySet();

    // Map of remote log metadata topic partition to consumed offsets.
    private final Map<Integer, Long> readOffsetsByMetaPartition = new ConcurrentHashMap<>();
    private final Map<TopicIdPartition, Long> readOffsetsByUserTopicPartition = new HashMap<>();

    public PrimaryConsumerTask(final Map<String, Object> props,
                               final RemotePartitionMetadataEventHandler handler,
                               final RemoteLogMetadataTopicPartitioner partitioner) {
        this.handler = Objects.requireNonNull(handler);
        this.partitioner = Objects.requireNonNull(partitioner);
        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        log.info("Starting consumer task thread.");
        try {
            while (!isClosed) {
                if (isAssignmentChanged) {
                    maybeWaitForPartitionAssignment();
                }
                final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(POLL_INTERVAL_MS));
                if (!consumerRecords.isEmpty()) {
                    log.debug("Processing {} records", consumerRecords.count());
                    for (final ConsumerRecord<byte[], byte[]> record: consumerRecords) {
                        RemoteLogMetadata remoteLogMetadata = serde.deserialize(record.value());
                        if (canProcess(remoteLogMetadata, record.offset())) {
                            handler.handleRemoteLogMetadata(remoteLogMetadata);
                            readOffsetsByUserTopicPartition.put(remoteLogMetadata.topicIdPartition(), record.offset());
                        }
                        readOffsetsByMetaPartition.put(record.partition(), record.offset());
                    }
                }
            }
        } catch (final WakeupException ex) {
            // ignore
        } catch (final Exception e) {
            log.error("Error occurred while processing the records", e);
        } finally {
            try {
                consumer.close(Duration.ofSeconds(30));
            } catch (final Exception e) {
                log.error("Error encountered while closing the consumer", e);
            }
        }
        log.info("Exited from consumer task thread");
    }

    private boolean canProcess(final RemoteLogMetadata metadata, final long recordOffset) {
        final TopicIdPartition idPartition = metadata.topicIdPartition();
        final Long readOffset = readOffsetsByUserTopicPartition.get(idPartition);
        return assignedUserTopicPartitions.contains(idPartition) && (readOffset == null || readOffset < recordOffset);
    }

    private void maybeWaitForPartitionAssignment() throws InterruptedException {
        final Set<Integer> metaPartitionSnapshot = new HashSet<>();
        synchronized (assignmentLock) {
            while (!isClosed && assignedMetaPartitions.isEmpty()) {
                log.debug("Waiting for remote log metadata partitions to be assigned");
                assignmentLock.wait();
            }
            if (isAssignmentChanged) {
                metaPartitionSnapshot.addAll(assignedMetaPartitions);
                isAssignmentChanged = false;
            }
        }
        if (!metaPartitionSnapshot.isEmpty()) {
            final Map<TopicPartition, Long> currentPosition = consumer.assignment()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), consumer::position));

            final Set<TopicPartition> remoteLogPartitions = getRemoteLogPartitions(metaPartitionSnapshot);
            consumer.assign(remoteLogPartitions);
            // for newly assigned user-partitions, read from the beginning of the corresponding metadata partition
            final Set<TopicPartition> seekBackToBeginOffset = assignedUserTopicPartitions.stream()
                    .filter(tpId -> !readOffsetsByUserTopicPartition.containsKey(tpId))
                    .map(userTpId -> new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partitioner.metadataPartition(userTpId)))
                    .collect(Collectors.toSet());
            consumer.seekToBeginning(seekBackToBeginOffset);

            // for other metadata partitions, read from the offset where the processing left last time.
            remoteLogPartitions.stream()
                    .filter(tp -> !seekBackToBeginOffset.contains(tp))
                    .forEach(tp -> consumer.seek(tp, currentPosition.get(tp)));
        }
    }

    public void addAssignmentsForPartitions(final Set<TopicIdPartition> partitions) {
        updateAssignments(partitions, Collections.emptySet());
    }

    public void removeAssignmentsForPartitions(final Set<TopicIdPartition> partitions) {
        updateAssignments(Collections.emptySet(), partitions);
    }

    private void updateAssignments(final Set<TopicIdPartition> addedPartitions,
                                   final Set<TopicIdPartition> removedPartitions) {
        Objects.requireNonNull(addedPartitions, "addedPartitions must not be null");
        Objects.requireNonNull(removedPartitions, "removedPartitions must not be null");
        log.info("Updating assignments for partitions added: {} and removed: {}", addedPartitions, removedPartitions);
        if (!addedPartitions.isEmpty() || !removedPartitions.isEmpty()) {
            synchronized (assignmentLock) {
                final Set<TopicIdPartition> idealUserPartitions = new HashSet<>(assignedUserTopicPartitions);
                idealUserPartitions.addAll(addedPartitions);
                idealUserPartitions.removeAll(removedPartitions);

                final Set<Integer> idealMetaPartitions = idealUserPartitions.stream()
                        .map(partitioner::metadataPartition).collect(Collectors.toSet());
                assignedMetaPartitions = Collections.unmodifiableSet(idealMetaPartitions);

                if (!idealUserPartitions.equals(assignedUserTopicPartitions)) {
                    isAssignmentChanged = true;
                    assignedUserTopicPartitions = Collections.unmodifiableSet(idealUserPartitions);
                }
                if (isAssignmentChanged) {
                    log.debug("Assigned user-topic-partitions: {} and it's respective metadata-partitions: {}",
                            assignedUserTopicPartitions, assignedMetaPartitions);
                    assignmentLock.notifyAll();
                }
            }
        }
    }

    public Optional<Long> receivedOffsetForPartition(final int partition) {
        return Optional.ofNullable(readOffsetsByMetaPartition.get(partition));
    }

    public boolean isMetadataPartitionAssigned(final int partition) {
        return assignedMetaPartitions.contains(partition);
    }

    public boolean isUserPartitionAssigned(final TopicIdPartition partition) {
        return assignedUserTopicPartitions.contains(partition);
    }

    public void close() {
        if (!isClosed) {
            synchronized (assignmentLock) {
                isClosed = true;
                consumer.wakeup();
                assignmentLock.notifyAll();
            }
        }
    }

    private static Set<TopicPartition> getRemoteLogPartitions(final Set<Integer> partitions) {
        return partitions.stream()
                .map(x -> new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, x))
                .collect(Collectors.toSet());
    }

}
