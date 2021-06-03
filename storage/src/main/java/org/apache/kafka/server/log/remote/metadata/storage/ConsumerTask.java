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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
class ConsumerTask implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerTask.class);

    private static final long POLL_INTERVAL_MS = 30L;

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler;
    private final RemoteLogMetadataTopicPartitioner topicPartitioner;
    private final Time time;

    private volatile boolean close = false;
    private volatile boolean assignPartitions = false;

    private final Object assignPartitionsLock = new Object();

    // Remote log metadata topic partitions that consumer is assigned to.
    private volatile Set<Integer> assignedMetaPartitions = Collections.emptySet();

    // User topic partitions that this broker is a leader/follower for.
    private Set<TopicIdPartition> assignedTopicPartitions = Collections.emptySet();

    // Map of remote log metadata topic partition to consumed offsets.
    private final Map<Integer, Long> partitionToConsumedOffsets = new ConcurrentHashMap<>();
    private Map<Integer, Long> committedPartitionToConsumedOffsets = Collections.emptyMap();

    private final long committedOffsetSyncIntervalMs;
    private CommittedOffsetsFile committedOffsetsFile;
    private long lastSyncedTimeMs;

    public ConsumerTask(KafkaConsumer<byte[], byte[]> consumer,
                        RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler,
                        RemoteLogMetadataTopicPartitioner topicPartitioner,
                        Path committedOffsetsPath,
                        Time time,
                        long committedOffsetSyncIntervalMs) {
        this.consumer = consumer;
        this.remotePartitionMetadataEventHandler = remotePartitionMetadataEventHandler;
        this.topicPartitioner = topicPartitioner;
        this.time = time;
        this.committedOffsetSyncIntervalMs = committedOffsetSyncIntervalMs;
        initializeConsumerAssignment(committedOffsetsPath);
    }

    private void initializeConsumerAssignment(Path committedOffsetsPath) {
        // look whether the committed file exists or not.
        File file = committedOffsetsPath.toFile();
        committedOffsetsFile = new CommittedOffsetsFile(file);
        try {
            if (file.createNewFile()) {
                log.info("Created file: [{}] successfully", file);
            } else {
                // load committed offset and assign them in the consumer
                final Map<Integer, Long> committedOffsets = committedOffsetsFile.read();
                final Set<Map.Entry<Integer, Long>> entries = committedOffsets.entrySet();

                if (!entries.isEmpty()) {
                    // assign topic partitions from the earlier committed offsets file.
                    Set<Integer> earlierAssignedPartitions = committedOffsets.keySet();
                    assignedMetaPartitions.addAll(earlierAssignedPartitions);
                    Set<TopicPartition> metadataTopicPartitions = earlierAssignedPartitions.stream()
                                                                                        .map(x -> new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, x))
                                                                                        .collect(Collectors.toSet());
                    consumer.assign(metadataTopicPartitions);

                    // Seek to the committed offsets
                    for (Map.Entry<Integer, Long> entry : entries) {
                        partitionToConsumedOffsets.put(entry.getKey(), entry.getValue());
                        consumer.seek(new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, entry.getKey()), entry.getValue());
                    }
                }
            }
        } catch (IOException e) {
            // Ignore the error and consumer consumes from the earliest offset.
            log.error("Encountered error while building committed offsets from the file", e);
        }
    }

    @Override
    public void run() {
        log.info("Started Consumer task thread.");
        lastSyncedTimeMs = time.milliseconds();
        try {
            while (!close) {
                Set<Integer> assignedMetaPartitionsSnapshot = maybeWaitForPartitionsAssignment();

                if (!assignedMetaPartitionsSnapshot.isEmpty()) {
                    executeReassignment(assignedMetaPartitionsSnapshot);
                }

                log.info("Polling consumer to receive remote log metadata topic records");
                ConsumerRecords<byte[], byte[]> consumerRecords
                        = consumer.poll(Duration.ofSeconds(POLL_INTERVAL_MS));
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    handleRemoteLogMetadata(serde.deserialize(record.value()));
                    partitionToConsumedOffsets.put(record.partition(), record.offset());
                }

                syncCommittedDataAndOffsets(false);
            }
        } catch (Exception e) {
            log.error("Error occurred in consumer task, close:[{}]", close, e);
        } finally {
            closeConsumer();
        }

        log.info("Exiting from consumer task thread");
    }

    private void syncCommittedDataAndOffsets(boolean forceSync) {
        boolean noOffsetUpdates = committedPartitionToConsumedOffsets.equals(partitionToConsumedOffsets);
        if (noOffsetUpdates || !forceSync && time.milliseconds() - lastSyncedTimeMs < committedOffsetSyncIntervalMs) {
            log.debug("Skip syncing committed offsets, noOffsetUpdates: {}, forceSync: {}", noOffsetUpdates, forceSync);
            return;
        }

        try {
            // todo sync the snapshot file
            // remotePartitionMetadataEventHandler.syncLogMetadataDataFile();
            committedOffsetsFile.write(partitionToConsumedOffsets);
            committedPartitionToConsumedOffsets = new HashMap<>(partitionToConsumedOffsets);
            lastSyncedTimeMs = System.currentTimeMillis();
        } catch (IOException e) {
            log.error("Error encountered while writing committed offsets to a local file", e);
        }
    }

    private void closeConsumer() {
        log.info("Closing the consumer instance");
        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(30));
            } catch (Exception e) {
                log.error("Error encountered while closing the consumer", e);
            }
        }
    }

    private Set<Integer> maybeWaitForPartitionsAssignment() {
        Set<Integer> assignedMetaPartitionsSnapshot = Collections.emptySet();
        synchronized (assignPartitionsLock) {
            while (assignedMetaPartitions.isEmpty()) {
                // If no partitions are assigned, wait until they are assigned.
                log.info("Waiting for assigned remote log metadata partitions..");
                try {
                    assignPartitionsLock.wait();
                } catch (InterruptedException e) {
                    throw new KafkaException(e);
                }
            }

            if (assignPartitions) {
                assignedMetaPartitionsSnapshot = new HashSet<>(assignedMetaPartitions);
                assignPartitions = false;
            }
        }
        return assignedMetaPartitionsSnapshot;
    }

    private void handleRemoteLogMetadata(RemoteLogMetadata remoteLogMetadata) {
        if (assignedTopicPartitions.contains(remoteLogMetadata.topicIdPartition())) {
            remotePartitionMetadataEventHandler.handleRemoteLogMetadata(remoteLogMetadata);
        } else {
            log.debug("This event {} is skipped as the topic partition is not assigned for this instance.", remoteLogMetadata);
        }
    }

    private void executeReassignment(Set<Integer> assignedMetaPartitionsSnapshot) {
        Set<TopicPartition> assignedMetaTopicPartitions = assignedMetaPartitionsSnapshot.stream()
                .map(partitionNum -> new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partitionNum))
                .collect(Collectors.toSet());
        log.info("Reassigning partitions to consumer task [{}]", assignedMetaTopicPartitions);
        consumer.assign(assignedMetaTopicPartitions);
    }

    public void addAssignmentsForPartitions(Set<TopicIdPartition> updatedPartitions) {
        updateAssignmentsForPartitions(updatedPartitions, Collections.emptySet());
    }

    public void removeAssignmentsForPartitions(Set<TopicIdPartition> partitions) {
        updateAssignmentsForPartitions(Collections.emptySet(), partitions);
    }

    private void updateAssignmentsForPartitions(Set<TopicIdPartition> addedPartitions,
                                                Set<TopicIdPartition> removedPartitions) {
        log.info("Updating assignments for addedPartitions: {} and removedPartition: {}", addedPartitions, removedPartitions);
        ensureNotClosed();

        Objects.requireNonNull(addedPartitions, "addedPartitions must not be null");
        Objects.requireNonNull(removedPartitions, "removedPartitions must not be null");

        if (addedPartitions.isEmpty() && removedPartitions.isEmpty()) {
            return;
        }

        synchronized (assignPartitionsLock) {
            Set<TopicIdPartition> updatedReassignedPartitions = new HashSet<>(assignedTopicPartitions);
            updatedReassignedPartitions.addAll(addedPartitions);
            updatedReassignedPartitions.removeAll(removedPartitions);
            Set<Integer> updatedAssignedMetaPartitions = new HashSet<>();
            for (TopicIdPartition tp : updatedReassignedPartitions) {
                updatedAssignedMetaPartitions.add(topicPartitioner.metadataPartition(tp));
            }
            assignedTopicPartitions = Collections.unmodifiableSet(updatedReassignedPartitions);
            log.debug("Assigned topic partitions: {}", assignedTopicPartitions);

            if (!updatedAssignedMetaPartitions.equals(assignedMetaPartitions)) {
                assignedMetaPartitions = Collections.unmodifiableSet(updatedAssignedMetaPartitions);
                log.debug("Assigned metadata topic partitions: {}", assignedMetaPartitions);
                assignPartitions = true;
                assignPartitionsLock.notifyAll();
            } else {
                log.debug("No change in assigned metadata topic partitions: {}", assignedMetaPartitions);
            }
        }
    }

    public Optional<Long> receivedOffsetForPartition(int partition) {
        return Optional.ofNullable(partitionToConsumedOffsets.get(partition));
    }

    public boolean assignedPartition(int partition) {
        return assignedMetaPartitions.contains(partition);
    }

    private void ensureNotClosed() {
        if (close) {
            throw new IllegalStateException("This instance is already closed");
        }
    }

    public void close() {
        if (!close) {
            close = true;
            consumer.wakeup();
            syncCommittedDataAndOffsets(true);
        }
    }
}