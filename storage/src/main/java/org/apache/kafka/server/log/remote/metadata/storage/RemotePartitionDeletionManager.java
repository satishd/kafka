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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME;

public class RemotePartitionDeletionManager implements Runnable, Closeable {
    private static final Logger log = LoggerFactory.getLogger(RemotePartitionDeletionManager.class);

    private static final long POLL_INTERVAL_MS = 50L;
    private static final String REMOTE_PARTITION_DELETION_GROUP = "__remote_partition_deletion_group";

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final int brokerId;
    private final Time time;
    private final RemoteStorageManager remoteStorageManager;
    private final RemoteLogMetadataManager remoteLogMetadataManager;

    // It indicates whether the closing process has been started or not. If it is set as true,
    // consumer will stop consuming messages and it will not allow partition assignments to be updated.
    private volatile boolean closing = false;
    private final ReentrantReadWriteLock rwCloseLock = new ReentrantReadWriteLock();
    private volatile boolean assignPartitions = false;

    private final Object assignPartitionsLock = new Object();

    // Remote log metadata topic partitions that consumer is assigned to.
    private volatile Set<TopicPartition> assignedMetaPartitions = Collections.emptySet();

    public RemotePartitionDeletionManager(Map<String, Object> consumerProps,
                                          RemoteStorageManager remoteStorageManager,
                                          RemoteLogMetadataManager remoteLogMetadataManager,
                                          Time time,
                                          int brokerId) {
        this.remoteStorageManager = remoteStorageManager;
        this.remoteLogMetadataManager = remoteLogMetadataManager;
        this.time = time;
        this.brokerId = brokerId;
        consumer = initializeConsumer(consumerProps);
    }

    private KafkaConsumer<byte[], byte[]> initializeConsumer(Map<String, Object> consumerProps) {
        HashMap<String, Object> props = new HashMap<>(consumerProps);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, REMOTE_PARTITION_DELETION_GROUP);

        return new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        log.info("Started RemotePartitionDeletionManager task thread.");
        rwCloseLock.readLock().lock();
        try {
            while (!closing) {
                Set<TopicPartition> assignedMetaPartitionsSnapshot = maybeWaitForPartitionsAssignment();
                if (!assignedMetaPartitionsSnapshot.isEmpty()) {
                    log.info("Reassigning partitions to consumer task [{}]", assignedMetaPartitionsSnapshot);
                    consumer.assign(assignedMetaPartitionsSnapshot);
                }

                log.info("Polling consumer to receive remote log metadata topic records");
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofSeconds(POLL_INTERVAL_MS));
                for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    try {
                        RemoteLogMetadata remoteLogMetadata = serde.deserialize(record.value());
                        handleRemoteLogMetadata(remoteLogMetadata);
                    } catch (RemoteStorageException e) {
                        log.error("Error occurred while deleting the segment", e);
                        // todo-tier retry for a few times before giving up on that segment.
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error occurred in remote partition deletion task, close:[{}]", closing, e);
            // todo-tier add a mechanism to restart the thread if needed.
        } finally {
            log.info("Exiting from remote partition deletion task thread");
            rwCloseLock.readLock().unlock();
            closeConsumer();
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

    private Set<TopicPartition> maybeWaitForPartitionsAssignment() {
        Set<TopicPartition> assignedMetaPartitionsSnapshot = Collections.emptySet();
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

    private void handleRemoteLogMetadata(RemoteLogMetadata remoteLogMetadata) throws RemoteStorageException {
        // Process remote log deletion updates and skip other updates.
        if (remoteLogMetadata instanceof RemotePartitionDeleteMetadata) {
            RemotePartitionDeleteMetadata partitionDeleteMetadata = (RemotePartitionDeleteMetadata) remoteLogMetadata;
            if (partitionDeleteMetadata.state() == RemotePartitionDeleteState.DELETE_PARTITION_MARKED ||
                    partitionDeleteMetadata.state() == RemotePartitionDeleteState.DELETE_PARTITION_STARTED) {
                //Start deleting the remaining segments.
                Iterator<RemoteLogSegmentMetadata> remoteLogSegmentMetadataIter = remoteLogMetadataManager
                        .listRemoteLogSegments(remoteLogMetadata.topicIdPartition());
                while (remoteLogSegmentMetadataIter.hasNext()) {
                    RemoteLogSegmentMetadata remoteLogSegmentMetadata = remoteLogSegmentMetadataIter.next();

                    // Delete the segment if it was not deleted earlier.
                    if (remoteLogSegmentMetadata.state() != RemoteLogSegmentState.DELETE_SEGMENT_FINISHED) {
                        // Update the remote log segment state to DELETE_SEGMENT_STARTED
                        remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
                                new RemoteLogSegmentMetadataUpdate(remoteLogSegmentMetadata.remoteLogSegmentId(), time.milliseconds(),
                                                                   RemoteLogSegmentState.DELETE_SEGMENT_STARTED, brokerId));
                        // Delete the segment in remote storage.
                        try {
                            remoteStorageManager.deleteLogSegmentData(remoteLogSegmentMetadata);
                        } catch (RemoteResourceNotFoundException e) {
                            // Skip the segment if it does not exist. This may have been deleted in the earlier runs.
                            log.warn("Error encountered while trying to delete segment: [{}]", remoteLogSegmentMetadataIter, e);
                        }

                        // Update the remote log segment state to DELETE_SEGMENT_FINISHED
                        remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
                                new RemoteLogSegmentMetadataUpdate(remoteLogSegmentMetadata.remoteLogSegmentId(), time.milliseconds(),
                                                                   RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId));
                    }
                }
            }
        }
    }

    public void updateAssignmentsForLeaderPartitions(Set<TopicIdPartition> leaderPartitions) {
        log.info("Updating assignments for leaderPartitions: {}", leaderPartitions);

        Objects.requireNonNull(leaderPartitions, "leaderPartitions must not be null");

        if (leaderPartitions.isEmpty()) {
            return;
        }
        // Check only for remote log metadata topic partitions.
        HashSet<TopicPartition> partitions = new HashSet<>();
        for (TopicIdPartition leaderPartition : leaderPartitions) {
            if (leaderPartition.topicPartition().topic().equals(REMOTE_LOG_METADATA_TOPIC_NAME)) {
                partitions.add(leaderPartition.topicPartition());
            }
        }

        synchronized (assignPartitionsLock) {
            if (!partitions.equals(assignedMetaPartitions)) {
                assignedMetaPartitions = Collections.unmodifiableSet(partitions);
                log.debug("Assigned metadata topic partitions: {}", assignedMetaPartitions);
                assignPartitions = true;
                assignPartitionsLock.notifyAll();
            } else {
                log.debug("No change in assigned metadata topic partitions: {}", assignedMetaPartitions);
            }
        }
    }

    public void close() {
        rwCloseLock.writeLock().lock();
        try {
            if (!closing) {
                closing = true;
                consumer.wakeup();
            }
        } finally {
            rwCloseLock.writeLock().unlock();
        }
    }
}
