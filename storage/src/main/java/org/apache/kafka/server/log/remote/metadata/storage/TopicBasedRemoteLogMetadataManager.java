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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is the {@link RemoteLogMetadataManager} implementation with storage as an internal topic with name {@link TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_TOPIC_NAME}.
 * This is used to publish and fetch {@link RemoteLogMetadata} for the registered user topic partitions with
 * {@link #onPartitionLeadershipChanges(Set, Set)}. Each broker will have an instance of this class and it subscribes
 * to metadata updates for the registered user topic partitions.
 */
public class TopicBasedRemoteLogMetadataManager implements RemoteLogMetadataManager {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManager.class);

    private boolean closed = false;
    private boolean initialized = false;
    private final Time time = Time.SYSTEM;
    private final boolean startConsumerThread;

    private volatile ProducerManager producerManager;
    private volatile ConsumerManager consumerManager;

    // This allows to gracefully close this instance using {@link #close()} method while there are some pending or new
    // requests calling different methods which use the resources like producer/consumer managers.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private RemotePartitionMetadataStore remotePartitionMetadataStore;
    private volatile TopicBasedRemoteLogMetadataManagerConfig rlmmConfig;
    private volatile RemoteLogMetadataTopicPartitioner rlmmTopicPartitioner;

    public TopicBasedRemoteLogMetadataManager() {
        this(true);
    }

    // Visible for testing.
    public TopicBasedRemoteLogMetadataManager(boolean startConsumerThread) {
        this.startConsumerThread = startConsumerThread;
    }

    @Override
    public void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        // This allows gracefully rejecting the requests while closing of this instance is in progress, which triggers
        // closing the producer/consumer manager instances.
        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            // This method is allowed only to add remote log segment with the initial state(which is RemoteLogSegmentState.COPY_SEGMENT_STARTED)
            // but not to update the existing remote log segment metadata.
            if (remoteLogSegmentMetadata.state() != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                throw new IllegalArgumentException(
                        "Given remoteLogSegmentMetadata should have state as " + RemoteLogSegmentState.COPY_SEGMENT_STARTED
                        + " but it contains state as: " + remoteLogSegmentMetadata.state());
            }

            // Publish the message to the topic.
            doPublishMetadata(remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition(),
                              remoteLogSegmentMetadata);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate segmentMetadataUpdate)
            throws RemoteStorageException {
        Objects.requireNonNull(segmentMetadataUpdate, "segmentMetadataUpdate can not be null");

        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            // Callers should use addRemoteLogSegmentMetadata to add RemoteLogSegmentMetadata with state as
            // RemoteLogSegmentState.COPY_SEGMENT_STARTED.
            if (segmentMetadataUpdate.state() == RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                throw new IllegalArgumentException("Given remoteLogSegmentMetadata should not have the state as: "
                                                   + RemoteLogSegmentState.COPY_SEGMENT_STARTED);
            }

            // Publish the message to the topic.
            doPublishMetadata(segmentMetadataUpdate.remoteLogSegmentId().topicIdPartition(), segmentMetadataUpdate);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remotePartitionDeleteMetadata, "remotePartitionDeleteMetadata can not be null");

        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            doPublishMetadata(remotePartitionDeleteMetadata.topicIdPartition(), remotePartitionDeleteMetadata);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void doPublishMetadata(TopicIdPartition topicIdPartition, RemoteLogMetadata remoteLogMetadata)
            throws RemoteStorageException {
        log.debug("Publishing metadata for partition: [{}] with context: [{}]", topicIdPartition, remoteLogMetadata);

        try {
            // Publish the message to the topic.
            RecordMetadata recordMetadata = producerManager.publishMessage(remoteLogMetadata);
            // Wait until the consumer catches up with this offset. This will ensure read-after-write consistency
            // semantics.
            consumerManager.waitTillConsumptionCatchesUp(recordMetadata);
        } catch (KafkaException e) {
            if (e instanceof RetriableException) {
                throw e;
            } else {
                throw new RemoteStorageException(e);
            }
        }
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                       int epochForOffset,
                                                                       long offset)
            throws RemoteStorageException {
        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            return remotePartitionMetadataStore.remoteLogSegmentMetadata(topicIdPartition, offset, epochForOffset);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<Long> highestOffsetForEpoch(TopicIdPartition topicIdPartition,
                                                int leaderEpoch)
            throws RemoteStorageException {
        lock.readLock().lock();
        try {

            ensureInitializedAndNotClosed();

            return remotePartitionMetadataStore.highestLogOffset(topicIdPartition, leaderEpoch);
        } finally {
            lock.readLock().unlock();
        }

    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            return remotePartitionMetadataStore.listRemoteLogSegments(topicIdPartition);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition, int leaderEpoch)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            return remotePartitionMetadataStore.listRemoteLogSegments(topicIdPartition, leaderEpoch);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int metadataPartition(TopicIdPartition topicIdPartition) {
        return rlmmTopicPartitioner.metadataPartition(topicIdPartition);
    }

    // Visible For Testing
    public Optional<Long> receivedOffsetForPartition(int metadataPartition) {
        return consumerManager.receivedOffsetForPartition(metadataPartition);
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                             Set<TopicIdPartition> followerPartitions) {
        Objects.requireNonNull(leaderPartitions, "leaderPartitions can not be null");
        Objects.requireNonNull(followerPartitions, "followerPartitions can not be null");

        log.info("Received leadership notifications with leader partitions {} and follower partitions {}",
                 leaderPartitions, followerPartitions);

        HashSet<TopicIdPartition> allPartitions = new HashSet<>(leaderPartitions);
        allPartitions.addAll(followerPartitions);
        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();
            for (TopicIdPartition partition : allPartitions) {
                remotePartitionMetadataStore.maybeLoadPartition(partition);
            }
            consumerManager.addAssignmentsForPartitions(allPartitions);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void onStopPartitions(Set<TopicIdPartition> partitions) {
        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();
            consumerManager.removeAssignmentsForPartitions(partitions);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs can not be null.");

        lock.writeLock().lock();
        try {
            if (!initialized) {
                log.info("Started initializing with configs: {}", configs);

                rlmmConfig = new TopicBasedRemoteLogMetadataManagerConfig(configs);
                rlmmTopicPartitioner = new RemoteLogMetadataTopicPartitioner(rlmmConfig.metadataTopicPartitionsCount());
                remotePartitionMetadataStore = new RemotePartitionMetadataStore(new File(rlmmConfig.logDir()).toPath());
                log.info("Successfully initialized with rlmmConfig: {}", rlmmConfig);

                producerManager = new ProducerManager(rlmmConfig, rlmmTopicPartitioner);
                consumerManager = new ConsumerManager(rlmmConfig, remotePartitionMetadataStore, rlmmTopicPartitioner, time);
                if (startConsumerThread) {
                    consumerManager.startConsumerThread();
                }
                initialized = true;
                log.info("Initialized resources successfully.");
            }
        } catch (Exception e) {
            throw new KafkaException("Encountered error while initializing producer/consumer");
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isInitialized() {
        lock.readLock().lock();
        try {
            return initialized;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void ensureInitializedAndNotClosed() {
        if (closed || !initialized) {
            throw new IllegalStateException("This instance is in invalid state, initialized: " + initialized +
                                                    " closed: " + closed);
        }
    }

    // Visible for testing.
    public TopicBasedRemoteLogMetadataManagerConfig config() {
        return rlmmConfig;
    }

    // Visible for testing.
    public void startConsumerThread() {
        if (consumerManager != null) {
            consumerManager.startConsumerThread();
        }
    }

    @Override
    public void close() throws IOException {
        // Close all the resources.
        log.info("Closing the resources.");
        lock.writeLock().lock();
        try {
            if (!closed) {
                Utils.closeQuietly(producerManager, "ProducerTask");
                Utils.closeQuietly(consumerManager, "RLMMConsumerManager");
                Utils.closeQuietly(remotePartitionMetadataStore, "RemotePartitionMetadataStore");
                closed = true;
            }
        } finally {
            lock.writeLock().unlock();
            log.info("Closed the resources.");
        }
    }
}
