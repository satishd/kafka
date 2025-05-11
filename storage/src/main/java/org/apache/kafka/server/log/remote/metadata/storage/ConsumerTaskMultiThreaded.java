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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * For any new assignment we need to seek to the beginning of the partition and consume all the messages stored in the
 * topic. This task is now delegated to secondary threads called {@link CatchupConsumer}. These threads run from the beginning
 * offset of the partition to the end offset that is stored at the creation time of these threads. After these threads
 * are completed, these partitions get re-assigned back to the primary thread.
 * <p>
 * After receiving these events it invokes {@link RemotePartitionMetadataEventHandler#handleRemoteLogSegmentMetadata(RemoteLogSegmentMetadata)},
 * which maintains in-memory representation of the state of {@link RemoteLogSegmentMetadata}.
 */
public class ConsumerTaskMultiThreaded implements IConsumerTask {
    private static final Logger log = LoggerFactory.getLogger(ConsumerTaskMultiThreaded.class);

    private final RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
    private final Consumer<byte[], byte[]> primaryConsumer;
    private final Function<Optional<String>, Consumer<byte[], byte[]>> consumerSupplier;
    private final RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler;
    private final RemoteLogMetadataTopicPartitioner topicPartitioner;
    // The timeout for the consumer to poll records from the remote log metadata topic.
    private final long pollTimeoutMs;
    private final Time time;

    // It indicates whether the ConsumerTask is closed or not.
    private volatile boolean isClosed = false;
    // It indicates whether the user topic partition assignment to the consumer has changed or not. If the assignment
    // has changed, the consumer will eventually start tracking the newly assigned partitions and stop tracking the
    // ones it is no longer assigned to.
    // The initial value is set to true to wait for partition assignment on the first execution; otherwise thread will
    // be busy without actually doing anything
    private volatile boolean hasAssignmentChanged = true;

    // It represents a lock for any operations related to the assignedTopicPartitions.
    private final Object assignPartitionsLock = new Object();

    // Remote log metadata topic partitions that consumer is assigned to.
    private volatile Set<Integer> assignedMetadataPartitions = Collections.emptySet();

    // User topic partitions that this broker is a leader/follower for.
    private volatile Map<TopicIdPartition, UserTopicIdPartition> assignedUserTopicIdPartitions = Collections.emptyMap();
    private volatile Set<TopicIdPartition> processedAssignmentOfUserTopicIdPartitions = Collections.emptySet();

    private long uninitializedAt;
    private boolean isAllUserTopicPartitionsInitialized;

    // Map of remote log metadata topic partition to consumed offsets.
    private final Map<Integer, Long> readOffsetsByMetadataPartition = new ConcurrentHashMap<>();
    private volatile Map<TopicIdPartition, Long> readOffsetsByUserTopicPartition = new ConcurrentHashMap<>();

    private Map<TopicPartition, StartAndEndOffsetHolder> offsetHolderByMetadataPartition = new HashMap<>();
    private boolean hasLastOffsetsFetchFailed = false;
    private long lastFailedFetchOffsetsTimestamp;
    // The interval between retries to fetch the start and end offsets for the metadata partitions after a failed fetch.
    private final long offsetFetchRetryIntervalMs;

    private final ExecutorService catchupConsumerExecutorService = Executors.newCachedThreadPool(ThreadUtils
            .createThreadFactory("RLMMCatchupConsumer-%d", false));
    private Map<TopicPartition, CatchupConsumerInfo> activeCatchupConsumers = new HashMap<>();
    private int errorRetryBackoffMs = 5000;

    public ConsumerTaskMultiThreaded(final RemotePartitionMetadataEventHandler remotePartitionMetadataEventHandler,
                                     final RemoteLogMetadataTopicPartitioner topicPartitioner,
                                     final Function<Optional<String>, Consumer<byte[], byte[]>> consumerSupplier,
                                     long pollTimeoutMs,
                                     long offsetFetchRetryIntervalMs,
                                     Time time) {
        this.primaryConsumer = consumerSupplier.apply(Optional.empty());
        this.remotePartitionMetadataEventHandler = Objects.requireNonNull(remotePartitionMetadataEventHandler);
        this.topicPartitioner = Objects.requireNonNull(topicPartitioner);
        this.pollTimeoutMs = pollTimeoutMs;
        this.offsetFetchRetryIntervalMs = offsetFetchRetryIntervalMs;
        this.time = Objects.requireNonNull(time);
        this.uninitializedAt = time.milliseconds();
        this.consumerSupplier = consumerSupplier;
    }

    @Override
    public void run() {
        log.info("Starting consumer task thread.");
        while (!isClosed) {
            ingestRecords();
        }
        closeConsumer();
        log.info("Exited from consumer task thread");
    }

    // public for testing
    public void ingestRecords() {
        try {
            if (hasAssignmentChanged) {
                maybeWaitForPartitionAssignments();
            }
            handleCatchupThreadsCompletion();
            pollAndProcessRecords(primaryConsumer);
            maybeMarkUserPartitionsAsReady();
        } catch (final WakeupException ex) {
            // ignore logging the error
            isClosed = true;
        } catch (final RetriableException ex) {
            log.warn("Retriable error occurred while processing the records. Retrying...", ex);
        } catch (final Exception ex) {
            // Don't close the consumer, retry on any exception. Sleep added to avoid busy loop.
            log.error("Error occurred while processing the records. Retrying...", ex);
            if (!isClosed) {
                try {
                    Thread.sleep(errorRetryBackoffMs);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    // public for testing
    public void closeConsumer() {
        try {
            primaryConsumer.close(Duration.ofSeconds(30));
        } catch (final Exception e) {
            log.error("Error encountered while closing the consumer", e);
        }
        log.info("Exited from consumer task thread");
    }

    private void pollAndProcessRecords(Consumer<byte[], byte[]> consumer) {
        if (!consumer.assignment().isEmpty()) {
            final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(pollTimeoutMs));
            if (!consumerRecords.isEmpty()) {
                log.debug("Processing {} records", consumerRecords.count());
                for (final ConsumerRecord<byte[], byte[]> record : consumerRecords) {
                    final RemoteLogMetadata remoteLogMetadata = serde.deserialize(record.value());
                    if (shouldProcess(remoteLogMetadata, record.offset())) {
                        remotePartitionMetadataEventHandler.handleRemoteLogMetadata(remoteLogMetadata);
                        readOffsetsByUserTopicPartition.put(remoteLogMetadata.topicIdPartition(), record.offset());
                    } else {
                        log.trace("The event {} is skipped because it is either already processed or not assigned to this consumer",
                                remoteLogMetadata);
                    }
                    log.trace("Updating consumed offset: {} for partition {}", record.offset(), record.partition());
                    readOffsetsByMetadataPartition.put(record.partition(), record.offset());
                }
            }
        }
    }

    private boolean shouldProcess(final RemoteLogMetadata metadata, final long recordOffset) {
        final TopicIdPartition tpId = metadata.topicIdPartition();
        final Long readOffset = readOffsetsByUserTopicPartition.get(tpId);
        return processedAssignmentOfUserTopicIdPartitions.contains(tpId) && (readOffset == null || readOffset < recordOffset);
    }

    private void maybeMarkUserPartitionsAsReady() {
        if (isAllUserTopicPartitionsInitialized) {
            return;
        }
        maybeFetchStartAndEndOffsets();
        boolean isAllInitialized = true;
        for (final UserTopicIdPartition utp : assignedUserTopicIdPartitions.values()) {
            if (utp.isAssigned && !utp.isInitialized) {
                final Integer metadataPartition = utp.metadataPartition;
                final StartAndEndOffsetHolder holder = offsetHolderByMetadataPartition.get(toRemoteLogPartition(metadataPartition));
                // The offset-holder can be null, when the recent assignment wasn't picked up by the consumer.
                if (holder != null) {
                    final Long readOffset = readOffsetsByMetadataPartition.getOrDefault(metadataPartition, -1L);
                    // 1) The end-offset was fetched only once during reassignment. The metadata-partition can receive
                    // new stream of records, so the consumer can read records more than the last-fetched end-offset.
                    // 2) When the internal topic becomes empty due to breach by size/time/start-offset, then there
                    // are no records to read.
                    if (readOffset + 1 >= holder.endOffset || holder.endOffset.equals(holder.startOffset)) {
                        markInitialized(utp);
                    } else {
                        log.debug("The user-topic-partition {} could not be marked initialized since the read-offset is {} " +
                                "but the end-offset is {} for the metadata-partition {}", utp, readOffset, holder.endOffset,
                            metadataPartition);
                    }
                } else {
                    log.debug("The offset-holder is null for the metadata-partition {}. The consumer may not have picked" +
                            " up the recent assignment", metadataPartition);
                }
            }
            isAllInitialized = isAllInitialized && utp.isAssigned && utp.isInitialized;
        }
        if (isAllInitialized) {
            log.info("Initialized for all the {} assigned user-partitions mapped to the {} meta-partitions in {} ms",
                assignedUserTopicIdPartitions.size(), assignedMetadataPartitions.size(),
                time.milliseconds() - uninitializedAt);
        }
        isAllUserTopicPartitionsInitialized = isAllInitialized;
    }

    private void handleCatchupThreadsCompletion() {
        if (!activeCatchupConsumers.isEmpty()) {
            // If primary consumer has no assignment then wait for atleast one of catch up consumer to finish
            if (primaryConsumer.assignment().isEmpty()) {
                CompletableFuture<?>[] cfs = activeCatchupConsumers.values()
                        .stream().map(m -> m.future).toArray(CompletableFuture[]::new);
                try {
                    // Wait for a minute since there are no assignments to the primary consumer.
                    // Added a timeout of 1 minute so that we can pick up any new assignment that happened while we are waiting
                    CompletableFuture.anyOf(cfs).get(1, TimeUnit.MINUTES);
                } catch (Exception e) {
                    // Swallow exception since we can fall back on primary consumer to continue where ever catch up thread left off at
                    log.error("Waiting for catch up consumers did not exit gracefully");
                }
            }
            // Check if any catch up consumer is done
            boolean isAnyConsumerCaughtUp = activeCatchupConsumers.keySet().removeIf(tp -> activeCatchupConsumers.get(tp).future.isDone());
            // If done then assign that metadata partition to the primary consumer
            if (isAnyConsumerCaughtUp) {
                Set<TopicPartition> topicPartitions = toRemoteLogPartitions(this.assignedMetadataPartitions);
                topicPartitions.removeAll(activeCatchupConsumers.keySet());
                assignPartitionsToPrimaryConsumer(topicPartitions);
            }
        }
    }

    void maybeWaitForPartitionAssignments() throws InterruptedException {
        // Snapshots of the metadata-partition and user-topic-partition are used to reduce the scope of the
        // synchronization block.
        // 1) LEADER_AND_ISR and STOP_REPLICA requests adds / removes the user-topic-partitions from the request
        //    handler threads. Those threads should not be blocked for a long time, therefore scope of the
        //    synchronization block is reduced to bare minimum.
        // 2) Note that the consumer#position, consumer#seekToBeginning, consumer#seekToEnd and the other consumer APIs
        //    response times are un-predictable. Those should not be kept in the synchronization block.
        final Set<Integer> metadataPartitionSnapshot = new HashSet<>();
        final Set<UserTopicIdPartition> assignedUserTopicIdPartitionsSnapshot = new HashSet<>();
        synchronized (assignPartitionsLock) {
            while (!isClosed && assignedUserTopicIdPartitions.isEmpty()) {
                log.debug("Waiting for remote log metadata partitions to be assigned");
                assignPartitionsLock.wait();
            }
            if (!isClosed && hasAssignmentChanged) {
                assignedUserTopicIdPartitions.values().forEach(utp -> {
                    metadataPartitionSnapshot.add(utp.metadataPartition);
                    assignedUserTopicIdPartitionsSnapshot.add(utp);
                });
                hasAssignmentChanged = false;
            }
        }
        if (!metadataPartitionSnapshot.isEmpty()) {
            final Set<TopicPartition> remoteLogPartitions = toRemoteLogPartitions(metadataPartitionSnapshot);
            this.assignedMetadataPartitions = Collections.unmodifiableSet(metadataPartitionSnapshot);

            // Identify the newly assigned user topic partitions
            final Set<TopicPartition> metadataPartitionsForNewUtps = assignedUserTopicIdPartitionsSnapshot
                    .stream()
                    .filter(utp -> !utp.isAssigned)
                    .map(utp -> toRemoteLogPartition(utp.metadataPartition))
                    .collect(Collectors.toSet());

            final Set<TopicPartition> metadataPartitionsCatchingUp = activeCatchupConsumers.keySet();

            // Actively catching up consumers need to be killed if they have a new utp assigned
            HashSet<TopicPartition> catchupConsumersToKill =
                    new HashSet<>(metadataPartitionsForNewUtps);
            catchupConsumersToKill.retainAll(metadataPartitionsCatchingUp);
            catchupConsumersToKill.forEach(tp -> activeCatchupConsumers.get(tp).catchupConsumer.close());

            CompletableFuture<?>[] cfs = catchupConsumersToKill.stream().map(tp -> activeCatchupConsumers.get(tp).future)
                    .toArray(CompletableFuture[]::new);
            try {
                // Wait for 2x the poll wait time for the closed runnables to complete execution
                CompletableFuture.allOf(cfs).get(pollTimeoutMs * 2, TimeUnit.MILLISECONDS);
            } catch (CancellationException | ExecutionException e) {
                // Ignore any runnable execution failures
            } catch (TimeoutException e) {
                log.warn("Timeout[200ms] while waiting for catchup consumers to close");
                // Reset isAssignmentChanged and return from the method. The cancellation will be retried again.
                hasAssignmentChanged = true;
                return;
            }

            catchupConsumersToKill.forEach(tp -> activeCatchupConsumers.remove(tp));

            processedAssignmentOfUserTopicIdPartitions = assignedUserTopicIdPartitionsSnapshot.stream()
                    .map(utp -> utp.topicIdPartition).collect(Collectors.toSet());

            // For all newly assigned utps we need to start a new catch up thread
            metadataPartitionsForNewUtps.forEach(tp -> {
                try {
                    // If not removed then new utp could be potentially marked initialized. The read offset value for the
                    // partition will now only be updated inside the new catchup thread. In the interim the primary thread
                    // might read the old set value and mark the new utp as intialized.
                    readOffsetsByMetadataPartition.remove(tp.partition());
                    CatchupConsumer catchupConsumer = new CatchupConsumer(tp);
                    CompletableFuture<?> completableFuture = CompletableFuture.runAsync(catchupConsumer, catchupConsumerExecutorService);
                    activeCatchupConsumers.put(tp, new CatchupConsumerInfo(completableFuture, catchupConsumer));
                } catch (Exception e) {
                    // Swallow exception since this failure usually happens due to offline partitions. The failed tp will
                    // now be assigned to the primary consumer.
                    log.error("Exception when trying to assign metadata partition {} to catch up threads", tp.partition(), e);
                }
            });

            // All metadata partitions not catching up on a separate thread are assigned to primary consumer
            final Set<TopicPartition> caughtUpMetadataPartitions = remoteLogPartitions.stream()
                    .filter(tp -> !activeCatchupConsumers.containsKey(tp)).collect(Collectors.toSet());
            assignPartitionsToPrimaryConsumer(caughtUpMetadataPartitions);

            Set<TopicIdPartition> processedAssignmentPartitions = new HashSet<>();
            // mark all the user-topic-partitions as assigned to the consumer.
            assignedUserTopicIdPartitionsSnapshot.forEach(utp -> {
                if (!utp.isAssigned) {
                    // Note that there can be a race between `remove` and `add` partition assignment. Calling the
                    // `maybeLoadPartition` here again to be sure that the partition gets loaded on the handler.
                    remotePartitionMetadataEventHandler.maybeLoadPartition(utp.topicIdPartition);
                    utp.isAssigned = true;
                }
                processedAssignmentPartitions.add(utp.topicIdPartition);
            });

            clearResourcesForUnassignedUserTopicPartitions(processedAssignmentPartitions);
            isAllUserTopicPartitionsInitialized = false;
            uninitializedAt = time.milliseconds();
            fetchStartAndEndOffsets();
        }
    }

    /**
     * This method assigns the given topic partitions to the primary consumer. It also seeks to the last read offset for the topic partition.
     *
     * @param caughtUpMetadataPartitions the topic partitions to assign to the primary consumer
     */
    private void assignPartitionsToPrimaryConsumer(Set<TopicPartition> caughtUpMetadataPartitions) {
        primaryConsumer.assign(caughtUpMetadataPartitions);
        caughtUpMetadataPartitions.forEach(tp -> {
            if (readOffsetsByMetadataPartition.containsKey(tp.partition())) {
                primaryConsumer.seek(tp, readOffsetsByMetadataPartition.get(tp.partition()));
            }
        });
    }

    private void clearResourcesForUnassignedUserTopicPartitions(Set<TopicIdPartition> assignedPartitions) {
        // Note that there can be previously assigned user-topic-partitions where no records are there to read
        // (eg) none of the segments for a partition were uploaded. Those partition resources won't be cleared.
        // It can be fixed later when required since they are empty resources.
        Set<TopicIdPartition> unassignedPartitions = readOffsetsByUserTopicPartition.keySet()
            .stream()
            .filter(e -> !assignedPartitions.contains(e))
            .collect(Collectors.toSet());
        unassignedPartitions.forEach(unassignedPartition -> {
            remotePartitionMetadataEventHandler.clearTopicPartition(unassignedPartition);
            readOffsetsByUserTopicPartition.remove(unassignedPartition);
        });
        log.info("Unassigned user-topic-partitions: {}", unassignedPartitions.size());
    }

    public void addAssignmentsForPartitions(final Set<TopicIdPartition> partitions) {
        updateAssignments(Objects.requireNonNull(partitions), Collections.emptySet());
    }

    public void removeAssignmentsForPartitions(final Set<TopicIdPartition> partitions) {
        updateAssignments(Collections.emptySet(), Objects.requireNonNull(partitions));
    }

    private void updateAssignments(final Set<TopicIdPartition> addedPartitions,
                                   final Set<TopicIdPartition> removedPartitions) {
        log.info("Updating assignments for partitions added: {} and removed: {}", addedPartitions, removedPartitions);
        if (!addedPartitions.isEmpty() || !removedPartitions.isEmpty()) {
            synchronized (assignPartitionsLock) {
                // Make a copy of the existing assignments and update the copy.
                final Map<TopicIdPartition, UserTopicIdPartition> updatedUserPartitions = new HashMap<>(assignedUserTopicIdPartitions);
                addedPartitions.forEach(tpId -> updatedUserPartitions.putIfAbsent(tpId, newUserTopicIdPartition(tpId)));
                removedPartitions.forEach(updatedUserPartitions::remove);
                if (!updatedUserPartitions.equals(assignedUserTopicIdPartitions)) {
                    assignedUserTopicIdPartitions = Collections.unmodifiableMap(updatedUserPartitions);
                    hasAssignmentChanged = true;
                    log.debug("Assigned user-topic-partitions: {}", assignedUserTopicIdPartitions);
                    assignPartitionsLock.notifyAll();
                }
            }
        }
    }

    public Optional<Long> readOffsetForMetadataPartition(final int partition) {
        return Optional.ofNullable(readOffsetsByMetadataPartition.get(partition));
    }

    public boolean isMetadataPartitionAssigned(final int partition) {
        return assignedMetadataPartitions.contains(partition);
    }

    public boolean isUserPartitionAssigned(final TopicIdPartition partition) {
        final UserTopicIdPartition utp = assignedUserTopicIdPartitions.get(partition);
        return utp != null && utp.isAssigned;
    }

    @Override
    public void close() {
        if (!isClosed) {
            log.info("Closing the instance");
            synchronized (assignPartitionsLock) {
                isClosed = true;
                assignedUserTopicIdPartitions.values().forEach(this::markInitialized);
                catchupConsumerExecutorService.shutdownNow();
                try {
                    catchupConsumerExecutorService.awaitTermination(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    log.error("Could not close the catchup consumer executor service within given timeout", e);
                }
                primaryConsumer.wakeup();
                assignPartitionsLock.notifyAll();
            }
        }
    }

    public Set<Integer> metadataPartitionsAssigned() {
        return Collections.unmodifiableSet(assignedMetadataPartitions);
    }

    private void fetchStartAndEndOffsets() {
        try {
            final Set<TopicPartition> uninitializedPartitions = assignedUserTopicIdPartitions.values().stream()
                .filter(utp -> utp.isAssigned && !utp.isInitialized)
                .map(utp -> toRemoteLogPartition(utp.metadataPartition))
                .collect(Collectors.toSet());
            // Removing the previous offset holder if it exists. During reassignment, if the list-offset
            // call to `earliest` and `latest` offset fails, then we should not use the previous values.
            uninitializedPartitions.forEach(tp -> offsetHolderByMetadataPartition.remove(tp));
            if (!uninitializedPartitions.isEmpty()) {
                Map<TopicPartition, Long> endOffsets = primaryConsumer.endOffsets(uninitializedPartitions);
                Map<TopicPartition, Long> startOffsets = primaryConsumer.beginningOffsets(uninitializedPartitions);
                offsetHolderByMetadataPartition = endOffsets.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> new StartAndEndOffsetHolder(startOffsets.get(e.getKey()), e.getValue())));

            }
            hasLastOffsetsFetchFailed = false;
        } catch (final RetriableException ex) {
            // ignore LEADER_NOT_AVAILABLE error, this can happen when the partition leader is not yet assigned.
            hasLastOffsetsFetchFailed = true;
            lastFailedFetchOffsetsTimestamp = time.milliseconds();
        }
    }

    private void maybeFetchStartAndEndOffsets() {
        // If the leader for a `__remote_log_metadata` partition is not available, then the call to `ListOffsets`
        // will fail after the default timeout of 1 min. Added a delay between the retries to prevent the thread from
        // aggressively fetching the list offsets. During this time, the recently reassigned user-topic-partitions
        // won't be marked as initialized.
        if (hasLastOffsetsFetchFailed && lastFailedFetchOffsetsTimestamp + offsetFetchRetryIntervalMs < time.milliseconds()) {
            fetchStartAndEndOffsets();
        }
    }

    private UserTopicIdPartition newUserTopicIdPartition(final TopicIdPartition tpId) {
        return new UserTopicIdPartition(tpId, topicPartitioner.metadataPartition(tpId));
    }

    private void markInitialized(final UserTopicIdPartition utp) {
        // Silently not initialize the utp
        if (!utp.isAssigned) {
            log.warn("Tried to initialize a UTP: {} that was not yet assigned!", utp);
            return;
        }
        if (!utp.isInitialized) {
            remotePartitionMetadataEventHandler.markInitialized(utp.topicIdPartition);
            utp.isInitialized = true;
        }
    }

    static Set<TopicPartition> toRemoteLogPartitions(final Set<Integer> partitions) {
        return partitions.stream()
            .map(ConsumerTaskMultiThreaded::toRemoteLogPartition)
            .collect(Collectors.toSet());
    }

    static TopicPartition toRemoteLogPartition(int partition) {
        return new TopicPartition(REMOTE_LOG_METADATA_TOPIC_NAME, partition);
    }

    // VisibleForTesting
    void setErrorRetryBackoffMs(int errorRetryBackoffMs) {
        this.errorRetryBackoffMs = errorRetryBackoffMs;
    }

    static class UserTopicIdPartition {
        private final TopicIdPartition topicIdPartition;
        private final Integer metadataPartition;
        // The `utp` will be initialized once it reads all the existing events from the remote log metadata topic.
        boolean isInitialized;
        // denotes whether this `utp` is assigned to the consumer
        boolean isAssigned;

        /**
         * UserTopicIdPartition denotes the user topic-partitions for which this broker acts as a leader/follower of.
         *
         * @param tpId               the unique topic partition identifier
         * @param metadataPartition  the remote log metadata partition mapped for this user-topic-partition.
         */
        public UserTopicIdPartition(final TopicIdPartition tpId, final Integer metadataPartition) {
            this.topicIdPartition = Objects.requireNonNull(tpId);
            this.metadataPartition = Objects.requireNonNull(metadataPartition);
            this.isInitialized = false;
            this.isAssigned = false;
        }

        @Override
        public String toString() {
            return "UserTopicIdPartition{" +
                "topicIdPartition=" + topicIdPartition +
                ", metadataPartition=" + metadataPartition +
                ", isInitialized=" + isInitialized +
                ", isAssigned=" + isAssigned +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UserTopicIdPartition that = (UserTopicIdPartition) o;
            return topicIdPartition.equals(that.topicIdPartition) && metadataPartition.equals(that.metadataPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicIdPartition, metadataPartition);
        }
    }

    static class StartAndEndOffsetHolder {
        Long startOffset;
        Long endOffset;

        public StartAndEndOffsetHolder(Long startOffset, Long endOffset) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        @Override
        public String toString() {
            return "StartAndEndOffsetHolder{" +
                "startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                '}';
        }
    }

    class CatchupConsumer implements Runnable, Closeable {
        final TopicPartition metadataTopicPartition;
        final long endOffset;
        final Consumer<byte[], byte[]> consumer;
        long currentOffset;
        volatile boolean isClosed = false;

        public CatchupConsumer(TopicPartition metadataTopicPartition) {
            this.metadataTopicPartition = metadataTopicPartition;
            int partitionId = metadataTopicPartition.partition();
            this.consumer = consumerSupplier.apply(Optional.of("-" + partitionId));
            Set<TopicPartition> assignment = Collections.singleton(metadataTopicPartition);
            this.consumer.assign(assignment);
            this.consumer.seekToBeginning(assignment);
            this.endOffset = this.consumer.endOffsets(assignment).get(metadataTopicPartition);
            this.currentOffset = this.consumer.beginningOffsets(assignment).get(metadataTopicPartition);
            log.info("Created catch up consumer for metadata topic partition {}", partitionId);
        }

        @Override
        public void run() {
            // Catch up till the end offset that was captured when the runnable was created
            while ((currentOffset + 1) < endOffset && !isClosed) {
                try {
                    pollAndProcessRecords(consumer);
                    currentOffset = readOffsetsByMetadataPartition.getOrDefault(metadataTopicPartition.partition(), currentOffset);
                } catch (final WakeupException ex) {
                    // ignore logging the error
                    break;
                } catch (final RetriableException ex) {
                    log.warn("Retriable error occurred while processing the records for partition {}. Retrying...",
                            metadataTopicPartition.partition(), ex);
                } catch (final Exception ex) {
                    log.error("Error occurred while processing the records for partition {}",
                            metadataTopicPartition.partition(), ex);
                    break;
                }
            }
            // Closing the consumer before closing the thread.
            try {
                consumer.close();
            } catch (final Exception e) {
                log.error("Error encountered while closing the catchup consumer for partition: {}",
                        metadataTopicPartition.partition(), e);
            }
            if ((currentOffset + 1) < endOffset)
                log.info("Catch up consumer finished execution for metadata partition: {}. Status: cancelled. Current offset: {}, End offset: {}",
                        metadataTopicPartition, currentOffset, endOffset);
            else
                log.info("Catch up consumer finished execution for metadata partition: {}. Status: success. Current offset: {}, End offset: {}",
                        metadataTopicPartition, currentOffset, endOffset);
        }

        @Override
        public void close() {
            isClosed = true;
        }
    }

    static class CatchupConsumerInfo {
        final CompletableFuture<?> future;
        final CatchupConsumer catchupConsumer;

        CatchupConsumerInfo(CompletableFuture<?> future, CatchupConsumer catchupConsumer) {
            this.future = future;
            this.catchupConsumer = catchupConsumer;
        }
    }
}
