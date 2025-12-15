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

package org.apache.kafka.controller.metrics;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.fault.FaultHandler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;


/**
 * This publisher translates metadata updates sent by MetadataLoader into changes to controller
 * metrics. Like all MetadataPublisher objects, it only receives notifications about events that
 * have been persisted to the metadata log. So on the active controller, it will run slightly
 * behind the latest in-memory state which has not yet been fully persisted to the log. This is
 * reasonable for metrics, which don't need up-to-the-millisecond update latency.
 *
 * NOTE: the ZK controller has some special rules for calculating preferredReplicaImbalanceCount
 * which we haven't implemented here. Specifically, the ZK controller considers reassigning
 * partitions to always have their preferred leader, even if they don't.
 * All other metrics should be the same, as far as is possible.
 */
public class ControllerMetadataMetricsPublisher implements MetadataPublisher {
    // Throttle URP updates to avoid expensive recalculation on frequent metadata changes
    private static final long URP_UPDATE_THROTTLE_MS = 60_000; // 1 minute

    private final ControllerMetadataMetrics metrics;
    private final FaultHandler faultHandler;
    private MetadataImage prevImage = MetadataImage.EMPTY;
    private long lastUrpUpdateTimeMs = 0;
    // Tracks whether cluster/topic changes require a URP metrics update
    private boolean urpUpdatePending = false;

    public ControllerMetadataMetricsPublisher(
        ControllerMetadataMetrics metrics,
        FaultHandler faultHandler
    ) {
        this.metrics = metrics;
        this.faultHandler = faultHandler;
    }

    @Override
    public String name() {
        return "ControllerMetadataMetricsPublisher";
    }

    @Override
    public void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage,
        LoaderManifest manifest
    ) {
        switch (manifest.type()) {
            case LOG_DELTA:
                try {
                    publishDelta(delta, newImage);
                } catch (Throwable e) {
                    faultHandler.handleFault("Failed to publish controller metrics from log delta " +
                            " ending at offset " + manifest.provenance().lastContainedOffset(), e);
                } finally {
                    prevImage = newImage;
                }
                break;
            case SNAPSHOT:
                try {
                    publishSnapshot(newImage);
                } catch (Throwable e) {
                    faultHandler.handleFault("Failed to publish controller metrics from " +
                            manifest.provenance().snapshotName(), e);
                } finally {
                    prevImage = newImage;
                }
                break;
        }
    }

    private void publishDelta(MetadataDelta delta, MetadataImage newImage) {
        ControllerMetricsChanges changes = new ControllerMetricsChanges();
        if (delta.clusterDelta() != null) {
            for (Entry<Integer, Optional<BrokerRegistration>> entry :
                    delta.clusterDelta().changedBrokers().entrySet()) {
                changes.handleBrokerChange(prevImage.cluster().brokers().get(entry.getKey()),
                        entry.getValue().orElse(null));
            }
        }
        if (delta.topicsDelta() != null) {
            for (Uuid topicId : delta.topicsDelta().deletedTopicIds()) {
                TopicImage prevTopic = prevImage.topics().topicsById().get(topicId);
                if (prevTopic == null) {
                    throw new RuntimeException("Unable to find deleted topic id " + topicId +
                            " in previous topics image.");
                }
                changes.handleDeletedTopic(prevTopic);
            }
            for (Entry<Uuid, TopicDelta> entry : delta.topicsDelta().changedTopics().entrySet()) {
                changes.handleTopicChange(prevImage.topics().getTopic(entry.getKey()), entry.getValue());
            }
        }
        changes.apply(metrics);
        if (delta.featuresDelta() != null) {
            delta.featuresDelta().getZkMigrationStateChange().ifPresent(state -> metrics.setZkMigrationState(state.value()));
        }
        // Only track cluster/topic deltas for URP updates since only these affect replica assignment
        if (delta.clusterDelta() != null || delta.topicsDelta() != null) {
            urpUpdatePending = true;
        }
        // Update per-broker URP metrics based on the new image (throttled).
        // Since URP calculation is expensive (iterates all partitions/replicas), we throttle
        // updates to at most once per minute, even if there are pending changes.
        if (urpUpdatePending) {
            long now = System.currentTimeMillis();
            if (now - lastUrpUpdateTimeMs >= URP_UPDATE_THROTTLE_MS) {
                updateUrpsByBroker(newImage);
                lastUrpUpdateTimeMs = now;
                urpUpdatePending = false;
            }
        }
    }

    private void publishSnapshot(MetadataImage newImage) {
        metrics.setGlobalTopicCount(newImage.topics().topicsById().size());
        int fencedBrokers = 0;
        int activeBrokers = 0;
        int zkBrokers = 0;
        for (BrokerRegistration broker : newImage.cluster().brokers().values()) {
            if (broker.fenced()) {
                fencedBrokers++;
            } else {
                activeBrokers++;
            }
            if (broker.isMigratingZkBroker()) {
                zkBrokers++;
            }
        }
        metrics.setFencedBrokerCount(fencedBrokers);
        metrics.setActiveBrokerCount(activeBrokers);
        metrics.setMigratingZkBrokerCount(zkBrokers);

        int totalPartitions = 0;
        int offlinePartitions = 0;
        int partitionsWithoutPreferredLeader = 0;
        for (TopicImage topicImage : newImage.topics().topicsById().values()) {
            for (PartitionRegistration partition : topicImage.partitions().values()) {
                if (!partition.hasLeader()) {
                    offlinePartitions++;
                }
                if (!partition.hasPreferredLeader()) {
                    partitionsWithoutPreferredLeader++;
                }
                totalPartitions++;
            }
        }
        metrics.setGlobalPartitionCount(totalPartitions);
        metrics.setOfflinePartitionCount(offlinePartitions);
        metrics.setPreferredReplicaImbalanceCount(partitionsWithoutPreferredLeader);
        metrics.setZkMigrationState(newImage.features().zkMigrationState().value());
        updateUrpsByBroker(newImage);
        lastUrpUpdateTimeMs = System.currentTimeMillis();
        urpUpdatePending = false;
    }

    private void updateUrpsByBroker(MetadataImage newImage) {
        // Initialize counts for all unfenced brokers to 0
        Map<Integer, Integer> counts = new HashMap<>();
        for (BrokerRegistration broker : newImage.cluster().brokers().values()) {
            if (!broker.fenced()) {
                counts.put(broker.id(), 0);
            }
        }
        if (counts.isEmpty()) {
            metrics.updateUrpsByBroker(counts);
            return;
        }
        // Iterate all partitions and count URPs for unfenced brokers on online partitions
        for (TopicImage topicImage : newImage.topics().topicsById().values()) {
            for (Entry<Integer, PartitionRegistration> pEntry : topicImage.partitions().entrySet()) {
                PartitionRegistration partition = pEntry.getValue();
                // Ignore partitions without a leader
                if (!partition.hasLeader()) {
                    continue;
                }
                TopicPartition tp = new TopicPartition(topicImage.name(), pEntry.getKey());
                LeaderAndIsrPartitionState state = partition.toLeaderAndIsrPartitionState(tp, false);
                HashSet<Integer> isrSet = new java.util.HashSet<>(state.isr());
                for (Integer replica : state.replicas()) {
                    if (!isrSet.contains(replica)) {
                        counts.merge(replica, 1, Integer::sum);
                    }
                }
            }
        }
        metrics.updateUrpsByBroker(counts);
    }

    // Visible for testing
    void resetUrpUpdateThrottle() {
        lastUrpUpdateTimeMs = 0;
    }

    @Override
    public void close() {
        metrics.close();
    }
}
