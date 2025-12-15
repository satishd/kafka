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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.DelegationTokenImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.image.writer.ImageReWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.controller.metrics.ControllerMetadataMetrics.TAG_BROKER_ID;
import static org.apache.kafka.controller.metrics.ControllerMetadataMetrics.URPS_CAUSED_BY_BROKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ControllerMetadataMetricsPublisherUrpTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshotPublishesUrpsAndIgnoresFenced() {
        MetricsRegistry registry = new MetricsRegistry();
        MockFaultHandler faultHandler = new MockFaultHandler("ControllerMetadataMetricsPublisherUrpTest");
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            ControllerMetadataMetricsPublisher publisher = new ControllerMetadataMetricsPublisher(metrics, faultHandler);

            MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
            ImageReWriter writer = new ImageReWriter(delta);
            // 4 brokers and broker id 4 is fenced and has no URPs
            Map<Integer, BrokerRegistration> brokers = new HashMap<>();
            brokers.put(1, broker(1, false));
            brokers.put(2, broker(2, false));
            brokers.put(3, broker(3, false));
            // broker 4 exists but fenced: should not be included in counts
            brokers.put(4, broker(4, true));
            MetadataImage newImage = image(new ClusterImage(brokers, Collections.emptyMap()), topicsWithUrps());
            newImage.write(writer, new ImageWriterOptions.Builder()
                    .setMetadataVersion(delta.image().features().metadataVersion())
                    .build());

            publisher.onMetadataUpdate(delta, newImage, snapshotManifest());

            Gauge<Integer> g1 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(1));
            Gauge<Integer> g2 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(2));
            Gauge<Integer> g3 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(3));
            Gauge<Integer> g4 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(4));

            assertNotNull(g1);
            assertNotNull(g2);
            assertNotNull(g3);
            assertNull(g4, "fenced broker should not have a gauge");

            // From partitions we created, broker 3 is out of ISR for 2 partitions
            assertEquals(0, g1.value());
            assertEquals(0, g2.value());
            assertEquals(2, g3.value());
        } finally {
            registry.shutdown();
            faultHandler.maybeRethrowFirstException();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeltaUpdatesUrpsWhenIsrHealsAndNoBrokerChanges() {
        MetricsRegistry registry = new MetricsRegistry();
        MockFaultHandler faultHandler = new MockFaultHandler("ControllerMetadataMetricsPublisherUrpTest");
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            ControllerMetadataMetricsPublisher publisher = new ControllerMetadataMetricsPublisher(metrics, faultHandler);

            // Snapshot with URPs present
            MetadataDelta delta1 = new MetadataDelta(MetadataImage.EMPTY);
            ImageReWriter writer1 = new ImageReWriter(delta1);
            MetadataImage img1 = image(clusterWith3Brokers(), topicsWithUrps());
            img1.write(writer1, new ImageWriterOptions.Builder()
                .setMetadataVersion(delta1.image().features().metadataVersion())
                .build());
            publisher.onMetadataUpdate(delta1, img1, snapshotManifest());

            Gauge<Integer> g3 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(3));
            assertNotNull(g3);
            assertEquals(2, g3.value());

            // Reset throttle to allow immediate delta update
            publisher.resetUrpUpdateThrottle();

            // Delta that heals ISR (no brokers change)
            MetadataDelta delta2 = new MetadataDelta(img1);
            ImageReWriter writer2 = new ImageReWriter(delta2);
            MetadataImage img2 = image(clusterWith3Brokers(), topicsHealed());
            img2.write(writer2, new ImageWriterOptions.Builder()
                .setMetadataVersion(delta2.image().features().metadataVersion())
                .build());
            publisher.onMetadataUpdate(delta2, img2, logDeltaManifest());

            g3 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(3));
            assertNotNull(g3);
            assertEquals(0, g3.value());

            // Brokers 1 and 2 still have gauges
            assertNotNull(registry.allMetrics().get(urpsMetricNameForBroker(1)));
            assertNotNull(registry.allMetrics().get(urpsMetricNameForBroker(2)));
        } finally {
            registry.shutdown();
            faultHandler.maybeRethrowFirstException();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLeaderlessPartitionsAreIgnored() {
        MetricsRegistry registry = new MetricsRegistry();
        MockFaultHandler faultHandler = new MockFaultHandler("ControllerMetadataMetricsPublisherUrpTest");
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            ControllerMetadataMetricsPublisher publisher = new ControllerMetadataMetricsPublisher(metrics, faultHandler);

            MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
            ImageReWriter writer = new ImageReWriter(delta);
            MetadataImage img = image(clusterWith3Brokers(), topicsWithoutLeaderOnly());
            img.write(writer, new ImageWriterOptions.Builder()
                .setMetadataVersion(delta.image().features().metadataVersion())
                .build());

            publisher.onMetadataUpdate(delta, img, snapshotManifest());

            // All brokers exist and are unfenced but no URPs should be counted
            Gauge<Integer> g1 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(1));
            Gauge<Integer> g2 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(2));
            Gauge<Integer> g3 = (Gauge<Integer>) registry.allMetrics().get(urpsMetricNameForBroker(3));
            assertNotNull(g1);
            assertNotNull(g2);
            assertNotNull(g3);
            assertEquals(0, g1.value());
            assertEquals(0, g2.value());
            assertEquals(0, g3.value());
        } finally {
            registry.shutdown();
            faultHandler.maybeRethrowFirstException();
        }
    }

    @Test
    public void testEmptyClusterProducesNoUrpGauges() {
        MetricsRegistry registry = new MetricsRegistry();
        MockFaultHandler faultHandler = new MockFaultHandler("ControllerMetadataMetricsPublisherUrpTest");
        try (ControllerMetadataMetrics metrics = new ControllerMetadataMetrics(Optional.of(registry))) {
            ControllerMetadataMetricsPublisher publisher = new ControllerMetadataMetricsPublisher(metrics, faultHandler);

            // No brokers at all
            Map<Integer, BrokerRegistration> brokers = new HashMap<>();
            ClusterImage cluster = new ClusterImage(brokers, Collections.emptyMap());
            MetadataDelta delta = new MetadataDelta(MetadataImage.EMPTY);
            ImageReWriter writer = new ImageReWriter(delta);
            MetadataImage img = image(cluster, topicsWithoutLeaderOnly());
            img.write(writer, new ImageWriterOptions.Builder()
                .setMetadataVersion(delta.image().features().metadataVersion())
                .build());

            publisher.onMetadataUpdate(delta, img, snapshotManifest());

            // No gauges should be registered
            assertNull(registry.allMetrics().get(urpsMetricNameForBroker(1)));
            assertNull(registry.allMetrics().get(urpsMetricNameForBroker(2)));
            assertNull(registry.allMetrics().get(urpsMetricNameForBroker(3)));
        } finally {
            registry.shutdown();
            faultHandler.maybeRethrowFirstException();
        }
    }

    private static MetricName urpsMetricNameForBroker(int brokerId) {
        LinkedHashMap<String, String> tags = new LinkedHashMap<>();
        tags.put(TAG_BROKER_ID, Integer.toString(brokerId));
        return KafkaYammerMetrics.getMetricName(
                "kafka.controller",
                "KafkaController",
                URPS_CAUSED_BY_BROKER,
                tags
        );
    }

    private static LoaderManifest snapshotManifest() {
        return new SnapshotManifest(MetadataProvenance.EMPTY, 0);
    }

    private static LoaderManifest logDeltaManifest() {
        return LogDeltaManifest.newBuilder()
                .provenance(MetadataProvenance.EMPTY)
                .leaderAndEpoch(org.apache.kafka.raft.LeaderAndEpoch.UNKNOWN)
                .numBatches(0)
                .elapsedNs(0)
                .numBytes(0).build();
    }

    private static BrokerRegistration broker(int id, boolean fenced) {
        return new BrokerRegistration.Builder()
                .setId(id)
                .setEpoch(1L)
                .setIncarnationId(Uuid.randomUuid())
                .setListeners(Collections.emptyList())
                .setSupportedFeatures(Collections.emptyMap())
                .setRack(Optional.empty())
                .setPod(Optional.empty())
                .setFenced(fenced)
                .setInControlledShutdown(false)
                .setIsMigratingZkBroker(false)
                .setDirectories(Collections.emptyList())
                .build();
    }

    private static PartitionRegistration pr(int[] replicas, int[] isr, int leader) {
        return new PartitionRegistration.Builder()
                .setReplicas(replicas)
                .setDirectories(org.apache.kafka.common.DirectoryId.migratingArray(replicas.length))
                .setIsr(isr)
                .setLeader(leader)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                .setLeaderEpoch(1)
                .setPartitionEpoch(1)
                .build();
    }

    private static TopicsImage topicsWithUrps() {
        Map<Integer, PartitionRegistration> partitions = new HashMap<>();
        // p0: leader 1, replicas [1,2,3], isr [1,2] => broker 3 has URP
        partitions.put(0, pr(new int[]{1, 2, 3}, new int[]{1, 2}, 1));
        // p1: leader 1, replicas [1,3], isr [1] => broker 3 has URP
        partitions.put(1, pr(new int[]{1, 3}, new int[]{1}, 1));
        // p2: no leader => ignored
        partitions.put(2, pr(new int[]{1, 2}, new int[]{1, 2}, -1));
        TopicImage t = new TopicImage("t", Uuid.randomUuid(), partitions);
        return TopicsImage.EMPTY.including(t);
    }

    private static TopicsImage topicsHealed() {
        Map<Integer, PartitionRegistration> partitions = new HashMap<>();
        // same replicas, ISR equals replicas -> no URPs
        partitions.put(0, pr(new int[]{1, 2, 3}, new int[]{1, 2, 3}, 1));
        partitions.put(1, pr(new int[]{1, 3}, new int[]{1, 3}, 1));
        // leaderless remains ignored
        partitions.put(2, pr(new int[]{1, 2}, new int[]{1, 2}, -1));
        TopicImage t = new TopicImage("t", Uuid.randomUuid(), partitions);
        return TopicsImage.EMPTY.including(t);
    }

    private static TopicsImage topicsWithoutLeaderOnly() {
        Map<Integer, PartitionRegistration> partitions = new HashMap<>();
        partitions.put(0, pr(new int[]{1, 2, 3}, new int[]{1, 2, 3}, -1));
        TopicImage t = new TopicImage("t", Uuid.randomUuid(), partitions);
        return TopicsImage.EMPTY.including(t);
    }

    private static ClusterImage clusterWith3Brokers() {
        Map<Integer, BrokerRegistration> brokers = new HashMap<>();
        brokers.put(1, broker(1, false));
        brokers.put(2, broker(2, false));
        brokers.put(3, broker(3, false));
        return new ClusterImage(brokers, Collections.emptyMap());
    }

    private static MetadataImage image(ClusterImage cluster, TopicsImage topics) {
        return new MetadataImage(
                MetadataProvenance.EMPTY,
                FeaturesImage.EMPTY,
                cluster,
                topics,
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY);
    }
}
