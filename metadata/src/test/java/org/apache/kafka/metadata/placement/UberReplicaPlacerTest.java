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
package org.apache.kafka.metadata.placement;

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UberReplicaPlacerTest {

    @Test
    public void testNewReplicaExcludeListEmpty() {
        ReplicaPlacer mockReplicaPlacer = (placement, cluster) -> null;
        Map<String, String> clusterConfig = Collections.emptyMap();

        UberReplicaPlacer replicaPlacer = new UberReplicaPlacer(mockReplicaPlacer, () -> clusterConfig);
        Set<Integer> excludedBrokers = replicaPlacer.newReplicaExcludeList();
        assertTrue(excludedBrokers.isEmpty());
    }

    @Test
    public void testNewReplicaExcludeListNonEmpty() {
        ReplicaPlacer mockReplicaPlacer = (placement, cluster) -> null;
        Map<String, String> clusterConfig = Collections.singletonMap(ServerLogConfigs.NEW_REPLICA_EXCLUDE_LIST_CONFIG, "0:1");

        UberReplicaPlacer replicaPlacer = new UberReplicaPlacer(mockReplicaPlacer, () -> clusterConfig);
        Set<Integer> excludedBrokers = replicaPlacer.newReplicaExcludeList();
        Set<Integer> expected = new HashSet<>(Arrays.asList(0, 1));
        assertEquals(2, excludedBrokers.size());
        assertEquals(expected, excludedBrokers);
    }

    @Test
    public void testPlacementWithExcludedBroker() {
        List<UsableBroker> brokers = IntStream.range(0, 3)
            .mapToObj(id -> new UsableBroker(id, Optional.empty(), false))
            .collect(Collectors.toList());

        ReplicaPlacer stripedReplicaPlacer = new StripedReplicaPlacer(new Random());
        int excludedBrokerId = 1;
        Map<String, String> clusterConfig = Collections.singletonMap(
            ServerLogConfigs.NEW_REPLICA_EXCLUDE_LIST_CONFIG, Integer.toString(excludedBrokerId));

        UberReplicaPlacer placer = new UberReplicaPlacer(stripedReplicaPlacer, () -> clusterConfig);
        TopicAssignment assignment = place(placer, 0, 1, (short) 2, brokers);
        List<Integer> resultReplicas = assignment.assignments().get(0).replicas();

        // Verify exclusion worked
        assertFalse(resultReplicas.contains(excludedBrokerId),
            "Excluded broker " + excludedBrokerId + " should not be in result: " + resultReplicas);
        assertEquals(2, resultReplicas.size(), "Should have exactly 2 replicas");

        // Verify only non-excluded brokers are used
        Set<Integer> expectedBrokers = new HashSet<>(Arrays.asList(0, 2));
        assertTrue(expectedBrokers.containsAll(resultReplicas),
            "Result should only contain non-excluded brokers");
    }

    @Test
    public void testPlacementWithMultipleExcludedBrokers() {
        List<UsableBroker> brokers = IntStream.range(0, 5)
            .mapToObj(id -> new UsableBroker(id, Optional.empty(), false))
            .collect(Collectors.toList());

        ReplicaPlacer stripedReplicaPlacer = new StripedReplicaPlacer(new Random());
        Map<String, String> clusterConfig = Collections.singletonMap(
            ServerLogConfigs.NEW_REPLICA_EXCLUDE_LIST_CONFIG, "1:3");

        UberReplicaPlacer placer = new UberReplicaPlacer(stripedReplicaPlacer, () -> clusterConfig);
        TopicAssignment assignment = place(placer, 0, 1, (short) 3, brokers);
        List<Integer> resultReplicas = assignment.assignments().get(0).replicas();

        // Verify no excluded brokers are present
        for (int excluded : Arrays.asList(1, 3)) {
            assertFalse(resultReplicas.contains(excluded),
                "Excluded broker " + excluded + " found in result: " + resultReplicas);
        }
        assertEquals(3, resultReplicas.size());

        // Verify only allowed brokers are used
        Set<Integer> allowedBrokers = new HashSet<>(Arrays.asList(0, 2, 4));
        assertTrue(allowedBrokers.containsAll(resultReplicas));
    }

    @Test
    public void testPlacementWithInsufficientNonExcludedBrokers() {
        // Setup: 3 brokers, exclude 2, try to place 2 replicas
        List<UsableBroker> brokers = IntStream.range(0, 3)
            .mapToObj(id -> new UsableBroker(id, Optional.empty(), false))
            .collect(Collectors.toList());

        ReplicaPlacer stripedReplicaPlacer = new StripedReplicaPlacer(new Random());
        Map<String, String> clusterConfig = Collections.singletonMap(
            ServerLogConfigs.NEW_REPLICA_EXCLUDE_LIST_CONFIG, "1:2");

        UberReplicaPlacer placer = new UberReplicaPlacer(stripedReplicaPlacer, () -> clusterConfig);
        assertThrows(InvalidReplicationFactorException.class, () -> place(placer, 0, 1, (short) 3, brokers));
    }

    private TopicAssignment place(ReplicaPlacer placer,
                                  int startPartition,
                                  int numPartitions,
                                  short replicationFactor,
                                  List<UsableBroker> brokers) {
        PlacementSpec placementSpec = new PlacementSpec(startPartition, numPartitions, replicationFactor);
        return placer.place(placementSpec, new ClusterDescriber() {
            @Override
            public Iterator<UsableBroker> usableBrokers() {
                return brokers.iterator();
            }

            @Override
            public Uuid defaultDir(int brokerId) {
                return DirectoryId.MIGRATING;
            }
        });
    }
}
