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
package org.apache.kafka.tools.reassign;

import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.apache.kafka.server.common.AdminOperationException;
import org.apache.kafka.server.config.QuotaConfigs;
import org.apache.kafka.tools.TerseException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Constructor;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.alterPartitionReassignments;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.alterReplicaLogDirs;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.calculateFollowerThrottles;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.calculateLeaderThrottles;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.calculateMovingBrokers;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.calculateProposedMoveMap;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.calculateReassigningBrokers;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.cancelPartitionReassignments;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.compareTopicPartitionReplicas;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.compareTopicPartitions;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.curReassignmentsToString;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.currentPartitionReplicaAssignmentToString;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.executeAssignment;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.executePartitionReassignmentsIncrementally;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.findLogDirMoveStates;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.findPartitionReassignmentStates;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.generateAssignment;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.getBrokerMetadata;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.getReplicaAssignmentForPartitions;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.getReplicaAssignmentForTopics;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.modifyInterBrokerThrottle;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.modifyLogDirThrottle;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.modifyTopicThrottles;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.parseExecuteAssignmentArgs;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.parseGenerateAssignmentArgs;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.partitionProposedReassignmentsIntoBatches;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.partitionReassignmentStatesToString;
import static org.apache.kafka.tools.reassign.ReassignPartitionsCommand.replicaMoveStatesToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(60)
public class ReassignPartitionsUnitTest {
    /** Must match {@code BATCH_REASSIGNMENT_POLL_INTERVAL_MS} in {@link ReassignPartitionsCommand}. */
    private static final long PARTITION_REASSIGNMENT_WAIT_POLL_MS = 500L;

    @BeforeAll
    public static void setUp() {
        Exit.setExitProcedure((statusCode, message) -> {
            throw new IllegalArgumentException(message);
        });
    }

    @AfterAll
    public static void tearDown() {
        Exit.resetExitProcedure();
    }

    /** Recorded alter batches sort keys with {@link ReassignPartitionsCommand#compareTopicPartitions} (admin maps are unordered). */
    private static List<TopicPartition> sortedTopicPartitionsForRecording(Collection<TopicPartition> keys) {
        List<TopicPartition> list = new ArrayList<>(keys);
        list.sort(ReassignPartitionsCommand::compareTopicPartitions);
        return list;
    }

    /** {@link AlterPartitionReassignmentsResult} has a package-private constructor; tests outside {@code clients.admin} use reflection. */
    private static AlterPartitionReassignmentsResult newAlterPartitionReassignmentsResult(
            Map<TopicPartition, KafkaFuture<Void>> futures) {
        try {
            Constructor<AlterPartitionReassignmentsResult> ctor =
                AlterPartitionReassignmentsResult.class.getDeclaredConstructor(Map.class);
            ctor.setAccessible(true);
            return ctor.newInstance(futures);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCompareTopicPartitions() {
        assertTrue(compareTopicPartitions(new TopicPartition("abc", 0),
            new TopicPartition("abc", 1)) < 0);
        assertFalse(compareTopicPartitions(new TopicPartition("def", 0),
            new TopicPartition("abc", 1)) < 0);
    }

    @Test
    public void testCompareTopicPartitionReplicas() {
        assertTrue(compareTopicPartitionReplicas(new TopicPartitionReplica("def", 0, 0),
            new TopicPartitionReplica("abc", 0, 1)) < 0);
        assertFalse(compareTopicPartitionReplicas(new TopicPartitionReplica("def", 0, 0),
            new TopicPartitionReplica("cde", 0, 0)) < 0);
    }

    @Test
    public void testPartitionReassignStatesToString() {
        Map<TopicPartition, PartitionReassignmentState> states = new HashMap<>();

        states.put(new TopicPartition("foo", 0),
            new PartitionReassignmentState(asList(1, 2, 3), asList(1, 2, 3), true));
        states.put(new TopicPartition("foo", 1),
            new PartitionReassignmentState(asList(1, 2, 3), asList(1, 2, 4), false));
        states.put(new TopicPartition("bar", 0),
            new PartitionReassignmentState(asList(1, 2, 3), asList(1, 2, 4), false));

        assertEquals(String.join(System.lineSeparator(),
            "Status of partition reassignment:",
            "Reassignment of partition bar-0 is still in progress.",
            "Reassignment of partition foo-0 is completed.",
            "Reassignment of partition foo-1 is still in progress."),
            partitionReassignmentStatesToString(states));
    }

    private void addTopics(MockAdminClient adminClient) {
        List<Node> b = adminClient.brokers();
        adminClient.addTopic(false, "foo", asList(
            new TopicPartitionInfo(0, b.get(0),
                asList(b.get(0), b.get(1), b.get(2)),
                asList(b.get(0), b.get(1))),
            new TopicPartitionInfo(1, b.get(1),
                asList(b.get(1), b.get(2), b.get(3)),
                asList(b.get(1), b.get(2), b.get(3)))
        ), Collections.emptyMap());
        adminClient.addTopic(false, "bar", asList(
            new TopicPartitionInfo(0, b.get(2),
                asList(b.get(2), b.get(3), b.get(0)),
                asList(b.get(2), b.get(3), b.get(0)))
        ), Collections.emptyMap());
    }

    @Test
    public void testFindPartitionReassignmentStates() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            // Create a reassignment and test findPartitionReassignmentStates.
            Map<TopicPartition, List<Integer>> reassignments = new HashMap<>();

            reassignments.put(new TopicPartition("foo", 0), asList(0, 1, 3));
            reassignments.put(new TopicPartition("quux", 0), asList(1, 2, 3));

            Map<TopicPartition, Throwable> reassignmentResult = alterPartitionReassignments(adminClient, reassignments);

            assertEquals(1, reassignmentResult.size());
            assertEquals(UnknownTopicOrPartitionException.class, reassignmentResult.get(new TopicPartition("quux", 0)).getClass());

            Map<TopicPartition, PartitionReassignmentState> expStates = new HashMap<>();

            expStates.put(new TopicPartition("foo", 0),
                new PartitionReassignmentState(asList(0, 1, 2), asList(0, 1, 3), false));
            expStates.put(new TopicPartition("foo", 1),
                new PartitionReassignmentState(asList(1, 2, 3), asList(1, 2, 3), true));

            Entry<Map<TopicPartition, PartitionReassignmentState>, Boolean> actual =
                findPartitionReassignmentStates(adminClient, asList(
                    new SimpleImmutableEntry<>(new TopicPartition("foo", 0), asList(0, 1, 3)),
                    new SimpleImmutableEntry<>(new TopicPartition("foo", 1), asList(1, 2, 3))
                ));

            assertEquals(expStates, actual.getKey());
            assertTrue(actual.getValue());

            // Cancel the reassignment and test findPartitionReassignmentStates again.
            Map<TopicPartition, Throwable> cancelResult = cancelPartitionReassignments(adminClient,
                new HashSet<>(asList(new TopicPartition("foo", 0), new TopicPartition("quux", 2))));

            assertEquals(1, cancelResult.size());
            assertEquals(UnknownTopicOrPartitionException.class, cancelResult.get(new TopicPartition("quux", 2)).getClass());

            expStates.clear();

            expStates.put(new TopicPartition("foo", 0),
                new PartitionReassignmentState(asList(0, 1, 2), asList(0, 1, 3), true));
            expStates.put(new TopicPartition("foo", 1),
                new PartitionReassignmentState(asList(1, 2, 3), asList(1, 2, 3), true));

            actual = findPartitionReassignmentStates(adminClient, asList(
                new SimpleImmutableEntry<>(new TopicPartition("foo", 0), asList(0, 1, 3)),
                new SimpleImmutableEntry<>(new TopicPartition("foo", 1), asList(1, 2, 3))
            ));

            assertEquals(expStates, actual.getKey());
            assertFalse(actual.getValue());
        }
    }

    @Test
    public void testFindLogDirMoveStates() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
                numBrokers(4).
                brokerLogDirs(asList(
                    asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
                    asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
                    asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"),
                    asList("/tmp/kafka-logs0", null)))
                .build()) {

            addTopics(adminClient);
            List<Node> b = adminClient.brokers();
            adminClient.addTopic(false, "quux", asList(
                    new TopicPartitionInfo(0, b.get(2),
                        asList(b.get(1), b.get(2), b.get(3)),
                        asList(b.get(1), b.get(2), b.get(3)))),
                Collections.emptyMap());

            Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>();

            replicaAssignment.put(new TopicPartitionReplica("foo", 0, 0), "/tmp/kafka-logs1");
            replicaAssignment.put(new TopicPartitionReplica("quux", 0, 0), "/tmp/kafka-logs1");

            adminClient.alterReplicaLogDirs(replicaAssignment).all().get();

            Map<TopicPartitionReplica, LogDirMoveState> states = new HashMap<>();

            states.put(new TopicPartitionReplica("bar", 0, 0), new CompletedMoveState("/tmp/kafka-logs0"));
            states.put(new TopicPartitionReplica("foo", 0, 0), new ActiveMoveState("/tmp/kafka-logs0",
                "/tmp/kafka-logs1", "/tmp/kafka-logs1"));
            states.put(new TopicPartitionReplica("foo", 1, 0), new CancelledMoveState("/tmp/kafka-logs0",
                "/tmp/kafka-logs1"));
            states.put(new TopicPartitionReplica("quux", 1, 0), new MissingLogDirMoveState("/tmp/kafka-logs1"));
            states.put(new TopicPartitionReplica("quuz", 0, 0), new MissingReplicaMoveState("/tmp/kafka-logs0"));

            Map<TopicPartitionReplica, String> targetMoves = new HashMap<>();

            targetMoves.put(new TopicPartitionReplica("bar", 0, 0), "/tmp/kafka-logs0");
            targetMoves.put(new TopicPartitionReplica("foo", 0, 0), "/tmp/kafka-logs1");
            targetMoves.put(new TopicPartitionReplica("foo", 1, 0), "/tmp/kafka-logs1");
            targetMoves.put(new TopicPartitionReplica("quux", 1, 0), "/tmp/kafka-logs1");
            targetMoves.put(new TopicPartitionReplica("quuz", 0, 0), "/tmp/kafka-logs0");

            assertEquals(states, findLogDirMoveStates(adminClient, targetMoves));
        }
    }

    @Test
    public void testReplicaMoveStatesToString() {
        Map<TopicPartitionReplica, LogDirMoveState> states = new HashMap<>();

        states.put(new TopicPartitionReplica("bar", 0, 0), new CompletedMoveState("/tmp/kafka-logs0"));
        states.put(new TopicPartitionReplica("foo", 0, 0), new ActiveMoveState("/tmp/kafka-logs0",
            "/tmp/kafka-logs1", "/tmp/kafka-logs1"));
        states.put(new TopicPartitionReplica("foo", 1, 0), new CancelledMoveState("/tmp/kafka-logs0",
            "/tmp/kafka-logs1"));
        states.put(new TopicPartitionReplica("quux", 0, 0), new MissingReplicaMoveState("/tmp/kafka-logs1"));
        states.put(new TopicPartitionReplica("quux", 1, 1), new ActiveMoveState("/tmp/kafka-logs0",
            "/tmp/kafka-logs1", "/tmp/kafka-logs2"));
        states.put(new TopicPartitionReplica("quux", 2, 1), new MissingLogDirMoveState("/tmp/kafka-logs1"));

        assertEquals(String.join(System.lineSeparator(),
            "Reassignment of replica bar-0-0 completed successfully.",
            "Reassignment of replica foo-0-0 is still in progress.",
            "Partition foo-1 on broker 0 is not being moved from log dir /tmp/kafka-logs0 to /tmp/kafka-logs1.",
            "Partition quux-0 cannot be found in any live log directory on broker 0.",
            "Partition quux-1 on broker 1 is being moved to log dir /tmp/kafka-logs2 instead of /tmp/kafka-logs1.",
            "Partition quux-2 is not found in any live log dir on broker 1. " +
                "There is likely an offline log directory on the broker."),
            replicaMoveStatesToString(states));
    }

    @Test
    public void testGetReplicaAssignments() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);

            Map<TopicPartition, List<Integer>> assignments = new HashMap<>();

            assignments.put(new TopicPartition("foo", 0), asList(0, 1, 2));
            assignments.put(new TopicPartition("foo", 1), asList(1, 2, 3));

            assertEquals(assignments, getReplicaAssignmentForTopics(adminClient, asList("foo")));

            assignments.clear();

            assignments.put(new TopicPartition("foo", 0), asList(0, 1, 2));
            assignments.put(new TopicPartition("bar", 0), asList(2, 3, 0));

            assertEquals(assignments,
                getReplicaAssignmentForPartitions(adminClient, new HashSet<>(asList(new TopicPartition("foo", 0), new TopicPartition("bar", 0)))));

            UnknownTopicOrPartitionException exception =
                assertInstanceOf(UnknownTopicOrPartitionException.class,
                    assertThrows(ExecutionException.class,
                        () -> getReplicaAssignmentForPartitions(adminClient,
                            new HashSet<>(asList(new TopicPartition("foo", 0), new TopicPartition("foo", 10))))).getCause());
            assertEquals("Unable to find partition: foo-10", exception.getMessage());
        }
    }

    @Test
    public void testGetBrokerRackInformation() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
            brokers(asList(new Node(0, "localhost", 9092, "rack0", "pod0"),
                new Node(1, "localhost", 9093, "rack1", "pod1"),
                new Node(2, "localhost", 9094, null))).
            build()) {

            assertEquals(asList(
                new BrokerMetadata(0, Optional.of("rack0"), Optional.of("pod0")),
                new BrokerMetadata(1, Optional.of("rack1"), Optional.of("pod1"))
            ), getBrokerMetadata(adminClient, asList(0, 1), true));
            assertEquals(asList(
                new BrokerMetadata(0, Optional.empty(), Optional.empty()),
                new BrokerMetadata(1, Optional.empty(), Optional.empty())
            ), getBrokerMetadata(adminClient, asList(0, 1), false));
            assertStartsWith("Not all brokers have rack information",
                assertThrows(AdminOperationException.class,
                    () -> getBrokerMetadata(adminClient, asList(1, 2), true)).getMessage());
            assertEquals(asList(
                new BrokerMetadata(1, Optional.empty(), Optional.empty()),
                new BrokerMetadata(2, Optional.empty(), Optional.empty())
            ), getBrokerMetadata(adminClient, asList(1, 2), false));
        }
    }

    @Test
    public void testParseGenerateAssignmentArgs() throws Exception {
        assertStartsWith("Broker list contains duplicate entries",
            assertThrows(AdminCommandFailedException.class, () -> parseGenerateAssignmentArgs(
                "{\"topics\": [{\"topic\": \"foo\"}], \"version\":1}", "1,1,2"),
                "Expected to detect duplicate broker list entries").getMessage());
        assertStartsWith("Broker list contains duplicate entries",
            assertThrows(AdminCommandFailedException.class, () -> parseGenerateAssignmentArgs(
                "{\"topics\": [{\"topic\": \"foo\"}], \"version\":1}", "5,2,3,4,5"),
                "Expected to detect duplicate broker list entries").getMessage());
        assertEquals(new SimpleImmutableEntry<>(asList(5, 2, 3, 4), asList("foo")),
            parseGenerateAssignmentArgs("{\"topics\": [{\"topic\": \"foo\"}], \"version\":1}", "5,2,3,4"));
        assertStartsWith("List of topics to reassign contains duplicate entries",
            assertThrows(AdminCommandFailedException.class, () -> parseGenerateAssignmentArgs(
                "{\"topics\": [{\"topic\": \"foo\"},{\"topic\": \"foo\"}], \"version\":1}", "5,2,3,4"),
                "Expected to detect duplicate topic entries").getMessage());
        assertEquals(new SimpleImmutableEntry<>(asList(5, 3, 4), asList("foo", "bar")),
            parseGenerateAssignmentArgs(
                "{\"topics\": [{\"topic\": \"foo\"},{\"topic\": \"bar\"}], \"version\":1}", "5,3,4"));
    }

    @Test
    public void testGenerateAssignmentFailsWithoutEnoughReplicas() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            assertStartsWith("Replication factor: 3 larger than available brokers: 2",
                assertThrows(InvalidReplicationFactorException.class,
                    () -> generateAssignment(adminClient, "{\"topics\":[{\"topic\":\"foo\"},{\"topic\":\"bar\"}]}", "0,1", false),
                    "Expected generateAssignment to fail").getMessage());
        }
    }

    @Test
    public void testGenerateAssignmentWithInvalidPartitionsFails() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(5).build()) {
            addTopics(adminClient);
            assertStartsWith("Topic quux not found",
                assertThrows(ExecutionException.class,
                    () -> generateAssignment(adminClient, "{\"topics\":[{\"topic\":\"foo\"},{\"topic\":\"quux\"}]}", "0,1", false),
                    "Expected generateAssignment to fail").getCause().getMessage());
        }
    }

    @Test
    public void testGenerateAssignmentWithInconsistentRacks() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
            brokers(asList(
                new Node(0, "localhost", 9092, "rack0"),
                new Node(1, "localhost", 9093, "rack0"),
                new Node(2, "localhost", 9094, null),
                new Node(3, "localhost", 9095, "rack1"),
                new Node(4, "localhost", 9096, "rack1"),
                new Node(5, "localhost", 9097, "rack2"))).
            build()) {

            addTopics(adminClient);
            assertStartsWith("Not all brokers have rack information.",
                assertThrows(AdminOperationException.class,
                    () -> generateAssignment(adminClient, "{\"topics\":[{\"topic\":\"foo\"}]}", "0,1,2,3", true),
                    "Expected generateAssignment to fail").getMessage());
            // It should succeed when --disable-rack-aware is used.
            Entry<Map<TopicPartition, List<Integer>>, Map<TopicPartition, List<Integer>>>
                proposedCurrent = generateAssignment(adminClient, "{\"topics\":[{\"topic\":\"foo\"}]}", "0,1,2,3", false);

            Map<TopicPartition, List<Integer>> expCurrent = new HashMap<>();

            expCurrent.put(new TopicPartition("foo", 0), asList(0, 1, 2));
            expCurrent.put(new TopicPartition("foo", 1), asList(1, 2, 3));

            assertEquals(expCurrent, proposedCurrent.getValue());
        }
    }

    @Test
    public void testGenerateAssignmentWithFewerBrokers() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            List<Integer> goalBrokers = asList(0, 1, 3);

            Entry<Map<TopicPartition, List<Integer>>, Map<TopicPartition, List<Integer>>>
                proposedCurrent = generateAssignment(adminClient,
                    "{\"topics\":[{\"topic\":\"foo\"},{\"topic\":\"bar\"}]}",
                    goalBrokers.stream().map(Object::toString).collect(Collectors.joining(",")), false);

            Map<TopicPartition, List<Integer>> expCurrent = new HashMap<>();

            expCurrent.put(new TopicPartition("foo", 0), asList(0, 1, 2));
            expCurrent.put(new TopicPartition("foo", 1), asList(1, 2, 3));
            expCurrent.put(new TopicPartition("bar", 0), asList(2, 3, 0));

            assertEquals(expCurrent, proposedCurrent.getValue());

            // The proposed assignment should only span the provided brokers
            proposedCurrent.getKey().values().forEach(replicas ->
                assertTrue(goalBrokers.containsAll(replicas),
                    "Proposed assignment " + proposedCurrent.getKey() + " puts replicas on brokers other than " + goalBrokers)
            );
        }
    }

    @Test
    public void testCurrentPartitionReplicaAssignmentToString() throws Exception {
        Map<TopicPartition, List<Integer>> proposedParts = new HashMap<>();

        proposedParts.put(new TopicPartition("foo", 1), asList(1, 2, 3));
        proposedParts.put(new TopicPartition("bar", 0), asList(7, 8, 9));

        Map<TopicPartition, List<Integer>> currentParts = new HashMap<>();

        currentParts.put(new TopicPartition("foo", 0), asList(1, 2, 3));
        currentParts.put(new TopicPartition("foo", 1), asList(4, 5, 6));
        currentParts.put(new TopicPartition("bar", 0), asList(7, 8));
        currentParts.put(new TopicPartition("baz", 0), asList(10, 11, 12));

        assertEquals(String.join(System.lineSeparator(),
            "Current partition replica assignment",
            "",
            "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"bar\",\"partition\":0,\"replicas\":[7,8],\"log_dirs\":[\"any\",\"any\"]}," +
                "{\"topic\":\"foo\",\"partition\":1,\"replicas\":[4,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]" +
                "}",
            "",
            "Save this to use as the --reassignment-json-file option during rollback"),
            currentPartitionReplicaAssignmentToString(proposedParts, currentParts)
        );
    }

    @Test
    public void testMoveMap() {
        // overwrite foo-0 with different reassignments
        // keep old reassignments of foo-1
        // overwrite foo-2 with same reassignments
        // overwrite foo-3 with new reassignments without overlap of old reassignments
        // overwrite foo-4 with a subset of old reassignments
        // overwrite foo-5 with a superset of old reassignments
        // add new reassignments to bar-0
        Map<TopicPartition, PartitionReassignment> currentReassignments = new HashMap<>();

        currentReassignments.put(new TopicPartition("foo", 0), new PartitionReassignment(
            asList(1, 2, 3, 4), asList(4), asList(3)));
        currentReassignments.put(new TopicPartition("foo", 1), new PartitionReassignment(
            asList(4, 5, 6, 7, 8), asList(7, 8), asList(4, 5)));
        currentReassignments.put(new TopicPartition("foo", 2), new PartitionReassignment(
            asList(1, 2, 3, 4), asList(3, 4), asList(1, 2)));
        currentReassignments.put(new TopicPartition("foo", 3), new PartitionReassignment(
            asList(1, 2, 3, 4), asList(3, 4), asList(1, 2)));
        currentReassignments.put(new TopicPartition("foo", 4), new PartitionReassignment(
            asList(1, 2, 3, 4), asList(3, 4), asList(1, 2)));
        currentReassignments.put(new TopicPartition("foo", 5), new PartitionReassignment(
            asList(1, 2, 3, 4), asList(3, 4), asList(1, 2)));

        Map<TopicPartition, List<Integer>> proposedParts = new HashMap<>();

        proposedParts.put(new TopicPartition("foo", 0), asList(1, 2, 5));
        proposedParts.put(new TopicPartition("foo", 2), asList(3, 4));
        proposedParts.put(new TopicPartition("foo", 3), asList(5, 6));
        proposedParts.put(new TopicPartition("foo", 4), asList(3));
        proposedParts.put(new TopicPartition("foo", 5), asList(3, 4, 5, 6));
        proposedParts.put(new TopicPartition("bar", 0), asList(1, 2, 3));

        Map<TopicPartition, List<Integer>> currentParts = new HashMap<>();

        currentParts.put(new TopicPartition("foo", 0), asList(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 1), asList(4, 5, 6, 7, 8));
        currentParts.put(new TopicPartition("foo", 2), asList(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 3), asList(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 4), asList(1, 2, 3, 4));
        currentParts.put(new TopicPartition("foo", 5), asList(1, 2, 3, 4));
        currentParts.put(new TopicPartition("bar", 0), asList(2, 3, 4));
        currentParts.put(new TopicPartition("baz", 0), asList(1, 2, 3));

        Map<String, Map<Integer, PartitionMove>> moveMap = calculateProposedMoveMap(currentReassignments, proposedParts, currentParts);

        Map<Integer, PartitionMove> fooMoves = new HashMap<>();

        fooMoves.put(0, new PartitionMove(new HashSet<>(asList(1, 2, 3)), new HashSet<>(asList(5))));
        fooMoves.put(1, new PartitionMove(new HashSet<>(asList(4, 5, 6)), new HashSet<>(asList(7, 8))));
        fooMoves.put(2, new PartitionMove(new HashSet<>(asList(1, 2)), new HashSet<>(asList(3, 4))));
        fooMoves.put(3, new PartitionMove(new HashSet<>(asList(1, 2)), new HashSet<>(asList(5, 6))));
        fooMoves.put(4, new PartitionMove(new HashSet<>(asList(1, 2)), new HashSet<>(asList(3))));
        fooMoves.put(5, new PartitionMove(new HashSet<>(asList(1, 2)), new HashSet<>(asList(3, 4, 5, 6))));

        Map<Integer, PartitionMove> barMoves = new HashMap<>();

        barMoves.put(0, new PartitionMove(new HashSet<>(asList(2, 3, 4)), new HashSet<>(asList(1))));

        assertEquals(fooMoves, moveMap.get("foo"));
        assertEquals(barMoves, moveMap.get("bar"));

        Map<String, String> expLeaderThrottle = new HashMap<>();

        expLeaderThrottle.put("foo", "0:1,0:2,0:3,1:4,1:5,1:6,2:1,2:2,3:1,3:2,4:1,4:2,5:1,5:2");
        expLeaderThrottle.put("bar", "0:2,0:3,0:4");

        assertEquals(expLeaderThrottle, calculateLeaderThrottles(moveMap));

        Map<String, String> expFollowerThrottle = new HashMap<>();

        expFollowerThrottle.put("foo", "0:5,1:7,1:8,2:3,2:4,3:5,3:6,4:3,5:3,5:4,5:5,5:6");
        expFollowerThrottle.put("bar", "0:1");

        assertEquals(expFollowerThrottle, calculateFollowerThrottles(moveMap));

        assertEquals(new HashSet<>(asList(1, 2, 3, 4, 5, 6, 7, 8)), calculateReassigningBrokers(moveMap));
        assertEquals(new HashSet<>(asList(0, 2)), calculateMovingBrokers(new HashSet<>(asList(
            new TopicPartitionReplica("quux", 0, 0),
            new TopicPartitionReplica("quux", 1, 2)))));
    }

    @Test
    public void testParseExecuteAssignmentArgs() throws Exception {
        assertStartsWith("Partition reassignment list cannot be empty",
            assertThrows(AdminCommandFailedException.class,
                () -> parseExecuteAssignmentArgs("{\"version\":1,\"partitions\":[]}"),
                "Expected to detect empty partition reassignment list").getMessage());
        assertStartsWith("Partition reassignment contains duplicate topic partitions",
            assertThrows(AdminCommandFailedException.class, () -> parseExecuteAssignmentArgs(
                "{\"version\":1,\"partitions\":" +
                    "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1],\"log_dirs\":[\"any\",\"any\"]}," +
                    "{\"topic\":\"foo\",\"partition\":0,\"replicas\":[2,3,4],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                    "]}"), "Expected to detect a partition list with duplicate entries").getMessage());
        assertStartsWith("Partition reassignment contains duplicate topic partitions",
            assertThrows(AdminCommandFailedException.class, () -> parseExecuteAssignmentArgs(
                "{\"version\":1,\"partitions\":" +
                    "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1],\"log_dirs\":[\"/abc\",\"/def\"]}," +
                    "{\"topic\":\"foo\",\"partition\":0,\"replicas\":[2,3],\"log_dirs\":[\"/abc\",\"/def\"]}" +
                    "]}"), "Expected to detect a partition replica list with duplicate entries").getMessage());
        assertStartsWith("Partition replica lists may not contain duplicate entries",
            assertThrows(AdminCommandFailedException.class, () -> parseExecuteAssignmentArgs(
                "{\"version\":1,\"partitions\":" +
                    "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,0],\"log_dirs\":[\"/abc\",\"/def\"]}," +
                    "{\"topic\":\"foo\",\"partition\":1,\"replicas\":[2,3],\"log_dirs\":[\"/abc\",\"/def\"]}" +
                    "]}"), "Expected to detect a partition replica list with duplicate entries").getMessage());

        Map<TopicPartition, List<Integer>> partitionsToBeReassigned = new HashMap<>();

        partitionsToBeReassigned.put(new TopicPartition("foo", 0), asList(1, 2, 3));
        partitionsToBeReassigned.put(new TopicPartition("foo", 1), asList(3, 4, 5));

        Entry<Map<TopicPartition, List<Integer>>, Map<TopicPartitionReplica, String>> actual = parseExecuteAssignmentArgs(
            "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}," +
                "{\"topic\":\"foo\",\"partition\":1,\"replicas\":[3,4,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                "]}");

        assertEquals(partitionsToBeReassigned, actual.getKey());
        assertTrue(actual.getValue().isEmpty());

        Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>();

        replicaAssignment.put(new TopicPartitionReplica("foo", 0, 1), "/tmp/a");
        replicaAssignment.put(new TopicPartitionReplica("foo", 0, 2), "/tmp/b");
        replicaAssignment.put(new TopicPartitionReplica("foo", 0, 3), "/tmp/c");

        actual = parseExecuteAssignmentArgs(
            "{\"version\":1,\"partitions\":" +
                "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"/tmp/a\",\"/tmp/b\",\"/tmp/c\"]}" +
                "]}");

        assertEquals(Collections.singletonMap(new TopicPartition("foo", 0), asList(1, 2, 3)), actual.getKey());
        assertEquals(replicaAssignment, actual.getValue());
    }

    @Test
    public void testExecuteWithInvalidPartitionsFails() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(5).build()) {
            addTopics(adminClient);
            assertStartsWith("Topic quux not found",
                assertThrows(ExecutionException.class, () -> executeAssignment(adminClient, false,
                    "{\"version\":1,\"partitions\":" +
                        "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1],\"log_dirs\":[\"any\",\"any\"]}," +
                        "{\"topic\":\"quux\",\"partition\":0,\"replicas\":[2,3,4],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                        "]}", -1L, -1L, 10000L, 0, false, Time.SYSTEM), "Expected reassignment with non-existent topic to fail").getCause().getMessage());
        }
    }

    @Test
    public void testExecuteWithInvalidBrokerIdFails() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            assertStartsWith("Unknown broker id 4",
                assertThrows(AdminCommandFailedException.class, () -> executeAssignment(adminClient, false,
                    "{\"version\":1,\"partitions\":" +
                        "[{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1],\"log_dirs\":[\"any\",\"any\"]}," +
                        "{\"topic\":\"foo\",\"partition\":1,\"replicas\":[2,3,4],\"log_dirs\":[\"any\",\"any\",\"any\"]}" +
                        "]}", -1L, -1L, 10000L, 0, false, Time.SYSTEM), "Expected reassignment with non-existent broker id to fail").getMessage());
        }
    }

    @Test
    public void testModifyBrokerInterBrokerThrottle() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            modifyInterBrokerThrottle(adminClient, new HashSet<>(asList(0, 1, 2)), 1000);
            modifyInterBrokerThrottle(adminClient, new HashSet<>(asList(0, 3)), 100);
            List<ConfigResource> brokers = new ArrayList<>();
            for (int i = 0; i < 4; i++)
                brokers.add(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(i)));
            Map<ConfigResource, Config> results = adminClient.describeConfigs(brokers).all().get();
            verifyBrokerThrottleResults(results.get(brokers.get(0)), 100, -1);
            verifyBrokerThrottleResults(results.get(brokers.get(1)), 1000, -1);
            verifyBrokerThrottleResults(results.get(brokers.get(2)), 1000, -1);
            verifyBrokerThrottleResults(results.get(brokers.get(3)), 100, -1);
        }
    }

    @Test
    public void testModifyLogDirThrottle() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            modifyLogDirThrottle(adminClient, new HashSet<>(asList(0, 1, 2)), 2000);
            modifyLogDirThrottle(adminClient, new HashSet<>(asList(0, 3)), -1);

            List<ConfigResource> brokers = new ArrayList<>();
            for (int i = 0; i < 4; i++)
                brokers.add(new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(i)));

            Map<ConfigResource, Config> results = adminClient.describeConfigs(brokers).all().get();

            verifyBrokerThrottleResults(results.get(brokers.get(0)), -1, 2000);
            verifyBrokerThrottleResults(results.get(brokers.get(1)), -1, 2000);
            verifyBrokerThrottleResults(results.get(brokers.get(2)), -1, 2000);
            verifyBrokerThrottleResults(results.get(brokers.get(3)), -1, -1);
        }
    }

    @Test
    public void testCurReassignmentsToString() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            assertEquals("No partition reassignments found.", curReassignmentsToString(adminClient));

            Map<TopicPartition, List<Integer>> reassignments = new HashMap<>();

            reassignments.put(new TopicPartition("foo", 1), asList(4, 5, 3));
            reassignments.put(new TopicPartition("foo", 0), asList(0, 1, 4, 2));
            reassignments.put(new TopicPartition("bar", 0), asList(2, 3));

            Map<TopicPartition, Throwable> reassignmentResult = alterPartitionReassignments(adminClient, reassignments);

            assertTrue(reassignmentResult.isEmpty());
            assertEquals(String.join(System.lineSeparator(),
                "Current partition reassignments:",
                "bar-0: replicas: 2,3,0. removing: 0.",
                "foo-0: replicas: 0,1,2. adding: 4.",
                "foo-1: replicas: 1,2,3. adding: 4,5. removing: 1,2."),
                curReassignmentsToString(adminClient));
        }
    }

    private void verifyBrokerThrottleResults(Config config,
                                             long expectedInterBrokerThrottle,
                                             long expectedReplicaAlterLogDirsThrottle) {
        Map<String, String> configs = new HashMap<>();
        config.entries().forEach(entry -> configs.put(entry.name(), entry.value()));
        if (expectedInterBrokerThrottle >= 0) {
            assertEquals(Long.toString(expectedInterBrokerThrottle),
                configs.getOrDefault(QuotaConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, ""));
            assertEquals(Long.toString(expectedInterBrokerThrottle),
                configs.getOrDefault(QuotaConfigs.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, ""));
        }
        if (expectedReplicaAlterLogDirsThrottle >= 0) {
            assertEquals(Long.toString(expectedReplicaAlterLogDirsThrottle),
                configs.getOrDefault(QuotaConfigs.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""));
        }
    }

    @Test
    public void testModifyTopicThrottles() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);

            Map<String, String> leaderThrottles = new HashMap<>();

            leaderThrottles.put("foo", "leaderFoo");
            leaderThrottles.put("bar", "leaderBar");

            modifyTopicThrottles(adminClient,
                leaderThrottles,
                Collections.singletonMap("bar", "followerBar"));
            List<ConfigResource> topics = Stream.of("bar", "foo").map(
                id -> new ConfigResource(ConfigResource.Type.TOPIC, id)).collect(Collectors.toList());
            Map<ConfigResource, Config> results = adminClient.describeConfigs(topics).all().get();
            verifyTopicThrottleResults(results.get(topics.get(0)), "leaderBar", "followerBar");
            verifyTopicThrottleResults(results.get(topics.get(1)), "leaderFoo", "");
        }
    }

    private void verifyTopicThrottleResults(Config config,
                                            String expectedLeaderThrottle,
                                            String expectedFollowerThrottle) {
        Map<String, String> configs = new HashMap<>();
        config.entries().forEach(entry -> configs.put(entry.name(), entry.value()));
        assertEquals(expectedLeaderThrottle,
            configs.getOrDefault(QuotaConfigs.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ""));
        assertEquals(expectedFollowerThrottle,
            configs.getOrDefault(QuotaConfigs.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, ""));
    }

    @Test
    public void testAlterReplicaLogDirs() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().
            numBrokers(4).
            brokerLogDirs(Collections.nCopies(4,
                asList("/tmp/kafka-logs0", "/tmp/kafka-logs1"))).
            build()) {

            addTopics(adminClient);

            Map<TopicPartitionReplica, String> assignment = new HashMap<>();

            assignment.put(new TopicPartitionReplica("foo", 0, 0), "/tmp/kafka-logs1");
            assignment.put(new TopicPartitionReplica("quux", 1, 0), "/tmp/kafka-logs1");

            assertEquals(
                new HashSet<>(asList(new TopicPartitionReplica("foo", 0, 0))),
                alterReplicaLogDirs(adminClient, assignment)
            );
        }
    }

    public void assertStartsWith(String prefix, String str) {
        assertTrue(str.startsWith(prefix), String.format("Expected the string to start with %s, but it was %s", prefix, str));
    }

    @Test
    public void testPartitionProposedReassignmentsIntoBatches() {
        Map<TopicPartition, List<Integer>> proposed = new LinkedHashMap<>();
        proposed.put(new TopicPartition("b", 1), asList(0, 1));
        proposed.put(new TopicPartition("a", 0), asList(0, 1));
        proposed.put(new TopicPartition("a", 1), asList(1, 0));

        assertEquals(1, partitionProposedReassignmentsIntoBatches(proposed, 0).size());
        assertEquals(proposed, partitionProposedReassignmentsIntoBatches(proposed, 0).get(0));

        List<Map<TopicPartition, List<Integer>>> batches = partitionProposedReassignmentsIntoBatches(proposed, 2);
        assertEquals(2, batches.size());
        assertEquals(2, batches.get(0).size());
        assertEquals(1, batches.get(1).size());
        // compareTopicPartitions: a-0, a-1, then b-1
        assertTrue(batches.get(0).containsKey(new TopicPartition("a", 0)));
        assertTrue(batches.get(0).containsKey(new TopicPartition("a", 1)));
        assertTrue(batches.get(1).containsKey(new TopicPartition("b", 1)));
    }

    /**
     * Batching must follow {@link ReassignPartitionsCommand#compareTopicPartitions}, not insertion order
     * (operators may list partitions in any order in the JSON file).
     */
    @Test
    public void testPartitionProposedReassignmentsIntoBatchesSortsKeysNotInsertionOrder() {
        Map<TopicPartition, List<Integer>> proposed = new LinkedHashMap<>();
        proposed.put(new TopicPartition("z", 0), asList(0));
        proposed.put(new TopicPartition("b", 1), asList(0, 1));
        proposed.put(new TopicPartition("a", 0), asList(0, 1));

        List<Map<TopicPartition, List<Integer>>> batches = partitionProposedReassignmentsIntoBatches(proposed, 2);
        assertEquals(2, batches.size());
        assertEquals(2, batches.get(0).size());
        assertEquals(1, batches.get(1).size());
        assertTrue(batches.get(0).containsKey(new TopicPartition("a", 0)));
        assertTrue(batches.get(0).containsKey(new TopicPartition("b", 1)));
        assertTrue(batches.get(1).containsKey(new TopicPartition("z", 0)));
    }

    @Test
    public void testParseExecuteAssignmentArgsReadsReplicaAssignmentsFromJson() throws Exception {
        String json = "{\"version\":1,\"partitions\":[" +
            "{\"topic\":\"bar\",\"partition\":0,\"replicas\":[2,3,0]}," +
            "{\"topic\":\"foo\",\"partition\":0,\"replicas\":[0,1,2]}" +
            "]}";
        Map<TopicPartition, List<Integer>> map = parseExecuteAssignmentArgs(json).getKey();
        assertEquals(2, map.size());
        assertEquals(asList(2, 3, 0), map.get(new TopicPartition("bar", 0)));
        assertEquals(asList(0, 1, 2), map.get(new TopicPartition("foo", 0)));
    }

    @Test
    public void testExecutePartitionReassignmentsIncrementallyRequiresPositiveBatchSize() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(1).build()) {
            assertEquals("Incremental partition reassignment requires reassignment-batch-size > 0",
                assertThrows(TerseException.class,
                    () -> executePartitionReassignmentsIncrementally(
                        adminClient,
                        Collections.singletonMap(new TopicPartition("t", 0), asList(0)),
                        0,
                        Time.SYSTEM)).getMessage());
        }
    }

    @Test
    public void testExecutePartitionReassignmentsIncrementallySinglePartition() throws Exception {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            Map<TopicPartition, List<Integer>> proposed = new LinkedHashMap<>();
            proposed.put(new TopicPartition("foo", 0), asList(0, 1, 2));
            assertTrue(executePartitionReassignmentsIncrementally(adminClient, proposed, 3, Time.SYSTEM).isEmpty());
        }
    }

    /**
     * Non-incremental execute with batch size 2: two {@code alterPartitionReassignments} calls (first batch size 2,
     * second size 1). The second alter runs only after {@code waitUntilBatchPartitionReassignmentsComplete} has polled
     * at least once with batch 1 still active, then sees it complete (mocked {@code listPartitionReassignments} and
     * {@code describeTopics}). Proposal {@code [3,1,2]} vs initial {@code [0,1,2]} simulates a real move for batch 1.
     * <p>
     * Assertions: exactly two alter RPC batches; {@link MockTime} at each alter shows the second alter at least one
     * poll interval ({@link #PARTITION_REASSIGNMENT_WAIT_POLL_MS} ms) after the first (the wait loop sleeps while batch 1
     * is incomplete). Only three {@code listPartitionReassignments} calls occur (execute preamble plus two wait polls).
     */
    @Test
    public void testExecuteNonIncrementalBatchWaitsForBatchBeforeNextAlter() throws Exception {
        MockTime mockTime = new MockTime();
        try (FixedBatchNonIncrementalRecordingMockAdminClient adminClient = new FixedBatchNonIncrementalRecordingMockAdminClient(mockTime)) {
            List<Node> brokers = adminClient.brokers();
            String topic = FixedBatchNonIncrementalRecordingMockAdminClient.TOPIC;
            adminClient.addTopic(false, topic, asList(
                new TopicPartitionInfo(0, brokers.get(0),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2))),
                new TopicPartitionInfo(1, brokers.get(1),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2))),
                new TopicPartitionInfo(2, brokers.get(2),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)))
            ), Collections.emptyMap());

            String json = "{\"version\":1,\"partitions\":[" +
                "{\"topic\":\"" + topic + "\",\"partition\":0,\"replicas\":[3,1,2]}," +
                "{\"topic\":\"" + topic + "\",\"partition\":1,\"replicas\":[3,1,2]}," +
                "{\"topic\":\"" + topic + "\",\"partition\":2,\"replicas\":[3,1,2]}" +
                "]}";

            executeAssignment(adminClient, false, json, -1L, -1L, 10000L, 2, false, mockTime);

            assertEquals(3, adminClient.listPartitionReassignmentsInvocations());
            assertEquals(2, adminClient.recordedAlterBatches().size(), "expect two AlterPartitionReassignments calls");
            assertEquals(2, adminClient.recordedAlterTimestampsMs().size());
            List<Long> alterTimes = adminClient.recordedAlterTimestampsMs();
            assertTrue(alterTimes.get(1) - alterTimes.get(0) >= PARTITION_REASSIGNMENT_WAIT_POLL_MS,
                () -> String.format(
                    "second alter should run only after waitUntilBatchPartitionReassignmentsComplete slept at least %d ms (first alter at %d ms, second at %d ms)",
                    PARTITION_REASSIGNMENT_WAIT_POLL_MS, alterTimes.get(0), alterTimes.get(1)));
            assertEquals(asList(
                    asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)),
                    Collections.singletonList(new TopicPartition(topic, 2))),
                adminClient.recordedAlterBatches());
        }
    }

    /**
     * After the first non-incremental batch completes, the second {@code alterPartitionReassignments} fails; the tool
     * surfaces a {@link TerseException} and does not start a third batch.
     */
    @Test
    public void testExecuteNonIncrementalSecondBatchAlterFails() throws Exception {
        MockTime mockTime = new MockTime();
        try (SecondBatchAlterFailsNonIncrementalMockAdminClient adminClient = new SecondBatchAlterFailsNonIncrementalMockAdminClient()) {
            List<Node> brokers = adminClient.brokers();
            String topic = SecondBatchAlterFailsNonIncrementalMockAdminClient.TOPIC;
            adminClient.addTopic(false, topic, asList(
                new TopicPartitionInfo(0, brokers.get(0),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2))),
                new TopicPartitionInfo(1, brokers.get(1),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2))),
                new TopicPartitionInfo(2, brokers.get(2),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)))
            ), Collections.emptyMap());

            String json = "{\"version\":1,\"partitions\":[" +
                "{\"topic\":\"" + topic + "\",\"partition\":0,\"replicas\":[3,1,2]}," +
                "{\"topic\":\"" + topic + "\",\"partition\":1,\"replicas\":[3,1,2]}," +
                "{\"topic\":\"" + topic + "\",\"partition\":2,\"replicas\":[3,1,2]}" +
                "]}";

            TerseException ex = assertThrows(TerseException.class,
                () -> executeAssignment(adminClient, false, json, -1L, -1L, 10000L, 2, false, mockTime));
            assertTrue(ex.getMessage().contains("injected second-batch failure"),
                () -> "unexpected message: " + ex.getMessage());
            assertEquals(1, adminClient.recordedAlterBatches().size());
            assertEquals(
                Collections.singletonList(asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1))),
                adminClient.recordedAlterBatches());
            assertEquals(3, adminClient.listPartitionReassignmentsInvocations());
        }
    }

    /**
     * N=3 partitions, batch window K=2: first {@code alterPartitionReassignments} targets {@code inc-0} and {@code inc-1}
     * with replicas {@code [3,1,2]} (initial {@code [0,1,2]}); the second alter targets {@code inc-2} only after the
     * first two leave the active reassignment set from {@code listPartitionReassignments}. Pending deque order follows
     * {@link ReassignPartitionsCommand#compareTopicPartitions}, not map insertion order; recorded alter keys use that order.
     * <p>
     * After the mock clears the first batch from the active set, {@code describeTopics} is overridden so partitions 0
     * and 1 appear at {@code [3,1,2]} while partition 2 still reads {@code [0,1,2]}, matching a partial replica move.
     */
    @Test
    public void testExecutePartitionReassignmentsIncrementallySlidingWindowAlterOrder() throws Exception {
        try (WindowedRecordingMockAdminClient adminClient = new WindowedRecordingMockAdminClient()) {
            List<Node> brokers = adminClient.brokers();
            String topic = WindowedRecordingMockAdminClient.TOPIC;
            adminClient.addTopic(false, topic, asList(
                new TopicPartitionInfo(0, brokers.get(0),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2))),
                new TopicPartitionInfo(1, brokers.get(1),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2))),
                new TopicPartitionInfo(2, brokers.get(2),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)),
                    asList(brokers.get(0), brokers.get(1), brokers.get(2)))
            ), Collections.emptyMap());

            Map<TopicPartition, List<Integer>> proposed = new LinkedHashMap<>();
            proposed.put(new TopicPartition(topic, 2), asList(3, 1, 2));
            proposed.put(new TopicPartition(topic, 0), asList(3, 1, 2));
            proposed.put(new TopicPartition(topic, 1), asList(3, 1, 2));

            assertTrue(executePartitionReassignmentsIncrementally(adminClient, proposed, 2, new MockTime()).isEmpty());

            assertEquals(2, adminClient.listPartitionReassignmentsInvocations());
            assertEquals(asList(
                    asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)),
                    Collections.singletonList(new TopicPartition(topic, 2))),
                adminClient.recordedAlterBatches());
        }
    }

    @Test
    public void testPropagateInvalidJsonError() {
        try (MockAdminClient adminClient = new MockAdminClient.Builder().numBrokers(4).build()) {
            addTopics(adminClient);
            assertStartsWith("Unexpected character",
                assertThrows(AdminOperationException.class, () -> executeAssignment(adminClient, false, "{invalid_json", -1L, -1L, 10000L, 0, false, Time.SYSTEM)).getMessage());
        }
    }

    /**
     * For {@link ReassignPartitionsUnitTest#testExecuteNonIncrementalBatchWaitsForBatchBeforeNextAlter}: records alter
     * batches; steers {@code listPartitionReassignments} so the first wait poll sees the first two partitions of topic
     * {@code batch_wait_test} as active, the next sees none; {@code describeTopics} then reports {@code [3,1,2]} for
     * those partitions (partition 2 unchanged in metadata) so {@code waitUntilBatchPartitionReassignmentsComplete}
     * matches a real replica move for batch 1.
     */
    private static final class FixedBatchNonIncrementalRecordingMockAdminClient extends MockAdminClient {
        /** Topic used only for {@link ReassignPartitionsUnitTest#testExecuteNonIncrementalBatchWaitsForBatchBeforeNextAlter}. */
        public static final String TOPIC = "batch_wait_test";
        private static final List<Node> BROKERS_4 = createBrokers(4);
        private static final Set<TopicPartition> BATCH_WAIT_FIRST_PARTITIONS = Collections.unmodifiableSet(new HashSet<>(asList(
            new TopicPartition(TOPIC, 0),
            new TopicPartition(TOPIC, 1))));

        private final List<List<TopicPartition>> recordedAlterBatches = new ArrayList<>();
        private final List<Long> recordedAlterTimestampsMs = new ArrayList<>();
        private final MockTime mockTime;
        private int listPartitionReassignmentsInvocations;

        FixedBatchNonIncrementalRecordingMockAdminClient(MockTime mockTime) {
            super(BROKERS_4, BROKERS_4.get(0));
            this.mockTime = mockTime;
        }

        private static List<Node> createBrokers(int n) {
            List<Node> brokers = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                brokers.add(new Node(i, "localhost", 1000 + i));
            }
            return brokers;
        }

        /** Partitions 0 and 1 at target {@code [3,1,2]}; partition 2 still on initial brokers. */
        private TopicDescription partialFixReplicaDescription() {
            List<Node> br = brokers();
            Node n0 = br.get(0);
            Node n1 = br.get(1);
            Node n2 = br.get(2);
            Node n3 = br.get(3);
            List<TopicPartitionInfo> partitions = asList(
                new TopicPartitionInfo(0, n3, asList(n3, n1, n2), asList(n3, n1, n2)),
                new TopicPartitionInfo(1, n3, asList(n3, n1, n2), asList(n3, n1, n2)),
                new TopicPartitionInfo(2, n2, asList(n0, n1, n2), asList(n0, n1, n2)));
            return new TopicDescription(TOPIC, false, partitions);
        }

        int listPartitionReassignmentsInvocations() {
            return listPartitionReassignmentsInvocations;
        }

        List<List<TopicPartition>> recordedAlterBatches() {
            return recordedAlterBatches;
        }

        List<Long> recordedAlterTimestampsMs() {
            return Collections.unmodifiableList(new ArrayList<>(recordedAlterTimestampsMs));
        }

        @Override
        public synchronized AlterPartitionReassignmentsResult alterPartitionReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> newReassignments,
            AlterPartitionReassignmentsOptions options) {
            recordedAlterTimestampsMs.add(mockTime.milliseconds());
            recordedAlterBatches.add(sortedTopicPartitionsForRecording(newReassignments.keySet()));
            return super.alterPartitionReassignments(newReassignments, options);
        }

        @Override
        public synchronized DescribeTopicsResult describeTopics(TopicCollection topics, DescribeTopicsOptions options) {
            if (!(topics instanceof TopicNameCollection)) {
                return super.describeTopics(topics, options);
            }
            Collection<String> topicNames = ((TopicNameCollection) topics).topicNames();
            if (!topicNames.contains(TOPIC) || listPartitionReassignmentsInvocations < 3) {
                return super.describeTopics(topics, options);
            }
            DescribeTopicsResult sup = super.describeTopics(topics, options);
            Map<String, KafkaFuture<TopicDescription>> futures = new HashMap<>(sup.topicNameValues());
            KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
            future.complete(partialFixReplicaDescription());
            futures.put(TOPIC, future);
            return new DescribeTopicsResult(null, futures) { };
        }

        @Override
        public synchronized ListPartitionReassignmentsResult listPartitionReassignments(
            Optional<Set<TopicPartition>> partitions,
            ListPartitionReassignmentsOptions options) {
            listPartitionReassignmentsInvocations++;
            if (listPartitionReassignmentsInvocations == 2) {
                return super.listPartitionReassignments(Optional.of(BATCH_WAIT_FIRST_PARTITIONS), options);
            }
            if (listPartitionReassignmentsInvocations == 3) {
                return super.listPartitionReassignments(Optional.of(Collections.emptySet()), options);
            }
            return super.listPartitionReassignments(partitions, options);
        }
    }

    /**
     * Same steering as {@link FixedBatchNonIncrementalRecordingMockAdminClient} for the first batch wait, but the second
     * {@code alterPartitionReassignments} completes exceptionally (see {@link ReassignPartitionsUnitTest#testExecuteNonIncrementalSecondBatchAlterFails}).
     */
    private static final class SecondBatchAlterFailsNonIncrementalMockAdminClient extends MockAdminClient {
        public static final String TOPIC = "fail_nb";
        private static final List<Node> BROKERS_4 = createBrokers(4);
        private static final Set<TopicPartition> FIRST_BATCH_PARTITIONS = Collections.unmodifiableSet(new HashSet<>(asList(
            new TopicPartition(TOPIC, 0),
            new TopicPartition(TOPIC, 1))));

        private final List<List<TopicPartition>> recordedAlterBatches = new ArrayList<>();
        private int listPartitionReassignmentsInvocations;
        private int alterPartitionReassignmentsCallCount;

        SecondBatchAlterFailsNonIncrementalMockAdminClient() {
            super(BROKERS_4, BROKERS_4.get(0));
        }

        private static List<Node> createBrokers(int n) {
            List<Node> brokers = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                brokers.add(new Node(i, "localhost", 1000 + i));
            }
            return brokers;
        }

        private TopicDescription partialFixReplicaDescription() {
            List<Node> br = brokers();
            Node n0 = br.get(0);
            Node n1 = br.get(1);
            Node n2 = br.get(2);
            Node n3 = br.get(3);
            List<TopicPartitionInfo> partitions = asList(
                new TopicPartitionInfo(0, n3, asList(n3, n1, n2), asList(n3, n1, n2)),
                new TopicPartitionInfo(1, n3, asList(n3, n1, n2), asList(n3, n1, n2)),
                new TopicPartitionInfo(2, n2, asList(n0, n1, n2), asList(n0, n1, n2)));
            return new TopicDescription(TOPIC, false, partitions);
        }

        int listPartitionReassignmentsInvocations() {
            return listPartitionReassignmentsInvocations;
        }

        List<List<TopicPartition>> recordedAlterBatches() {
            return recordedAlterBatches;
        }

        @Override
        public synchronized AlterPartitionReassignmentsResult alterPartitionReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> newReassignments,
            AlterPartitionReassignmentsOptions options) {
            alterPartitionReassignmentsCallCount++;
            if (alterPartitionReassignmentsCallCount == 1) {
                recordedAlterBatches.add(sortedTopicPartitionsForRecording(newReassignments.keySet()));
                return super.alterPartitionReassignments(newReassignments, options);
            }
            Map<TopicPartition, KafkaFuture<Void>> futures = new HashMap<>();
            for (TopicPartition tp : newReassignments.keySet()) {
                KafkaFutureImpl<Void> fut = new KafkaFutureImpl<>();
                fut.completeExceptionally(new InvalidReplicationFactorException("injected second-batch failure"));
                futures.put(tp, fut);
            }
            return newAlterPartitionReassignmentsResult(futures);
        }

        @Override
        public synchronized DescribeTopicsResult describeTopics(TopicCollection topics, DescribeTopicsOptions options) {
            if (!(topics instanceof TopicNameCollection)) {
                return super.describeTopics(topics, options);
            }
            Collection<String> topicNames = ((TopicNameCollection) topics).topicNames();
            if (!topicNames.contains(TOPIC) || listPartitionReassignmentsInvocations < 3) {
                return super.describeTopics(topics, options);
            }
            DescribeTopicsResult sup = super.describeTopics(topics, options);
            Map<String, KafkaFuture<TopicDescription>> futures = new HashMap<>(sup.topicNameValues());
            KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
            future.complete(partialFixReplicaDescription());
            futures.put(TOPIC, future);
            return new DescribeTopicsResult(null, futures) { };
        }

        @Override
        public synchronized ListPartitionReassignmentsResult listPartitionReassignments(
            Optional<Set<TopicPartition>> partitions,
            ListPartitionReassignmentsOptions options) {
            listPartitionReassignmentsInvocations++;
            if (listPartitionReassignmentsInvocations == 2) {
                return super.listPartitionReassignments(Optional.of(FIRST_BATCH_PARTITIONS), options);
            }
            if (listPartitionReassignmentsInvocations == 3) {
                return super.listPartitionReassignments(Optional.of(Collections.emptySet()), options);
            }
            return super.listPartitionReassignments(partitions, options);
        }
    }

    /**
     * Records each {@code alterPartitionReassignments} batch (topic-partition keys sorted with
     * {@link ReassignPartitionsCommand#compareTopicPartitions} for stable assertions).
     * The second {@code listPartitionReassignments} response uses an empty partition filter so the mock returns no
     * active reassignments for the first batch; {@link ReassignPartitionsCommand#removeCompletedInFlightPartitionReassignments}
     * then drops them and the sliding window submits the remaining partition.
     */
    private static final class WindowedRecordingMockAdminClient extends MockAdminClient {
        /** Topic used only for {@link ReassignPartitionsUnitTest#testExecutePartitionReassignmentsIncrementallySlidingWindowAlterOrder}. */
        public static final String TOPIC = "inc";
        private static final List<Node> BROKERS_4 = createBrokers(4);
        private static final Set<TopicPartition> INC_FIRST_BATCH = Collections.unmodifiableSet(new HashSet<>(asList(
            new TopicPartition(TOPIC, 0),
            new TopicPartition(TOPIC, 1))));

        private final List<List<TopicPartition>> recordedAlterBatches = new ArrayList<>();
        private int listPartitionReassignmentsInvocations;

        WindowedRecordingMockAdminClient() {
            super(BROKERS_4, BROKERS_4.get(0));
        }

        private static List<Node> createBrokers(int n) {
            List<Node> brokers = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                brokers.add(new Node(i, "localhost", 1000 + i));
            }
            return brokers;
        }

        /** Partitions 0 and 1 at target {@code [3,1,2]}; partition 2 still on initial brokers. */
        private TopicDescription partialIncReplicaTopicDescription() {
            List<Node> br = brokers();
            Node n0 = br.get(0);
            Node n1 = br.get(1);
            Node n2 = br.get(2);
            Node n3 = br.get(3);
            List<TopicPartitionInfo> partitions = asList(
                new TopicPartitionInfo(0, n3, asList(n3, n1, n2), asList(n3, n1, n2)),
                new TopicPartitionInfo(1, n3, asList(n3, n1, n2), asList(n3, n1, n2)),
                new TopicPartitionInfo(2, n2, asList(n0, n1, n2), asList(n0, n1, n2)));
            return new TopicDescription(TOPIC, false, partitions);
        }

        int listPartitionReassignmentsInvocations() {
            return listPartitionReassignmentsInvocations;
        }

        List<List<TopicPartition>> recordedAlterBatches() {
            return recordedAlterBatches;
        }

        @Override
        public synchronized AlterPartitionReassignmentsResult alterPartitionReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> newReassignments,
            AlterPartitionReassignmentsOptions options) {
            recordedAlterBatches.add(sortedTopicPartitionsForRecording(newReassignments.keySet()));
            return super.alterPartitionReassignments(newReassignments, options);
        }

        @Override
        public synchronized DescribeTopicsResult describeTopics(TopicCollection topics, DescribeTopicsOptions options) {
            if (!(topics instanceof TopicNameCollection)) {
                return super.describeTopics(topics, options);
            }
            Collection<String> topicNames = ((TopicNameCollection) topics).topicNames();
            if (!topicNames.contains(TOPIC) || listPartitionReassignmentsInvocations < 2) {
                return super.describeTopics(topics, options);
            }
            DescribeTopicsResult sup = super.describeTopics(topics, options);
            Map<String, KafkaFuture<TopicDescription>> futures = new HashMap<>(sup.topicNameValues());
            KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
            future.complete(partialIncReplicaTopicDescription());
            futures.put(TOPIC, future);
            return new DescribeTopicsResult(null, futures) { };
        }

        @Override
        public synchronized ListPartitionReassignmentsResult listPartitionReassignments(
            Optional<Set<TopicPartition>> partitions,
            ListPartitionReassignmentsOptions options) {
            listPartitionReassignmentsInvocations++;
            if (listPartitionReassignmentsInvocations == 1) {
                return super.listPartitionReassignments(Optional.of(INC_FIRST_BATCH), options);
            }
            if (listPartitionReassignmentsInvocations == 2) {
                return super.listPartitionReassignments(Optional.of(Collections.emptySet()), options);
            }
            return super.listPartitionReassignments(partitions, options);
        }
    }
}
