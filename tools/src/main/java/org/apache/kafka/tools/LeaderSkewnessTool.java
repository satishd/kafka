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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


/**
 * The LeaderSkewnessTool is a utility for calculating and reassigning the leader skewness of Kafka topics.
 * It ensures that the leader replicas are evenly distributed across the available brokers and racks.
 *
 * <p>Usage:</p>
 * <pre>
 javac -d . -cp .:* LeaderSkewnessTool.java
 java -Dlog4j.configuration=file:../config/tools-log4j.properties -cp .:* org/apache/kafka/tools/LeaderSkewnessTool {bootstrap-server} {topic} {excluded-brokers} * </pre>

 * <p>Arguments:</p>
 * <ul>
 *   <li><b>bootstrap-servers</b>: A comma-separated list of host:port pairs to use for establishing the initial connection to the Kafka cluster.</li>
 *   <li><b>topic</b>: The name of the topic for which the leader skewness needs to be calculated and reassigned.</li>
 *   <li><b>excluded-brokers</b> (optional): A colon-separated list of broker IDs to exclude from the reassignment process.</li>
 * </ul>
 *
 * <p>Example:</p>
 * <pre>
 * java -cp kafka-tools.jar org.apache.kafka.tools.LeaderSkewnessTool localhost:9092 my-topic 1:2:3
 * </pre>
 *
 * <p>This tool performs the following steps:</p>
 * <ol>
 *   <li>Retrieves the cluster configuration and identifies brokers to exclude from the reassignment process.</li>
 *   <li>Groups the available brokers by rack and calculates the current partition placements for the specified topic.</li>
 *   <li>Determines if the topic can be reassigned based on the number of racks and replication factor.</li>
 *   <li>Calculates a new replica assignment that ensures even distribution of leader replicas across racks.</li>
 *   <li>Stores the new replica assignment plan in a JSON file.</li>
 *   <li>Prints a summary of the reassignment process.</li>
 * </ol>
 *
 */
public class LeaderSkewnessTool {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderSkewnessTool.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String topic = args[1];
        Set<Integer> excludedBrokers = new HashSet<>();
        if (args.length > 2) {
            String excludedBrokersStr = args[2];
            String[] chunks = excludedBrokersStr.split(":");
            for (String chunk : chunks) {
                excludedBrokers.add(Integer.parseInt(chunk.trim()));
            }
        }

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        try (Admin admin = Admin.create(props)) {
            DescribeClusterResult describeClusterResult = admin.describeCluster();
            Collection<Node> nodes = describeClusterResult.nodes().get();

            ConfigResource clusterResource = new ConfigResource(ConfigResource.Type.BROKER, "");
            Map<ConfigResource, Config> resourceConfigMap =
                    admin.describeConfigs(Collections.singleton(clusterResource)).all().get();
            Config config = resourceConfigMap.get(clusterResource);
            if (config == null) {
                LOGGER.error("Failed to get cluster config");
                return;
            }
            Set<Integer> leaderDeprioritizedList = getBrokerIds(config, "leader.deprioritized.list");
            Set<Integer> newReplicaExcludeList = getBrokerIds(config, "new.replica.exclude.list");
            Map<String, List<Node>> nodesByRack = nodes.stream()
                    .filter(node -> !newReplicaExcludeList.contains(node.id()))
                    .filter(node -> !excludedBrokers.contains(node.id()))
                    .collect(Collectors.groupingBy(Node::rack));
            List<String> racks = new ArrayList<>(nodesByRack.keySet());
            List<PartitionPlacement> currentPlacements = getCurrentPlacements(admin, Collections.singletonList(topic));
            if (!canReassignTopic(topic, racks, nodesByRack, currentPlacements)) {
                return;
            }

            // Calculate the new replica assignment based on rack awareness (IG)
            Map<String, Integer> rackToCounter = new HashMap<>();
            List<PartitionPlacement> newPlacements = new ArrayList<>();
            for (PartitionPlacement placement : currentPlacements) {
                List<Integer> newReplicas = new ArrayList<>();
                for (int i = 0; i < placement.replicas.size(); i++) {
                    String rack = racks.get(i % racks.size());
                    List<Node> nodesInRack = nodesByRack.get(rack);
                    int counter = rackToCounter.getOrDefault(rack, 0);
                    Node replica = nodesInRack.get(counter % nodesInRack.size());
                    newReplicas.add(replica.id());
                    rackToCounter.put(rack, counter + 1);
                }
                newPlacements.add(new PartitionPlacement(placement.partition, newReplicas));
            }
            shufflePreferredReplica(newPlacements);
            String filename = generateFilename();
            storeReplicaPlan(newPlacements, new File(filename));
            LOGGER.info("Successfully calculated new replica assignment and stored it in the file: {}", filename);
            printSummary(nodes, excludedBrokers, leaderDeprioritizedList, newReplicaExcludeList, racks, nodesByRack,
                    currentPlacements, newPlacements);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error("Error occurred while calculating leader skewness", e);
        }
    }

    private static boolean canReassignTopic(String topic,
                                            List<String> racks,
                                            Map<String, List<Node>> nodesByRack,
                                            List<PartitionPlacement> currentPlacements) {
        if (currentPlacements.isEmpty()) {
            LOGGER.error("No partitions found for topic {}", topic);
            return false;
        }
        int replicationFactor = 1;
        for (PartitionPlacement placement : currentPlacements) {
            replicationFactor = Math.max(replicationFactor, placement.replicas.size());
        }
        if (replicationFactor < 4 && racks.size() < replicationFactor) {
            LOGGER.error("Number of racks {} is less than replication factor {}", racks.size(), replicationFactor);
            return false;
        } else if (replicationFactor >= 4 && racks.size() < 3) {
            LOGGER.error("Number of racks {}, with this optimal replica assignment cannot be satisfied for RF: {}",
                    racks.size(), replicationFactor);
            return false;
        }
        for (String rack : racks) {
            if (nodesByRack.get(rack).isEmpty()) {
                LOGGER.error("No nodes found in rack {}", rack);
                return false;
            }
        }
        return true;
    }

    private static void printSummary(Collection<Node> nodes,
                                     Set<Integer> excludedBrokers,
                                     Set<Integer> leaderDeprioritizedList,
                                     Set<Integer> newReplicaExcludeList,
                                     List<String> racks,
                                     Map<String, List<Node>> nodesByRack,
                                     List<PartitionPlacement> currentPlacements,
                                     List<PartitionPlacement> newPlacements) {
        LOGGER.info("Summary:");
        LOGGER.info("Number of brokers in the cluster: {}", nodes.size());
        LOGGER.info("Excluded brokers: {}, leader deprioritized list: {}, new replica exclude list: {}",
                excludedBrokers, leaderDeprioritizedList, newReplicaExcludeList);
        Set<Integer> ineligibleBrokers = new HashSet<>();
        ineligibleBrokers.addAll(excludedBrokers);
        ineligibleBrokers.addAll(newReplicaExcludeList);
        LOGGER.info("Number of eligible brokers for replica assignment: {}", nodes.size() - ineligibleBrokers.size());
        LOGGER.info("Num racks: {}, racks: {}", racks.size(), racks);
        for (String rack : racks) {
            List<String> rackNodes = nodesByRack.get(rack)
                    .stream()
                    .map(node -> node.host() + ":" + node.port() + " id: " + node.idString()).collect(Collectors.toList());
            LOGGER.info("Rack: {}, numNodes: {}, nodes: {}", rack, rackNodes.size(), rackNodes);
        }
        LOGGER.info("Total number of replicas to be reassigned: {} for {} partitions",
                currentPlacements.stream().mapToInt(pp -> pp.replicas.size()).sum(), currentPlacements.size());
        Map<Integer, Node> nodeById = nodes.stream().collect(Collectors.toMap(Node::id, node -> node));
        Map<String, Integer> numReplicasAssignedPerRack = newPlacements.stream()
                .flatMap(placement -> placement.replicas.stream())
                .map(replica -> nodeById.get(replica).rack())
                .collect(Collectors.toMap(rack -> rack, rack -> 1, Integer::sum));
        LOGGER.info("Number of replicas reassigned to each rack: {}", numReplicasAssignedPerRack);
        LOGGER.info("Number of replicas reassigned to each broker: ");
        Map<Integer, LeaderAndReplicaCounter> brokerToCounter = new HashMap<>();
        for (PartitionPlacement placement : newPlacements) {
            boolean isLeaderFound = false;
            for (int replica : placement.replicas) {
                LeaderAndReplicaCounter leaderAndReplicaCounter = brokerToCounter.computeIfAbsent(replica, k -> new LeaderAndReplicaCounter());
                leaderAndReplicaCounter.replicaCounter++;
                if (!isLeaderFound && !leaderDeprioritizedList.contains(replica)) {
                    leaderAndReplicaCounter.leaderCounter++;
                    isLeaderFound = true;
                }
            }
            // If all the replicas are deprioritized, then the first replica is considered as the leader
            if (!isLeaderFound) {
                int replica = placement.replicas.get(0);
                LeaderAndReplicaCounter leaderAndReplicaCounter = brokerToCounter.computeIfAbsent(replica, k -> new LeaderAndReplicaCounter());
                leaderAndReplicaCounter.leaderCounter++;
            }
        }
        for (Map.Entry<Integer, LeaderAndReplicaCounter> entry : brokerToCounter.entrySet()) {
            LOGGER.info("Broker: {}, numLeader: {}, numReplicas: {}", entry.getKey(), entry.getValue().leaderCounter, entry.getValue().replicaCounter);
        }
        LOGGER.info("Summary completed");
    }

    private static void shufflePreferredReplica(List<PartitionPlacement> newPlacements) {
        for (PartitionPlacement placement : newPlacements) {
            Collections.shuffle(placement.replicas);
        }
    }

    private static String generateFilename() {
        // Get the current timestamp
        Date now = new Date();

        // Format the timestamp to a string
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = dateFormat.format(now);

        // Create a filename using the formatted timestamp
        return "/tmp/replica_plan_" + timestamp + ".json";
    }

    private static Set<Integer> getBrokerIds(Config config, String configName) {
        Set<Integer> brokerIds = new HashSet<>();
        ConfigEntry configEntry = config.get(configName);
        if (configEntry != null && configEntry.value() != null && !configEntry.value().trim().isEmpty()) {
            String[] chunks = configEntry.value().split(":");
            for (String chunk : chunks) {
                if (!chunk.trim().isEmpty()) {
                    brokerIds.add(Integer.parseInt(chunk.trim()));
                }
            }
        }
        return brokerIds;
    }

    private static List<PartitionPlacement> getCurrentPlacements(Admin admin, List<String> topics)
            throws ExecutionException, InterruptedException {
        List<PartitionPlacement> currentPlacements = new ArrayList<>();
        Map<String, TopicDescription> descriptionMap = admin.describeTopics(topics).allTopicNames().get();
        for (TopicDescription description : descriptionMap.values()) {
            String topic = description.name();
            for (TopicPartitionInfo partitionInfo : description.partitions()) {
                TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());
                List<Integer> replicas = partitionInfo.replicas().stream().map(Node::id).collect(Collectors.toList());
                currentPlacements.add(new PartitionPlacement(partition, replicas));
            }
        }
        return currentPlacements;
    }

    private static void storeReplicaPlan(List<PartitionPlacement> placements, File file) throws IOException {
        List<PartitionInput> partitionInputs = new ArrayList<>();
        for (PartitionPlacement pp : placements) {
            PartitionInput pi = new PartitionInput(pp.partition.topic(), pp.partition.partition(), pp.replicas);
            partitionInputs.add(pi);
        }
        ReassignmentInput reassignmentInput = new ReassignmentInput(1, partitionInputs);
        String value = OBJECT_MAPPER.writeValueAsString(reassignmentInput);
        Files.write(file.toPath(), value.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }

    private static class PartitionPlacement {
        TopicPartition partition;
        List<Integer> replicas;

        private PartitionPlacement(TopicPartition partition, List<Integer> replicas) {
            this.partition = partition;
            this.replicas = replicas;
        }
    }

    private static class ReassignmentInput {
        int version;
        List<PartitionInput> partitions;

        private ReassignmentInput(int version, List<PartitionInput> partitions) {
            this.version = version;
            this.partitions = partitions;
        }
    }

    private static class PartitionInput {
        String topic;
        int partition;
        List<Integer> replicas;
        @JsonProperty("log_dirs")
        List<String> logDirs = new ArrayList<>();

        private PartitionInput(String topic, int partition, List<Integer> replicas) {
            this.topic = topic;
            this.partition = partition;
            this.replicas = replicas;
            replicas.forEach(replica -> logDirs.add("any"));
        }
    }

    private static class LeaderAndReplicaCounter {
        int leaderCounter;
        int replicaCounter;
    }
}
