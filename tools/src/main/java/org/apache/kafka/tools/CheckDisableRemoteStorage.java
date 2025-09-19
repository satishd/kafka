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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeLogDirsOptions;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CheckDisableRemoteStorage {

    private static final long USABLE_DISK_SPACE = 12L * 1024 * 1024 * 1024 * 1024; // 12 TB
    private static final double WARNING_DISK_USAGE_PERCENTAGE = 70.0; // 70%
    private static final double CRITICAL_DISK_USAGE_PERCENTAGE = 80.0; // 80%

    /**
     * To run the script:
     *  sh kafka-run-class.sh org.apache.kafka.tools.CheckDisableRemoteStorage <bootstrapServers> <topicsToDisableRemote> [multiplier]
     * @param args bootstrapServers topicsToDisableRemote [multiplier]
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = args[0];
        Set<String> topicsToDisableRemote = Arrays.stream(args[1].split(","))
                .collect(Collectors.toSet());
        double multiplier = args.length > 2 ? Double.parseDouble(args[2]) : 1.0;

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (Admin admin = Admin.create(props)) {
            List<Integer> brokerIds = getBrokerIds(admin);
            Map<ConfigResource, Config> topicConfigs = getTopicConfigs(admin);
            TopicValidationResult validationResult = validateTopics(topicsToDisableRemote, topicConfigs);

            DiskUsageResult diskUsageResult = calculateDiskUsage(admin, brokerIds, validationResult.eligibleTopics, multiplier);

            printResults(validationResult, diskUsageResult, multiplier);
        }
    }

    private static List<Integer> getBrokerIds(Admin admin) throws ExecutionException, InterruptedException {
        Collection<Node> nodes = admin.describeCluster().nodes().get();
        List<Integer> brokerIds = new ArrayList<>();
        for (Node node : nodes) {
            brokerIds.add(node.id());
        }
        Collections.sort(brokerIds);
        return brokerIds;
    }

    private static Map<ConfigResource, Config> getTopicConfigs(Admin admin) throws ExecutionException, InterruptedException {
        Set<String> topics = admin.listTopics().names().get();
        List<ConfigResource> configResources = new ArrayList<>();
        for (String topic : topics) {
            configResources.add(new ConfigResource(ConfigResource.Type.TOPIC, topic));
        }
        return admin.describeConfigs(configResources).all().get();
    }

    private static TopicValidationResult validateTopics(Set<String> topicsToDisableRemote, Map<ConfigResource, Config> topicConfigs) {
        Set<String> allTopics = topicConfigs.keySet().stream()
                .map(ConfigResource::name)
                .collect(Collectors.toSet());

        Set<String> eligibleTopics = new HashSet<>();
        List<String> invalidTopics = new ArrayList<>();

        for (String topic : topicsToDisableRemote) {
            if (!allTopics.contains(topic)) {
                invalidTopics.add(topic);
                continue;
            }
            Config config = topicConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, topic));
            ConfigEntry configEntry = config.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG);
            if (configEntry != null && configEntry.value().equals("true")) {
                eligibleTopics.add(topic);
            }
        }

        return new TopicValidationResult(eligibleTopics, invalidTopics);
    }

    private static DiskUsageResult calculateDiskUsage(Admin admin, List<Integer> brokerIds, Set<String> eligibleTopics, double multiplier) throws ExecutionException, InterruptedException {
        DescribeLogDirsOptions logDirsOptions = new DescribeLogDirsOptions();
        logDirsOptions.includeRemoteInfo(true);
        Map<Integer, Map<String, LogDirDescription>> logDirDescriptionByBroker =
                admin.describeLogDirs(brokerIds, logDirsOptions).allDescriptions().get();

        Map<Integer, Long> currentUsageByBroker = calculateCurrentDiskUsage(logDirDescriptionByBroker);
        Map<Integer, Long> newUsageByBroker = new HashMap<>(currentUsageByBroker);
        Set<Integer> brokersWithEligibleTopics = calculateNewDiskUsage(logDirDescriptionByBroker, eligibleTopics, newUsageByBroker, multiplier);

        return new DiskUsageResult(currentUsageByBroker, newUsageByBroker, brokersWithEligibleTopics);
    }

    private static Map<Integer, Long> calculateCurrentDiskUsage(Map<Integer, Map<String, LogDirDescription>> logDirDescriptionByBroker) {
        Map<Integer, Long> usedDiskSpaceByBroker = new HashMap<>();
        for (Map.Entry<Integer, Map<String, LogDirDescription>> entry : logDirDescriptionByBroker.entrySet()) {
            Integer brokerId = entry.getKey();
            Map<String, LogDirDescription> logDirsDescription = entry.getValue();
            // We don't use JBOD, so there will be only one log directory per broker
            for (LogDirDescription logDirDescription : logDirsDescription.values()) {
                long usedDiskSpace = logDirDescription.replicaInfos().values().stream()
                        .mapToLong(ReplicaInfo::size)
                        .sum();
                usedDiskSpaceByBroker.merge(brokerId, usedDiskSpace, Long::sum);
            }
        }
        return usedDiskSpaceByBroker;
    }

    private static Set<Integer> calculateNewDiskUsage(Map<Integer, Map<String, LogDirDescription>> logDirDescriptionByBroker,
                                                      Set<String> eligibleTopics,
                                                      Map<Integer, Long> usedDiskSpaceByBroker,
                                                      double multiplier) {
        Set<Integer> brokersWithEligibleTopics = new HashSet<>();
        for (Map.Entry<Integer, Map<String, LogDirDescription>> entry : logDirDescriptionByBroker.entrySet()) {
            Integer brokerId = entry.getKey();
            Map<String, LogDirDescription> logDirsDescription = entry.getValue();
            // We don't use JBOD, so there will be only one log directory per broker
            for (LogDirDescription logDirDescription : logDirsDescription.values()) {
                Map<TopicPartition, ReplicaInfo> replicaInfoByPartition = logDirDescription.replicaInfos();
                for (Map.Entry<TopicPartition, ReplicaInfo> replicaInfoEntry : replicaInfoByPartition.entrySet()) {
                    TopicPartition partition = replicaInfoEntry.getKey();
                    if (eligibleTopics.contains(partition.topic())) {
                        brokersWithEligibleTopics.add(brokerId);
                        ReplicaInfo replicaInfo = replicaInfoEntry.getValue();
                        long commonSegmentsSizeInBothLocalAndRemote = replicaInfo.size() - replicaInfo.onlyLocalLogSize();
                        long increaseDiskUsage = replicaInfo.remoteLogSize() - commonSegmentsSizeInBothLocalAndRemote;
                        long adjustedIncreaseDiskUsage = (long) (increaseDiskUsage * multiplier);
                        usedDiskSpaceByBroker.merge(brokerId, adjustedIncreaseDiskUsage, Long::sum);
                    }
                }
            }
        }
        return brokersWithEligibleTopics;
    }

    private static void printResults(TopicValidationResult validationResult, DiskUsageResult diskUsageResult, double multiplier) {
        System.out.println("=== CheckDisableRemoteStorage Analysis ===");
        System.out.println("Total disk space: " + (USABLE_DISK_SPACE / (1024L * 1024 * 1024 * 1024)) + " TB");
        System.out.println("Multiplier: " + multiplier);
        System.out.println();

        printInvalidTopics(validationResult.invalidTopics);
        printEligibleTopics(validationResult.eligibleTopics);
        printBrokerUsageTable(diskUsageResult);
    }

    private static void printInvalidTopics(List<String> invalidTopics) {
        if (!invalidTopics.isEmpty()) {
            System.out.println("Invalid topics (do not exist):");
            for (String topic : invalidTopics) {
                System.out.println("  - " + topic);
            }
            System.out.println();
        }
    }

    private static void printEligibleTopics(Set<String> eligibleTopics) {
        System.out.println("Topics eligible for remote storage disabling:");
        if (eligibleTopics.isEmpty()) {
            System.out.println("  No eligible topics found (none have remote storage enabled)");
        } else {
            for (String topic : eligibleTopics) {
                System.out.println("  - " + topic);
            }
        }
        System.out.println();
    }

    private static void printBrokerUsageTable(DiskUsageResult diskUsageResult) {
        System.out.println("Broker   Current  New      Change   Status");
        System.out.println("ID       Usage    Usage    (GB)");
        System.out.println("-------------------------------------------------------");

        List<Integer> sortedBrokers = new ArrayList<>(diskUsageResult.brokersWithEligibleTopics);
        sortedBrokers.sort((b1, b2) ->
                Long.compare(diskUsageResult.newUsageByBroker.get(b2), diskUsageResult.newUsageByBroker.get(b1)));

        if (sortedBrokers.isEmpty()) {
            System.out.println("No brokers found with eligible topics.");
        } else {
            for (Integer brokerId : sortedBrokers) {
                printBrokerUsageRow(brokerId, diskUsageResult);
            }
        }
    }

    private static void printBrokerUsageRow(Integer brokerId, DiskUsageResult diskUsageResult) {
        long currentUsage = diskUsageResult.currentUsageByBroker.get(brokerId);
        long newUsage = diskUsageResult.newUsageByBroker.get(brokerId);
        long remoteDataToAdd = newUsage - currentUsage;

        double remoteDataToAddGB = remoteDataToAdd / (1024.0 * 1024.0 * 1024.0);
        double currentUsagePercent = (currentUsage * 100.0) / USABLE_DISK_SPACE;
        double newUsagePercent = (newUsage * 100.0) / USABLE_DISK_SPACE;

        String status = determineStatus(newUsagePercent);

        System.out.printf("%-8d %5.1f%%   %5.1f%%   %6.1fGB  %s%n",
                brokerId,
                currentUsagePercent,
                newUsagePercent,
                remoteDataToAddGB,
                status);
    }

    private static String determineStatus(double newUsagePercent) {
        if (newUsagePercent >= CRITICAL_DISK_USAGE_PERCENTAGE) {
            return "CRITICAL";
        } else if (newUsagePercent >= WARNING_DISK_USAGE_PERCENTAGE) {
            return "WARNING";
        } else {
            return "OK";
        }
    }

    private static class TopicValidationResult {
        final Set<String> eligibleTopics;
        final List<String> invalidTopics;

        TopicValidationResult(Set<String> eligibleTopics, List<String> invalidTopics) {
            this.eligibleTopics = eligibleTopics;
            this.invalidTopics = invalidTopics;
        }
    }

    private static class DiskUsageResult {
        final Map<Integer, Long> currentUsageByBroker;
        final Map<Integer, Long> newUsageByBroker;
        final Set<Integer> brokersWithEligibleTopics;

        DiskUsageResult(Map<Integer, Long> currentUsageByBroker,
                        Map<Integer, Long> newUsageByBroker,
                        Set<Integer> brokersWithEligibleTopics) {
            this.currentUsageByBroker = currentUsageByBroker;
            this.newUsageByBroker = newUsageByBroker;
            this.brokersWithEligibleTopics = brokersWithEligibleTopics;
        }
    }
}