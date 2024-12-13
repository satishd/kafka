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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TopicRetentionChanger {

    private static final long LOCAL_RETENTION_MS = 12 * 60 * 60 * 1000;
    private static final long LOCAL_RETENTION_BYTES = 300 * 1024 * 1024 * 1024L;
    private static final long RETENTION_BYTES = 300 * 1024 * 1024 * 1024L;

    private static void changeConfig(Admin admin,
                                     Set<String> topics,
                                     boolean isDryRun) throws ExecutionException, InterruptedException {
        List<AlterConfigOp> configOps = Arrays.asList(
                new AlterConfigOp(new ConfigEntry("local.retention.ms", String.valueOf(LOCAL_RETENTION_MS)), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("local.retention.bytes", String.valueOf(LOCAL_RETENTION_BYTES)), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("retention.bytes", String.valueOf(RETENTION_BYTES)), AlterConfigOp.OpType.SET)
        );
        Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = new HashMap<>();
        for (String topic : topics) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            alterConfigs.put(resource, configOps);
        }
        if (!isDryRun) {
            admin.incrementalAlterConfigs(alterConfigs).all().get();
            System.out.println("Updated the configs for " + alterConfigs.size() + " topics");
        } else {
            System.out.println("Dry run mode enabled. No changes will be made. AlterConfigs size: " + alterConfigs.size());
        }
    }

    private static void verifyConfig(Admin admin,
                                     Set<String> topics) throws ExecutionException, InterruptedException {
        List<ConfigResource> resources = new ArrayList<>();
        for (String topic : topics) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            resources.add(resource);
        }
        Map<ConfigResource, Config> resourceConfigMap = admin.describeConfigs(resources).all().get();
        for (Map.Entry<ConfigResource, Config> entry : resourceConfigMap.entrySet()) {
            ConfigResource resource = entry.getKey();
            Config config = entry.getValue();
            for (ConfigEntry configEntry : config.entries()) {
                if (configEntry.name().equals("local.retention.ms") && !configEntry.value().equals(String.valueOf(LOCAL_RETENTION_MS))) {
                    System.out.println("Topic " + resource.name() + " does not have a local retention time of 12 hours");
                }
                if (configEntry.name().equals("local.retention.bytes") && !configEntry.value().equals(String.valueOf(LOCAL_RETENTION_BYTES))) {
                    System.out.println("Topic " + resource.name() + " does not have a local retention bytes of 300 GB");
                }
                if (configEntry.name().equals("retention.bytes") && !configEntry.value().equals(String.valueOf(RETENTION_BYTES))) {
                    System.out.println("Topic " + resource.name() + " does not have a retention bytes of 300 GB");
                }
            }
        }
    }

    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String topicsFile = args[1];
        boolean isDryRun = Boolean.parseBoolean(args[2]);

        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000");
        try (Admin client = Admin.create(props)) {
            Set<String> validTopics = client.listTopics().names().get();
            Set<String> topics = readTopics(new File(topicsFile), validTopics);
            if (topics.isEmpty()) {
                System.out.println("No valid topics to update");
                return;
            }

            System.out.println("Updating the topic configs for " + topics.size() + " topics");
            changeConfig(client, topics, isDryRun);

            Thread.sleep(2000);
            System.out.println("Validating the topic configs for " + topics.size() + " topics");
            verifyConfig(client, topics);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Set<String> readTopics(File file, Set<String> validTopics) {
        Set<String> topics = new HashSet<>();
        try {
            List<String> lines = Files.readAllLines(file.toPath(), StandardCharsets.UTF_8);
            for (String line : lines) {
                String topic = line.trim();
                if (validTopics.contains(topic)) {
                    topics.add(topic);
                } else {
                    System.out.println("Topic " + topic + " does not exist!");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return topics;
    }
}