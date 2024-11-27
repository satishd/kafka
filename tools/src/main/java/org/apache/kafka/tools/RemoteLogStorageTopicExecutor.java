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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.stream.Collectors;

public abstract class RemoteLogStorageTopicExecutor {

    final Logger log = LoggerFactory.getLogger(getClass());
    final String bootstrapServer;
    final int batchSize;
    final int batchIntervalMs;
    final int requestTimeoutMs;
    Set<String> topicNames;

    public RemoteLogStorageTopicExecutor(Namespace namespace) throws IOException {
        this.bootstrapServer = namespace.getString("bootstrap_server");
        this.batchSize = namespace.getInt("batch_size");
        this.batchIntervalMs = namespace.getInt("batch_interval");
        this.requestTimeoutMs = namespace.getInt("request_timeout");

        File topicFile = namespace.get("topic_file");
        if (topicFile != null) {
            topicNames = new HashSet<>(Files.readAllLines(topicFile.toPath(), StandardCharsets.UTF_8));
        }
    }

    void execute() {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);

        try (AdminClient client = AdminClient.create(props)) {
            if (topicNames == null) {
                topicNames = client.listTopics().names().get();
            }
            Arrays.asList("__remote_log_metadata", "__consumer_offsets", "__transaction_state")
                    .forEach(topicNames::remove);
            log.info("Total number of topics excluding the internal ones: {}", topicNames.size());

            Collection<ConfigResource> resources = topicNames.stream()
                    .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
                    .collect(Collectors.toList());
            Map<ConfigResource, Config> configMap = client.describeConfigs(resources).all().get();
            log.info("Fetched all the topic configs: {}", configMap.size());

            List<String> eligibleTopics = new ArrayList<>();
            List<String> ineligibleTopics = new ArrayList<>();
            for (String topic: topicNames) {
                Config config = configMap.get(new ConfigResource(ConfigResource.Type.TOPIC, topic));
                if (config != null) {
                    ConfigEntry cleanupPolicyConfig = config.get(TopicConfig.CLEANUP_POLICY_CONFIG);
                    ConfigEntry remoteStorageConfig = config.get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG);
                    if (isEligibleTopic(cleanupPolicyConfig, remoteStorageConfig)) {
                        eligibleTopics.add(topic);
                    } else {
                        ineligibleTopics.add(topic);
                    }
                } else {
                    log.warn("Ignoring the invalid topic: {}", topic);
                }
            }
            log.info("Ineligible topics size: {}", ineligibleTopics.size());

            int size = eligibleTopics.size();
            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = new HashMap<>();
            Collection<AlterConfigOp> configOps = alterConfigOps();
            for (int idx = 1; idx <= size; idx++) {
                String topic = eligibleTopics.get(idx - 1);
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                alterConfigs.put(configResource, configOps);
                log.info("Altering the topic config for: {}", topic);
                if (idx % batchSize == 0) {
                    executeAlterCommand(client, alterConfigs);
                    log.info("Sleeping for {} seconds. Completed: {}/{}", batchIntervalMs / 1000, idx, size);
                    Thread.sleep(batchIntervalMs);
                }
            }
            executeAlterCommand(client, alterConfigs);
            log.info("Updated the configs for {} topics", eligibleTopics.size());
        } catch (ExecutionException | InterruptedException e) {
            log.error("Error while updating the topic configs", e);
        }
    }

    abstract boolean isEligibleTopic(ConfigEntry cleanupPolicyConfig, ConfigEntry remoteStorageConfig);

    abstract Collection<AlterConfigOp> alterConfigOps();

    private void executeAlterCommand(AdminClient client, Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs)
            throws InterruptedException, ExecutionException {
        try {
            if (!alterConfigs.isEmpty()) {
                client.incrementalAlterConfigs(alterConfigs).all().get();
                alterConfigs.clear();
            }
        } catch (KafkaException ex) {
            // Handle topic deletion in middle of config change
            List<String> names = alterConfigs.keySet().stream().map(ConfigResource::name).collect(Collectors.toList());
            log.error("Not able to incrementally alter the configs for topics: {}", names, ex);
        }
    }

    @Override
    public String toString() {
        return "RemoteLogStorageTopicExecutor{" +
                "bootstrapServer='" + bootstrapServer + '\'' +
                ", batchSize=" + batchSize +
                ", batchIntervalMs=" + batchIntervalMs +
                ", requestTimeoutMs=" + requestTimeoutMs +
                '}';
    }

    static Namespace parseArguments(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("remote-log-storage-topic-executor")
                .defaultHelp(true)
                .description("Driver to enable/disable topics to the remote storage");
        parser.addArgument("--bootstrap-server")
                .required(true)
                .help("REQUIRED: A comma separated host:port list for establishing the connection to the kafka cluster.");
        parser.addArgument("--batch-size")
                .type(Integer.class)
                .required(false)
                .setDefault(100)
                .help("Number of topics to update the configs in one batch");
        parser.addArgument("--batch-interval")
                .type(Integer.class)
                .required(false)
                .setDefault(900_000)
                .help("The amount of time to wait before executing the next batch. Timeout value is in ms");
        parser.addArgument("--request-timeout")
                .type(Integer.class)
                .required(false)
                .setDefault(120_000)
                .help("Admin client request timeout in ms");
        parser.addArgument("--topic-file")
                .type(Arguments.fileType().verifyCanRead())
                .required(false)
                .help("List of topic names in a file");
        return parser.parseArgsOrFail(args);
    }
}
