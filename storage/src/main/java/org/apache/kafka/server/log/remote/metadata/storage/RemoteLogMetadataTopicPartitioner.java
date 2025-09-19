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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME;

public class RemoteLogMetadataTopicPartitioner {
    public static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataTopicPartitioner.class);
    private final int numMetadataTopicPartitions;

    public RemoteLogMetadataTopicPartitioner(int numMetadataTopicPartitions) {
        this.numMetadataTopicPartitions = numMetadataTopicPartitions;
    }

    public int metadataPartition(TopicIdPartition topicIdPartition) {
        Objects.requireNonNull(topicIdPartition, "TopicPartition can not be null");

        int partitionNum = Utils.toPositive(Utils.murmur2(toBytes(topicIdPartition))) % numMetadataTopicPartitions;
        log.debug("No of partitions [{}], partitionNum: [{}] for given topic: [{}]", numMetadataTopicPartitions, partitionNum, topicIdPartition);
        return partitionNum;
    }

    private byte[] toBytes(TopicIdPartition topicIdPartition) {
        // We do not want to depend upon hash code generation of Uuid as that may change.
        int hash = Objects.hash(topicIdPartition.topicId().getLeastSignificantBits(),
                                topicIdPartition.topicId().getMostSignificantBits(),
                                topicIdPartition.partition());

        return toBytes(hash);
    }

    private byte[] toBytes(int n) {
        return new byte[]{
            (byte) (n >> 24),
            (byte) (n >> 16),
            (byte) (n >> 8),
            (byte) n
        };
    }

    /**
     * To run the script:
     *   sh kafka-run-class.sh org.apache.kafka.server.log.remote.metadata.storage.RemoteLogMetadataTopicPartitioner <bootstrapServer> <topics>
     * @param args bootstrapServer topics
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if (args.length < 2) {
            System.err.println("Two parameters are required: <bootstrapServer> <topics>");
            return;
        }
        String bootstrapServer = args[0];
        Set<String> topics = Arrays.stream(args[1].split(",")).collect(Collectors.toSet());
        topics.add(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME);

        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        Map<String, TopicDescription> topicDescriptionMap = new HashMap<>();
        try (Admin admin = Admin.create(adminProps)) {
            DescribeTopicsResult describeTopicsResult = admin.describeTopics(topics);
            for (String topic : topics) {
                try {
                    TopicDescription topicDescription = describeTopicsResult.topicNameValues().get(topic).get();
                    topicDescriptionMap.put(topic, topicDescription);
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                        System.out.println("Topic: " + topic + " does not exist");
                    } else {
                        throw ex;
                    }
                }
            }
        }

        if (!topicDescriptionMap.isEmpty()) {
            int metadataTopicPartitionCount = topicDescriptionMap.get(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME)
                    .partitions().size();
            RemoteLogMetadataTopicPartitioner partitioner = new RemoteLogMetadataTopicPartitioner(metadataTopicPartitionCount);
            System.out.println("The number of partitions in " + TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME + " topic is " + metadataTopicPartitionCount);
            topicDescriptionMap.forEach((topic, description) -> {
                if (!REMOTE_LOG_METADATA_TOPIC_NAME.equals(topic)) {
                    for (TopicPartitionInfo partitionInfo : description.partitions()) {
                        TopicIdPartition tpId = new TopicIdPartition(description.topicId(), partitionInfo.partition(), topic);
                        int metadataPartition = partitioner.metadataPartition(tpId);
                        System.out.println("The __remote_log_metadata partition for " + tpId + " is " + metadataPartition);
                    }
                }
            });
        }
    }
}
