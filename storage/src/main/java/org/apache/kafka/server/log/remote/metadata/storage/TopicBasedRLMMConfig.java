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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

public class TopicBasedRLMMConfig {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRLMMConfig.class.getName());

    public static final String REMOTE_LOG_METADATA_TOPIC_NAME = "__remote_log_metadata";
    public static final String REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP =
            "remote.log.metadata.topic.replication.factor";
    public static final String REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP = "remote.log.metadata.topic.num.partitions";
    public static final String REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP =
            "remote.log.metadata.topic.retention.ms";

    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_PARTITIONS = 50;
    public static final long DEFAULT_REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS = -1L;
    public static final int DEFAULT_REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR = 3;

    public static final String REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX = "remote.log.metadata.common.client.";
    public static final String REMOTE_LOG_METADATA_PRODUCER_PREFIX = "remote.log.metadata.producer.";
    public static final String REMOTE_LOG_METADATA_CONSUMER_PREFIX = "remote.log.metadata.consumer.";

    private static final String REMOTE_LOG_METADATA_CLIENT_PREFIX = "__remote_log_metadata_client";
    private static final String BROKER_ID = "broker.id";

    private static final String[] MANDATORY_PROPS = {REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP,
        REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP, BROKER_ID};

    private final String clientIdPrefix;
    private final int metadataTopicPartitionsCount;
    private final String bootstrapServers;
    private Map<String, Object> consumerProps;
    private Map<String, Object> producerProps;

    public TopicBasedRLMMConfig(Map<String, ?> props) {
        log.info("Received props: [{}]", props);

        ensureMandatoryProps(props);

        // `bootstrap.servers` is mandatory as producer and consumer need to connect to the local broker.
        bootstrapServers = props.get(BOOTSTRAP_SERVERS_CONFIG).toString();

        metadataTopicPartitionsCount =
                Integer.parseInt(props.get(TopicBasedRLMMConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP).toString());

        clientIdPrefix = REMOTE_LOG_METADATA_CLIENT_PREFIX + "_" + props.get(BROKER_ID) + "_" + props.get("broker.epoch");

        initializeProducerConsumerProperties(props);
    }

    private void ensureMandatoryProps(Map<String, ?> configs) {
        for (String name : MANDATORY_PROPS) {
            if (configs.get(name) == null) {
                throw new InvalidConfigurationException("Mandatory property with name: " + name + " does not exist in the given configs");
            }
        }
    }

    private void initializeProducerConsumerProperties(Map<String, ?> configs) {
        Map<String, Object> commonClientConfigs = new HashMap<>();
        commonClientConfigs.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        Map<String, Object> producerOnlyConfigs = new HashMap<>();
        Map<String, Object> consumerOnlyConfigs = new HashMap<>();

        for (Map.Entry<String, ?> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX)) {
                commonClientConfigs.put(key.substring(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX.length()), entry.getValue());
            } else if (key.startsWith(REMOTE_LOG_METADATA_PRODUCER_PREFIX)) {
                producerOnlyConfigs.put(key.substring(REMOTE_LOG_METADATA_PRODUCER_PREFIX.length()), entry.getValue());
            } else if (key.startsWith(REMOTE_LOG_METADATA_CONSUMER_PREFIX)) {
                consumerOnlyConfigs.put(key.substring(REMOTE_LOG_METADATA_CONSUMER_PREFIX.length()), entry.getValue());
            }
        }

        HashMap<String, Object> allProducerConfigs = new HashMap<>(commonClientConfigs);
        allProducerConfigs.putAll(producerOnlyConfigs);
        producerProps = createProducerProps(allProducerConfigs);

        HashMap<String, Object> allConsumerConfigs = new HashMap<>(commonClientConfigs);
        allConsumerConfigs.putAll(consumerOnlyConfigs);
        consumerProps = createConsumerProps(allConsumerConfigs);
    }

    public String remoteLogMetadataTopicName() {
        return REMOTE_LOG_METADATA_TOPIC_NAME;
    }

    public int metadataTopicPartitionsCount() {
        return metadataTopicPartitionsCount;
    }

    public Map<String, Object> consumerProperties() {
        return consumerProps;
    }

    public Map<String, Object> producerProperties() {
        return producerProps;
    }

    private Map<String, Object> createConsumerProps(HashMap<String, Object> allConsumerConfigs) {
        Map<String, Object> props = new HashMap<>(allConsumerConfigs);

        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientIdPrefix + "_consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
    }

    private Map<String, Object> createProducerProps(HashMap<String, Object> allProducerConfigs) {
        Map<String, Object> props = new HashMap<>(allProducerConfigs);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "_producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return Collections.unmodifiableMap(props);
    }

    @Override
    public String toString() {
        return "TopicBasedRLMMConfig{" +
                "clientIdPrefix='" + clientIdPrefix + '\'' +
                ", metadataTopicPartitionsCount=" + metadataTopicPartitionsCount +
                ", consumerProps=" + consumerProps +
                ", producerProps=" + producerProps +
                '}';
    }
}