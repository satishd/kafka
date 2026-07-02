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
package org.apache.kafka.lake.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration for the segment-lake converter worker.
 *
 * <p>This first stage only needs the settings required to discover finished remote log segments
 * from the {@code __remote_log_metadata} topic. Storage, decoding and Hudi settings are added by
 * later commits.
 */
public class ConverterConfig extends AbstractConfig {

    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    private static final String BOOTSTRAP_SERVERS_DOC =
            "Bootstrap servers of the Kafka cluster hosting the remote log metadata topic.";

    public static final String METADATA_TOPIC_CONFIG = "remote.log.metadata.topic.name";
    public static final String METADATA_TOPIC_DEFAULT = "__remote_log_metadata";
    private static final String METADATA_TOPIC_DOC =
            "Name of the internal remote log metadata topic to consume segment metadata from.";

    public static final String GROUP_ID_CONFIG = "group.id";
    public static final String GROUP_ID_DEFAULT = "segment-lake-ingestion";
    private static final String GROUP_ID_DOC = "Consumer group id used to read the metadata topic.";

    public static final String POLL_TIMEOUT_MS_CONFIG = "poll.timeout.ms";
    public static final long POLL_TIMEOUT_MS_DEFAULT = 1000L;
    private static final String POLL_TIMEOUT_MS_DOC = "Timeout in milliseconds for each metadata consumer poll.";

    public static final String MAX_POLL_RECORDS_CONFIG = "max.poll.records";
    public static final int MAX_POLL_RECORDS_DEFAULT = 500;
    private static final String MAX_POLL_RECORDS_DOC = "Maximum number of metadata records fetched per poll.";

    public static final String TOPICS_ALLOWLIST_CONFIG = "topics.allowlist";
    private static final String TOPICS_ALLOWLIST_DOC =
            "Comma separated list of topics to ingest. Empty means all topics are considered.";

    public static final String RSM_CLASS_NAME_CONFIG = "remote.storage.manager.class.name";
    private static final String RSM_CLASS_NAME_DOC =
            "Fully qualified class name of the RemoteStorageManager implementation used to fetch segments. "
                    + "Leave empty to run discovery only.";

    public static final String RSM_CLASS_PATH_CONFIG = "remote.storage.manager.class.path";
    private static final String RSM_CLASS_PATH_DOC =
            "Optional classpath from which to load the RemoteStorageManager in a child-first class loader.";

    /** Prefix for keys forwarded (with the prefix stripped) to {@code RemoteStorageManager.configure}. */
    public static final String RSM_CONFIG_PREFIX = "rsm.config.";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, BOOTSTRAP_SERVERS_DOC)
            .define(METADATA_TOPIC_CONFIG, ConfigDef.Type.STRING, METADATA_TOPIC_DEFAULT,
                    ConfigDef.Importance.LOW, METADATA_TOPIC_DOC)
            .define(GROUP_ID_CONFIG, ConfigDef.Type.STRING, GROUP_ID_DEFAULT,
                    ConfigDef.Importance.MEDIUM, GROUP_ID_DOC)
            .define(POLL_TIMEOUT_MS_CONFIG, ConfigDef.Type.LONG, POLL_TIMEOUT_MS_DEFAULT,
                    ConfigDef.Importance.LOW, POLL_TIMEOUT_MS_DOC)
            .define(MAX_POLL_RECORDS_CONFIG, ConfigDef.Type.INT, MAX_POLL_RECORDS_DEFAULT,
                    ConfigDef.Importance.LOW, MAX_POLL_RECORDS_DOC)
            .define(TOPICS_ALLOWLIST_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(),
                    ConfigDef.Importance.MEDIUM, TOPICS_ALLOWLIST_DOC)
            .define(RSM_CLASS_NAME_CONFIG, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.HIGH, RSM_CLASS_NAME_DOC)
            .define(RSM_CLASS_PATH_CONFIG, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.LOW, RSM_CLASS_PATH_DOC);

    public ConverterConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
    }

    public String metadataTopic() {
        return getString(METADATA_TOPIC_CONFIG);
    }

    public Duration pollTimeout() {
        return Duration.ofMillis(getLong(POLL_TIMEOUT_MS_CONFIG));
    }

    public List<String> topicsAllowlist() {
        return getList(TOPICS_ALLOWLIST_CONFIG);
    }

    public String rsmClassName() {
        return getString(RSM_CLASS_NAME_CONFIG);
    }

    public String rsmClassPath() {
        return getString(RSM_CLASS_PATH_CONFIG);
    }

    /**
     * @return RSM-specific settings (keys under {@link #RSM_CONFIG_PREFIX}, prefix stripped) to hand to
     *         {@code RemoteStorageManager.configure}.
     */
    public Map<String, Object> rsmConfigs() {
        return originalsWithPrefix(RSM_CONFIG_PREFIX);
    }

    /**
     * @return consumer properties for reading the remote log metadata topic. Values are raw bytes;
     *         deserialization into metadata records is done by the caller via {@code RemoteLogMetadataSerde}.
     */
    public Properties consumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getString(BOOTSTRAP_SERVERS_CONFIG));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, getString(GROUP_ID_CONFIG));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getInt(MAX_POLL_RECORDS_CONFIG));
        return props;
    }
}
