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

    public static final String SCHEMA_CLIENT_CLASS_NAME_CONFIG = "schema.client.class.name";
    private static final String SCHEMA_CLIENT_CLASS_NAME_DOC =
            "Fully qualified class name of the SchemaClient implementation used to resolve Heatpipe "
                    + "writer schemas (expected to be backed by Uber's Schema Service). Leave empty to run "
                    + "discovery and fetch only, without decoding or writing to Hudi.";

    /** Prefix for keys forwarded (with the prefix stripped) to {@code SchemaClient.configure}. */
    public static final String SCHEMA_CLIENT_CONFIG_PREFIX = "schema.client.config.";

    public static final String HUDI_TABLE_BASE_PATH_CONFIG = "hudi.table.base.path";
    private static final String HUDI_TABLE_BASE_PATH_DOC =
            "Base path of the Hudi table, e.g. cfs://ns-cloudlake/... or oci://bucket@namespace/prefix. "
                    + "The URI scheme is opaque to this module.";

    public static final String HUDI_TABLE_NAME_CONFIG = "hudi.table.name";
    private static final String HUDI_TABLE_NAME_DOC = "Hudi table name.";

    public static final String HUDI_RECORD_KEY_FIELD_CONFIG = "hudi.record.key.field";
    private static final String HUDI_RECORD_KEY_FIELD_DOC =
            "Name of the decoded record field used to derive the Hudi record key.";

    public static final String HUDI_PARTITION_PATH_FIELD_CONFIG = "hudi.partition.path.field";
    private static final String HUDI_PARTITION_PATH_FIELD_DOC =
            "Name of the decoded record field used to derive the Hudi partition path.";

    public static final String DEADLETTER_PATH_CONFIG = "deadletter.path";
    private static final String DEADLETTER_PATH_DOC =
            "Local (or mounted) path that undecodable records are appended to.";

    public static final String MAX_CONCURRENT_SEGMENTS_CONFIG = "max.concurrent.segments";
    private static final int MAX_CONCURRENT_SEGMENTS_DEFAULT = 4;
    private static final String MAX_CONCURRENT_SEGMENTS_DOC =
            "Maximum number of segments fetched, decoded, and written to Hudi concurrently.";

    public static final String READ_BLOCK_BYTES_CONFIG = "read.block.bytes";
    public static final int READ_BLOCK_BYTES_DEFAULT = 4 * 1024 * 1024;
    private static final String READ_BLOCK_BYTES_DOC =
            "Size in bytes of the block read buffer used when streaming a segment from remote storage. "
                    + "Segments are read and parsed one record batch at a time rather than loaded whole, so "
                    + "peak memory per in-flight segment is bounded by this block size plus the largest record "
                    + "batch. Larger blocks reduce the number of reads against the object store at the cost of "
                    + "more memory per concurrent segment.";

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
                    ConfigDef.Importance.LOW, RSM_CLASS_PATH_DOC)
            .define(SCHEMA_CLIENT_CLASS_NAME_CONFIG, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.HIGH, SCHEMA_CLIENT_CLASS_NAME_DOC)
            .define(HUDI_TABLE_BASE_PATH_CONFIG, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.HIGH, HUDI_TABLE_BASE_PATH_DOC)
            .define(HUDI_TABLE_NAME_CONFIG, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.HIGH, HUDI_TABLE_NAME_DOC)
            .define(HUDI_RECORD_KEY_FIELD_CONFIG, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.MEDIUM, HUDI_RECORD_KEY_FIELD_DOC)
            .define(HUDI_PARTITION_PATH_FIELD_CONFIG, ConfigDef.Type.STRING, "",
                    ConfigDef.Importance.MEDIUM, HUDI_PARTITION_PATH_FIELD_DOC)
            .define(DEADLETTER_PATH_CONFIG, ConfigDef.Type.STRING, "segment-lake-ingestion-dead-letters.tsv",
                    ConfigDef.Importance.MEDIUM, DEADLETTER_PATH_DOC)
            .define(MAX_CONCURRENT_SEGMENTS_CONFIG, ConfigDef.Type.INT, MAX_CONCURRENT_SEGMENTS_DEFAULT,
                    ConfigDef.Importance.LOW, MAX_CONCURRENT_SEGMENTS_DOC)
            .define(READ_BLOCK_BYTES_CONFIG, ConfigDef.Type.INT, READ_BLOCK_BYTES_DEFAULT,
                    ConfigDef.Range.atLeast(1), ConfigDef.Importance.LOW, READ_BLOCK_BYTES_DOC);

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

    public String schemaClientClassName() {
        return getString(SCHEMA_CLIENT_CLASS_NAME_CONFIG);
    }

    /**
     * @return SchemaClient-specific settings (keys under {@link #SCHEMA_CLIENT_CONFIG_PREFIX}, prefix
     *         stripped) to hand to {@code SchemaClient.configure}, if it implements {@code Configurable}.
     */
    public Map<String, Object> schemaClientConfigs() {
        return originalsWithPrefix(SCHEMA_CLIENT_CONFIG_PREFIX);
    }

    public String hudiTableBasePath() {
        return getString(HUDI_TABLE_BASE_PATH_CONFIG);
    }

    public String hudiTableName() {
        return getString(HUDI_TABLE_NAME_CONFIG);
    }

    public String hudiRecordKeyField() {
        return getString(HUDI_RECORD_KEY_FIELD_CONFIG);
    }

    public String hudiPartitionPathField() {
        return getString(HUDI_PARTITION_PATH_FIELD_CONFIG);
    }

    public String deadLetterPath() {
        return getString(DEADLETTER_PATH_CONFIG);
    }

    public int maxConcurrentSegments() {
        return getInt(MAX_CONCURRENT_SEGMENTS_CONFIG);
    }

    public int readBlockBytes() {
        return getInt(READ_BLOCK_BYTES_CONFIG);
    }

    /**
     * @return true once every setting required to run the full decode-and-write pipeline is present;
     *         otherwise the worker falls back to discovery (and, if an RSM is set, fetch) only.
     */
    public boolean decodeAndWriteEnabled() {
        return !rsmClassName().trim().isEmpty()
                && !schemaClientClassName().trim().isEmpty()
                && !hudiTableBasePath().trim().isEmpty()
                && !hudiTableName().trim().isEmpty()
                && !hudiRecordKeyField().trim().isEmpty()
                && !hudiPartitionPathField().trim().isEmpty();
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
