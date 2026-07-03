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
package org.apache.kafka.lake.write;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Writes a segment's decoded records as Hudi commits on an object-store-backed table (OCS via
 * {@code cfs://} or {@code oci://}, config-driven — see {@link HudiWriterConfig}).
 *
 * <p>Append-only: every record is a new row (no precombine/dedup), so writes always use
 * {@code insert}. {@code bulkInsert} is not supported by {@code HoodieJavaWriteClient} (the
 * Spark-free Java engine) as of Hudi 0.12.3 &mdash; only the Spark client supports it. Record key
 * and partition path are derived from the decoded record via caller-supplied functions (typically
 * {@code topic-partition-offset} and a date partition, per the design doc), keeping this class
 * independent of any particular topic's schema.
 *
 * <p>The Hudi table is initialized on first use if it does not already exist. A
 * {@link HoodieJavaWriteClient} is cached and reused per Avro schema across segments: constructing a
 * client reads the timeline and initialises engine state, which is wasteful to repeat per small
 * segment. {@code HoodieJavaWriteClient} is a <b>single-writer</b> engine and this class is
 * <b>not</b> thread-safe — it is designed to be owned and driven by a single commit thread (see the
 * pipeline's single-writer commit stage). One worker instance owns a given table.
 */
public class HudiSegmentWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HudiSegmentWriter.class);

    private final HudiWriterConfig config;
    private final Configuration hadoopConf;
    private final Function<GenericRecord, String> recordKey;
    private final Function<GenericRecord, String> partitionPath;
    private final HoodieJavaEngineContext engineContext;
    private final Map<String, HoodieJavaWriteClient<HoodieAvroPayload>> clientsBySchema = new HashMap<>();
    private boolean tableInitialized;

    public HudiSegmentWriter(HudiWriterConfig config, Configuration hadoopConf,
                              Function<GenericRecord, String> recordKey,
                              Function<GenericRecord, String> partitionPath) {
        this.config = config;
        this.hadoopConf = hadoopConf;
        this.recordKey = recordKey;
        this.partitionPath = partitionPath;
        this.engineContext = new HoodieJavaEngineContext(hadoopConf);
    }

    /**
     * Write {@code records} as one Hudi commit, tagged with {@code commitExtraMetadata} (the ingested
     * offset range — see {@code OffsetTracker}). The write client for {@code schema} is created on
     * first use and reused for subsequent calls.
     *
     * @param records             decoded inner records for one segment; must share {@code schema}.
     * @param schema              the records' Avro schema.
     * @param commitExtraMetadata metadata to attach to the Hudi commit instant.
     * @return the commit instant time, or empty if {@code records} was empty (no commit is made).
     */
    public Option<String> write(List<GenericRecord> records, Schema schema,
                                 Map<String, String> commitExtraMetadata) {
        if (records.isEmpty()) {
            return Option.empty();
        }
        ensureTableInitialized(schema);

        HoodieJavaWriteClient<HoodieAvroPayload> client = clientFor(schema);
        String instant = client.startCommit();
        List<HoodieRecord<HoodieAvroPayload>> hoodieRecords = records.stream()
                .map(record -> new HoodieAvroRecord<HoodieAvroPayload>(
                        new HoodieKey(recordKey.apply(record), partitionPath.apply(record)),
                        new HoodieAvroPayload(Option.of(record))))
                .collect(Collectors.toList());
        List<WriteStatus> writeStatuses = client.insert(hoodieRecords, instant);
        for (WriteStatus status : writeStatuses) {
            if (status.hasErrors()) {
                throw new HudiWriteException("Hudi write reported errors for instant " + instant
                        + ", partition " + status.getPartitionPath()
                        + ": " + status.getErrors().size() + " record error(s)"
                        + (status.getGlobalError() != null ? ", global error" : ""),
                        status.getGlobalError());
            }
        }
        client.commit(instant, writeStatuses, Option.of(commitExtraMetadata));
        LOG.info("Committed {} record(s) to Hudi table {} as instant {}",
                records.size(), config.tableBasePath(), instant);
        return Option.of(instant);
    }

    private HoodieJavaWriteClient<HoodieAvroPayload> clientFor(Schema schema) {
        return clientsBySchema.computeIfAbsent(schema.toString(),
            s -> new HoodieJavaWriteClient<>(engineContext, buildWriteConfig(schema)));
    }

    private void ensureTableInitialized(Schema schema) {
        if (tableInitialized) {
            return;
        }
        try {
            HoodieTableMetaClient.builder()
                    .setConf(hadoopConf)
                    .setBasePath(config.tableBasePath())
                    .build();
        } catch (TableNotFoundException e) {
            LOG.info("Initializing new Hudi table {} at {}", config.tableName(), config.tableBasePath());
            try {
                HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(HoodieTableType.COPY_ON_WRITE)
                        .setTableName(config.tableName())
                        .setPayloadClassName(HoodieAvroPayload.class.getName())
                        .setRecordKeyFields(config.recordKeyField())
                        .setPartitionFields(config.partitionPathField())
                        .fromProperties(new java.util.Properties())
                        .initTable(hadoopConf, config.tableBasePath());
            } catch (IOException initFailure) {
                throw new HudiWriteException("Failed to initialize Hudi table at "
                        + config.tableBasePath(), initFailure);
            }
        }
        tableInitialized = true;
    }

    private HoodieWriteConfig buildWriteConfig(Schema schema) {
        return HoodieWriteConfig.newBuilder()
                .withEngineType(EngineType.JAVA)
                .withPath(config.tableBasePath())
                .withSchema(schema.toString())
                .forTable(config.tableName())
                // Explicit commit() below is what attaches commitExtraMetadata (the ingested
                // offset range); auto-commit would complete the instant before that happens.
                .withAutoCommit(false)
                .build();
    }

    @Override
    public void close() {
        for (HoodieJavaWriteClient<HoodieAvroPayload> client : clientsBySchema.values()) {
            try {
                client.close();
            } catch (RuntimeException e) {
                LOG.warn("Failed to close Hudi write client for table {}", config.tableBasePath(), e);
            }
        }
        clientsBySchema.clear();
    }
}
