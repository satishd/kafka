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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link HudiSegmentWriter} end-to-end against a {@code file://}-backed table, which
 * needs no OCI/CFS setup: it proves the write/commit path itself, independent of which object-store
 * scheme production ultimately uses.
 */
public class HudiSegmentWriterTest {

    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"amount\",\"type\":\"long\"},"
                    + "{\"name\":\"day\",\"type\":\"string\"}]}");

    @Test
    public void writesRecordsAsOneCommitWithExtraMetadata(@TempDir Path tempDir) {
        String tableBasePath = tempDir.resolve("orders_table").toString();
        HudiWriterConfig config = new HudiWriterConfig(tableBasePath, "orders_table", "id", "day");
        HudiSegmentWriter writer = new HudiSegmentWriter(config, new Configuration(),
                record -> record.get("id").toString(),
                record -> record.get("day").toString());

        List<GenericRecord> records = Arrays.asList(
                record("order-1", 100L, "2026-07-01"),
                record("order-2", 250L, "2026-07-01"));

        Option<String> instant = writer.write(records, SCHEMA, Collections.singletonMap("kafka.endOffset", "41"));

        assertTrue(instant.isPresent());

        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(new Configuration())
                .setBasePath(tableBasePath)
                .build();
        HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
        assertEquals(1, timeline.countInstants());
        HoodieInstant commit = timeline.getInstants().findFirst().orElseThrow(AssertionError::new);
        assertEquals(instant.get(), commit.getTimestamp());
    }

    @Test
    public void emptyBatchWritesNoCommit(@TempDir Path tempDir) {
        String tableBasePath = tempDir.resolve("orders_table").toString();
        HudiWriterConfig config = new HudiWriterConfig(tableBasePath, "orders_table", "id", "day");
        HudiSegmentWriter writer = new HudiSegmentWriter(config, new Configuration(),
                record -> record.get("id").toString(),
                record -> record.get("day").toString());

        Option<String> instant = writer.write(Collections.emptyList(), SCHEMA, Collections.emptyMap());

        assertTrue(!instant.isPresent());
    }

    private static GenericRecord record(String id, long amount, String day) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("id", id);
        record.put("amount", amount);
        record.put("day", day);
        return record;
    }
}
