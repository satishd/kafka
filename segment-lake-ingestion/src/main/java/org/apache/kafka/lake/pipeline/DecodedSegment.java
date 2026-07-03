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
package org.apache.kafka.lake.pipeline;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

/**
 * A fully decoded segment ready to be committed to Hudi: the source segment metadata, its decoded
 * records grouped by Avro schema, the commit extra-metadata (ingested offset range) to tag the
 * commit with, and the count of records that were dead-lettered during decode.
 *
 * <p>Produced by the concurrent decode stage and handed to the single writer thread (see
 * {@link Pipeline}); immutable once constructed.
 */
final class DecodedSegment {

    private final RemoteLogSegmentMetadata segment;
    private final Map<Schema, List<GenericRecord>> recordsBySchema;
    private final Map<String, String> commitExtraMetadata;
    private final long deadLettered;

    DecodedSegment(RemoteLogSegmentMetadata segment,
                   Map<Schema, List<GenericRecord>> recordsBySchema,
                   Map<String, String> commitExtraMetadata,
                   long deadLettered) {
        this.segment = segment;
        this.recordsBySchema = recordsBySchema;
        this.commitExtraMetadata = commitExtraMetadata;
        this.deadLettered = deadLettered;
    }

    RemoteLogSegmentMetadata segment() {
        return segment;
    }

    Map<Schema, List<GenericRecord>> recordsBySchema() {
        return recordsBySchema;
    }

    Map<String, String> commitExtraMetadata() {
        return commitExtraMetadata;
    }

    long deadLettered() {
        return deadLettered;
    }

    long recordCount() {
        long total = 0;
        for (List<GenericRecord> records : recordsBySchema.values()) {
            total += records.size();
        }
        return total;
    }
}
