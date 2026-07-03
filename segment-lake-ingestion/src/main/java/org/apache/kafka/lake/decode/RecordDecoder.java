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
package org.apache.kafka.lake.decode;

import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Decodes a single Kafka record value into an Avro {@link GenericRecord}.
 *
 * <p>Implementations must not throw on malformed input: any record that cannot be decoded should
 * be routed to a {@link DeadLetterSink} and {@link Optional#empty()} returned, so that one bad
 * record does not stop the rest of a segment from being ingested.
 */
public interface RecordDecoder {

    /**
     * @param topic  the topic the record belongs to (schemas are looked up per topic).
     * @param offset the record's offset within its partition, used only for dead-letter reporting.
     * @param value  the raw record value, e.g. from {@link org.apache.kafka.common.record.Record#value()}.
     * @return the decoded inner record, or empty if the value could not be decoded.
     */
    Optional<GenericRecord> decode(String topic, long offset, ByteBuffer value);
}
