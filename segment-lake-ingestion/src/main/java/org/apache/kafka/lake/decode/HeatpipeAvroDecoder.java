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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Decodes a Heatpipe-wrapped Avro record value: parses the {@link HeatpipeHeader}, fetches the
 * writer schema for the record's (topic, schemaVersion) via {@link SchemaClient}, Avro-decodes the
 * body, and unwraps the {@code msg} field (a {@code ["null", inner]} union) to the inner record.
 *
 * <p>Every failure mode (bad header, schema fetch failure, malformed Avro, null {@code msg}) is
 * routed to the configured {@link DeadLetterSink}; this class never throws for malformed input.
 */
public class HeatpipeAvroDecoder implements RecordDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(HeatpipeAvroDecoder.class);
    private static final String MSG_FIELD = "msg";

    private final SchemaClient schemaClient;
    private final DeadLetterSink deadLetterSink;
    private final DecoderFactory decoderFactory = DecoderFactory.get();

    public HeatpipeAvroDecoder(SchemaClient schemaClient, DeadLetterSink deadLetterSink) {
        this.schemaClient = schemaClient;
        this.deadLetterSink = deadLetterSink;
    }

    @Override
    public Optional<GenericRecord> decode(String topic, long offset, ByteBuffer value) {
        HeatpipeHeader header;
        try {
            header = HeatpipeHeader.parse(value);
        } catch (InvalidHeatpipeHeaderException e) {
            deadLetter(topic, offset, value, "ErrInvalidHeaderVersion: " + e.getMessage());
            return Optional.empty();
        }

        Schema schema;
        try {
            schema = schemaClient.schemaFor(topic, header.schemaVersion());
        } catch (SchemaFetchException e) {
            deadLetter(topic, offset, value, "schema fetch failed for version "
                    + header.schemaVersion() + ": " + e.getMessage());
            return Optional.empty();
        }

        GenericRecord wrapper;
        try {
            wrapper = decodeAvro(schema, header.avroPayload(value));
        } catch (IOException | RuntimeException e) {
            deadLetter(topic, offset, value, "Avro decode failed: " + e.getMessage());
            return Optional.empty();
        }

        Schema.Field msgField = wrapper.getSchema().getField(MSG_FIELD);
        if (msgField == null) {
            deadLetter(topic, offset, value, "wrapper schema has no '" + MSG_FIELD + "' field");
            return Optional.empty();
        }
        Object msg = wrapper.get(msgField.pos());
        if (msg == null) {
            deadLetter(topic, offset, value, "null '" + MSG_FIELD + "' field");
            return Optional.empty();
        }
        if (!(msg instanceof GenericRecord)) {
            deadLetter(topic, offset, value, "'" + MSG_FIELD + "' field is not a record: "
                    + msg.getClass().getName());
            return Optional.empty();
        }
        return Optional.of((GenericRecord) msg);
    }

    private GenericRecord decodeAvro(Schema schema, ByteBuffer payload) throws IOException {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = decoderFactory.binaryDecoder(toArray(payload), null);
        return reader.read(null, decoder);
    }

    private void deadLetter(String topic, long offset, ByteBuffer value, String reason) {
        LOG.debug("Dead-lettering record from topic {}: {}", topic, reason);
        deadLetterSink.record(topic, offset, value, reason);
    }

    private static byte[] toArray(ByteBuffer buffer) {
        if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.position() == 0
                && buffer.remaining() == buffer.array().length) {
            return buffer.array();
        }
        ByteBuffer dup = buffer.duplicate();
        byte[] out = new byte[dup.remaining()];
        dup.get(out);
        return out;
    }
}
