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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HeatpipeAvroDecoderTest {

    private static final int SCHEMA_VERSION = 7;
    private static final String TOPIC = "orders";

    private static final Schema INNER_SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"long\"}]}");
    private static final Schema WRAPPER_SCHEMA = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Wrapper\",\"fields\":["
                    + "{\"name\":\"msg\",\"type\":[\"null\"," + INNER_SCHEMA.toString() + "]}]}");

    private final List<String> deadLettered = new ArrayList<>();
    private final DeadLetterSink deadLetterSink = new DeadLetterSink() {
        @Override
        public void record(String topic, long offset, ByteBuffer value, String reason) {
            deadLettered.add(topic + "/" + offset + ": " + reason);
        }

        @Override
        public void close() {
        }
    };

    private FakeSchemaClient schemaClient;
    private HeatpipeAvroDecoder decoder;

    @BeforeEach
    public void setUp() {
        schemaClient = new FakeSchemaClient();
        decoder = new HeatpipeAvroDecoder(schemaClient, deadLetterSink);
    }

    @Test
    public void decodesInnerRecordFromMsgUnion() {
        schemaClient.schemasByVersion.put(SCHEMA_VERSION, WRAPPER_SCHEMA);
        GenericRecord inner = new GenericData.Record(INNER_SCHEMA);
        inner.put("id", 42L);
        ByteBuffer value = withV1Header(SCHEMA_VERSION, encodeWrapper(inner));

        Optional<GenericRecord> result = decoder.decode(TOPIC, 100L, value);

        assertTrue(result.isPresent());
        assertEquals(42L, result.get().get("id"));
        assertTrue(deadLettered.isEmpty());
    }

    @Test
    public void deadLettersNullMsg() {
        schemaClient.schemasByVersion.put(SCHEMA_VERSION, WRAPPER_SCHEMA);
        ByteBuffer value = withV1Header(SCHEMA_VERSION, encodeWrapper(null));

        Optional<GenericRecord> result = decoder.decode(TOPIC, 101L, value);

        assertFalse(result.isPresent());
        assertEquals(1, deadLettered.size());
        assertTrue(deadLettered.get(0).contains("null 'msg'"));
    }

    @Test
    public void deadLettersInvalidHeader() {
        ByteBuffer value = ByteBuffer.wrap(new byte[]{0x00, 0x00});

        Optional<GenericRecord> result = decoder.decode(TOPIC, 102L, value);

        assertFalse(result.isPresent());
        assertEquals(1, deadLettered.size());
        assertTrue(deadLettered.get(0).contains("ErrInvalidHeaderVersion"));
    }

    @Test
    public void deadLettersSchemaFetchFailure() {
        schemaClient.failWith = new SchemaFetchException("schema service unreachable");
        ByteBuffer value = withV1Header(SCHEMA_VERSION, new byte[]{1, 2, 3});

        Optional<GenericRecord> result = decoder.decode(TOPIC, 103L, value);

        assertFalse(result.isPresent());
        assertEquals(1, deadLettered.size());
        assertTrue(deadLettered.get(0).contains("schema fetch failed"));
    }

    @Test
    public void deadLettersMalformedAvroPayload() {
        schemaClient.schemasByVersion.put(SCHEMA_VERSION, WRAPPER_SCHEMA);
        ByteBuffer value = withV1Header(SCHEMA_VERSION, new byte[]{(byte) 0xFF, (byte) 0xFF});

        Optional<GenericRecord> result = decoder.decode(TOPIC, 104L, value);

        assertFalse(result.isPresent());
        assertEquals(1, deadLettered.size());
        assertTrue(deadLettered.get(0).contains("Avro decode failed"));
    }

    private static byte[] encodeWrapper(GenericRecord inner) {
        GenericRecord wrapper = new GenericData.Record(WRAPPER_SCHEMA);
        wrapper.put("msg", inner);
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            new GenericDatumWriter<GenericRecord>(WRAPPER_SCHEMA).write(wrapper, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ByteBuffer withV1Header(int schemaVersion, byte[] avroPayload) {
        ByteBuffer buf = ByteBuffer.allocate(4 + avroPayload.length);
        buf.put((byte) 0x30).put((byte) 0x46);
        buf.put((byte) ((schemaVersion >> 8) & 0xFF));
        buf.put((byte) (schemaVersion & 0xFF));
        buf.put(avroPayload);
        buf.flip();
        return buf;
    }

    private static final class FakeSchemaClient implements SchemaClient {
        private final java.util.Map<Integer, Schema> schemasByVersion = new java.util.HashMap<>();
        private SchemaFetchException failWith;

        @Override
        public Schema schemaFor(String topic, int schemaVersion) throws SchemaFetchException {
            if (failWith != null) {
                throw failWith;
            }
            Schema schema = schemasByVersion.get(schemaVersion);
            if (schema == null) {
                throw new SchemaFetchException("no schema registered for version " + schemaVersion);
            }
            return schema;
        }
    }
}
