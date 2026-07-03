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

import java.nio.ByteBuffer;

/**
 * Parses the small binary header that Heatpipe prepends to a record value, ahead of the Avro
 * payload.
 *
 * <p>Two header layouts are in use:
 * <ul>
 *   <li>V1 (4 bytes): magic {@code 0x30 0x46} ("0F"), then a big-endian {@code uint16} schema
 *       version.</li>
 *   <li>V2 (8 bytes): magic {@code 0x79 0x32 0xD4 0x6C}, a reserved byte, a {@code metaVersion}
 *       byte, then a big-endian {@code uint16} schema version.</li>
 * </ul>
 *
 * <p>This is a manual fallback/validation path: {@code heatpipe4j} is expected to own header
 * parsing, schema fetch, and the {@code msg} union unwrap in production. This class exists so the
 * layout is independently testable and so decoding does not have a hard compile-time dependency on
 * an unresolved internal library.
 */
public final class HeatpipeHeader {

    private static final byte[] V1_MAGIC = {0x30, 0x46};
    private static final byte[] V2_MAGIC = {0x79, 0x32, (byte) 0xD4, 0x6C};
    private static final int V1_HEADER_LENGTH = 4;
    private static final int V2_HEADER_LENGTH = 8;
    private static final int NO_META_VERSION = -1;

    private final int version;
    private final int metaVersion;
    private final int schemaVersion;
    private final int headerLength;

    private HeatpipeHeader(int version, int metaVersion, int schemaVersion, int headerLength) {
        this.version = version;
        this.metaVersion = metaVersion;
        this.schemaVersion = schemaVersion;
        this.headerLength = headerLength;
    }

    /** @return the header version, {@code 1} or {@code 2}. */
    public int version() {
        return version;
    }

    /** @return the {@code metaVersion} byte for a V2 header, or {@code -1} for V1. */
    public int metaVersion() {
        return metaVersion;
    }

    /** @return the schema version to fetch from the Schema Service. */
    public int schemaVersion() {
        return schemaVersion;
    }

    /** @return the number of leading bytes occupied by the header. */
    public int headerLength() {
        return headerLength;
    }

    /**
     * Parse the header from the start of {@code value}, without consuming it.
     *
     * @param value the raw record value; only bytes {@code [position, headerLength)} are read.
     * @return the parsed header.
     * @throws InvalidHeatpipeHeaderException if the buffer is too short or the magic is unrecognized.
     */
    public static HeatpipeHeader parse(ByteBuffer value) {
        int available = value.remaining();
        int base = value.position();
        if (available >= V2_HEADER_LENGTH && matches(value, base, V2_MAGIC)) {
            int metaVersion = Byte.toUnsignedInt(value.get(base + 5));
            int schemaVersion = readUnsignedShortBE(value, base + 6);
            return new HeatpipeHeader(2, metaVersion, schemaVersion, V2_HEADER_LENGTH);
        }
        if (available >= V1_HEADER_LENGTH && matches(value, base, V1_MAGIC)) {
            int schemaVersion = readUnsignedShortBE(value, base + 2);
            return new HeatpipeHeader(1, NO_META_VERSION, schemaVersion, V1_HEADER_LENGTH);
        }
        if (available < V1_HEADER_LENGTH) {
            throw new InvalidHeatpipeHeaderException(
                    "Value is too short to contain a Heatpipe header: " + available + " byte(s)");
        }
        throw new InvalidHeatpipeHeaderException(
                "Unrecognized Heatpipe header magic at position " + base);
    }

    /**
     * @param value the same buffer passed to {@link #parse(ByteBuffer)}.
     * @return a slice covering the Avro payload, i.e. the bytes after the header.
     */
    public ByteBuffer avroPayload(ByteBuffer value) {
        ByteBuffer slice = value.duplicate();
        slice.position(value.position() + headerLength);
        return slice.slice();
    }

    private static boolean matches(ByteBuffer buf, int base, byte[] magic) {
        for (int i = 0; i < magic.length; i++) {
            if (buf.get(base + i) != magic[i]) {
                return false;
            }
        }
        return true;
    }

    private static int readUnsignedShortBE(ByteBuffer buf, int offset) {
        int hi = Byte.toUnsignedInt(buf.get(offset));
        int lo = Byte.toUnsignedInt(buf.get(offset + 1));
        return (hi << 8) | lo;
    }
}
