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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HeatpipeHeaderTest {

    @Test
    public void parsesV1Header() {
        byte[] payload = {1, 2, 3, 4, 5};
        ByteBuffer value = withHeader(new byte[]{0x30, 0x46, 0x00, 0x2A}, payload);

        HeatpipeHeader header = HeatpipeHeader.parse(value);

        assertEquals(1, header.version());
        assertEquals(-1, header.metaVersion());
        assertEquals(42, header.schemaVersion());
        assertEquals(4, header.headerLength());
        assertArrayEquals(payload, remaining(header.avroPayload(value)));
    }

    @Test
    public void parsesV2Header() {
        byte[] payload = {9, 8, 7};
        ByteBuffer value = withHeader(
                new byte[]{0x79, 0x32, (byte) 0xD4, 0x6C, 0x00, 0x05, 0x01, 0x2C}, payload);

        HeatpipeHeader header = HeatpipeHeader.parse(value);

        assertEquals(2, header.version());
        assertEquals(5, header.metaVersion());
        assertEquals(300, header.schemaVersion());
        assertEquals(8, header.headerLength());
        assertArrayEquals(payload, remaining(header.avroPayload(value)));
    }

    @Test
    public void rejectsUnrecognizedMagic() {
        ByteBuffer value = ByteBuffer.wrap(new byte[]{0x00, 0x00, 0x00, 0x00, 0x00});

        assertThrows(InvalidHeatpipeHeaderException.class, () -> HeatpipeHeader.parse(value));
    }

    @Test
    public void rejectsTruncatedBuffer() {
        ByteBuffer value = ByteBuffer.wrap(new byte[]{0x30});

        assertThrows(InvalidHeatpipeHeaderException.class, () -> HeatpipeHeader.parse(value));
    }

    @Test
    public void rejectsTruncatedV2Header() {
        // Matches the V2 magic but is too short for the full 8-byte header.
        ByteBuffer value = ByteBuffer.wrap(new byte[]{0x79, 0x32, (byte) 0xD4, 0x6C, 0x00});

        assertThrows(InvalidHeatpipeHeaderException.class, () -> HeatpipeHeader.parse(value));
    }

    private static ByteBuffer withHeader(byte[] header, byte[] payload) {
        ByteBuffer buf = ByteBuffer.allocate(header.length + payload.length);
        buf.put(header);
        buf.put(payload);
        buf.flip();
        return buf;
    }

    private static byte[] remaining(ByteBuffer buf) {
        byte[] out = new byte[buf.remaining()];
        buf.duplicate().get(out);
        return out;
    }
}
