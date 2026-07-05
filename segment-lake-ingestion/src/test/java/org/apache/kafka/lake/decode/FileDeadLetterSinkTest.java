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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileDeadLetterSinkTest {

    @TempDir
    Path tempDir;

    @Test
    public void writesOneLinePerRecordAndCreatesParentDirs() throws IOException {
        Path path = tempDir.resolve("nested/dead-letters.tsv");
        ByteBuffer value = ByteBuffer.wrap("payload".getBytes(StandardCharsets.UTF_8));

        try (FileDeadLetterSink sink = new FileDeadLetterSink(path)) {
            sink.record("orders", 5L, value, "bad header");
            sink.record("orders", 6L, value, "reason\twith\ttabs");
        }

        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        assertEquals(2, lines.size());

        String[] fields = lines.get(0).split("\t");
        assertEquals("orders", fields[1]);
        assertEquals("5", fields[2]);
        assertEquals("bad header", fields[3]);
        assertEquals("payload", new String(Base64.getDecoder().decode(fields[4]), StandardCharsets.UTF_8));

        assertTrue(lines.get(1).contains("reason with tabs"));
    }

    @Test
    public void writesDistinguishableSegmentAuditLine() throws IOException {
        Path path = tempDir.resolve("dead-letters.tsv");

        try (FileDeadLetterSink sink = new FileDeadLetterSink(path)) {
            sink.record("orders", 5L, ByteBuffer.wrap("payload".getBytes(StandardCharsets.UTF_8)), "bad header");
            sink.recordAbandonedSegment("orders-0", "seg-uuid", 10L, 42L, "evicted after pending timeout");
        }

        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        assertEquals(2, lines.size());

        String[] fields = lines.get(1).split("\t");
        assertEquals("SEGMENT", fields[0]);
        assertEquals("orders-0", fields[2]);
        assertEquals("seg-uuid", fields[3]);
        assertEquals("10", fields[4]);
        assertEquals("42", fields[5]);
        assertEquals("evicted after pending timeout", fields[6]);
    }
}
