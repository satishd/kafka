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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CommittedOffsetsFileTest {

    @Test
    public void testReadWriteCommittedOffsetsFile() throws IOException {
        File file = TestUtils.tempFile();
        CommittedOffsetsFile committedOffsetsFile = new CommittedOffsetsFile(file);

        Map<Integer, Long> entries = new HashMap<>();
        entries.put(0, 100L);
        entries.put(1, 200L);
        entries.put(2, 150L);
        committedOffsetsFile.writeEntries(entries);

        Map<Integer, Long> readEntries = committedOffsetsFile.readEntries();

        Assertions.assertEquals(entries, readEntries);
    }

    @Test
    public void testEmptyCommittedOffsetsFile() throws IOException {
        File file = TestUtils.tempFile();
        CommittedOffsetsFile committedOffsetsFile = new CommittedOffsetsFile(file);

        committedOffsetsFile.writeEntries(Collections.emptyMap());

        Map<Integer, Long> readEntries = committedOffsetsFile.readEntries();

        Assertions.assertTrue(readEntries.isEmpty());
    }
}
