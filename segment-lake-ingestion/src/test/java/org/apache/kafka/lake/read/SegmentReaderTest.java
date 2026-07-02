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
package org.apache.kafka.lake.read;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentReaderTest {

    @Test
    public void countsDataRecordsFromFetchedSegment() throws Exception {
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE,
                new SimpleRecord("v1".getBytes()),
                new SimpleRecord("v2".getBytes()),
                new SimpleRecord("v3".getBytes()));
        byte[] bytes = toByteArray(records.buffer());

        RemoteStorageManager rsm = mock(RemoteStorageManager.class);
        RemoteLogSegmentMetadata metadata = mock(RemoteLogSegmentMetadata.class);
        when(rsm.fetchLogSegment(eq(metadata), eq(0)))
                .thenReturn(new ByteArrayInputStream(bytes));

        SegmentReader reader = new SegmentReader(rsm);
        assertEquals(3L, reader.countDataRecords(metadata));
    }

    @Test
    public void emptySegmentHasNoRecords() throws Exception {
        RemoteStorageManager rsm = mock(RemoteStorageManager.class);
        RemoteLogSegmentMetadata metadata = mock(RemoteLogSegmentMetadata.class);
        when(rsm.fetchLogSegment(any(), eq(0)))
                .thenReturn(new ByteArrayInputStream(toByteArray(MemoryRecords.EMPTY.buffer())));

        SegmentReader reader = new SegmentReader(rsm);
        assertEquals(0L, reader.countDataRecords(metadata));
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }
}
