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
package org.apache.kafka.lake.discovery;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SegmentAssemblerTest {

    private final TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), 0, "events");

    @Test
    public void startedThenFinishedUpdateEmitsFinalizedSegment() {
        SegmentAssembler assembler = new SegmentAssembler();
        RemoteLogSegmentId id = RemoteLogSegmentId.generateNew(tp);

        RemoteLogSegmentMetadata started = new RemoteLogSegmentMetadata(
                id, 0L, 99L, -1L, 1, 1000L, 1024,
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                Collections.singletonMap(0, 0L));

        assertTrue(assembler.accept(started).isEmpty(), "A started segment must not be emitted");
        assertEquals(1, assembler.pendingCount());

        CustomMetadata custom = new CustomMetadata("oci://bucket@ns/prefix".getBytes(StandardCharsets.UTF_8));
        RemoteLogSegmentMetadataUpdate finished = new RemoteLogSegmentMetadataUpdate(
                id, 2000L, Optional.of(custom), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 1);

        List<RemoteLogSegmentMetadata> emitted = assembler.accept(finished);

        assertEquals(1, emitted.size());
        RemoteLogSegmentMetadata segment = emitted.get(0);
        assertEquals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED, segment.state());
        assertEquals(Optional.of(custom), segment.customMetadata());
        assertEquals(0L, segment.startOffset());
        assertEquals(99L, segment.endOffset());
        assertEquals(0, assembler.pendingCount(), "Finalized segment must be removed from pending");
    }

    @Test
    public void finishedUpdateWithoutStartedIsIgnored() {
        SegmentAssembler assembler = new SegmentAssembler();
        RemoteLogSegmentId id = RemoteLogSegmentId.generateNew(tp);

        RemoteLogSegmentMetadataUpdate finished = new RemoteLogSegmentMetadataUpdate(
                id, 2000L, Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 1);

        assertTrue(assembler.accept(finished).isEmpty(), "Update without a started record must be ignored");
        assertEquals(0, assembler.pendingCount());
    }

    @Test
    public void deleteStateRemovesPendingSegment() {
        SegmentAssembler assembler = new SegmentAssembler();
        RemoteLogSegmentId id = RemoteLogSegmentId.generateNew(tp);

        RemoteLogSegmentMetadata started = new RemoteLogSegmentMetadata(
                id, 0L, 99L, -1L, 1, 1000L, 1024,
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                Collections.singletonMap(0, 0L));
        assembler.accept(started);

        RemoteLogSegmentMetadataUpdate deleteStarted = new RemoteLogSegmentMetadataUpdate(
                id, 3000L, Optional.empty(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, 1);

        assertTrue(assembler.accept(deleteStarted).isEmpty());
        assertEquals(0, assembler.pendingCount());
    }
}
