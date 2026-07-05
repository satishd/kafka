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
import java.util.concurrent.atomic.AtomicLong;

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

    @Test
    public void deleteOfPendingSegmentIsDrainedForAudit() {
        SegmentAssembler assembler = new SegmentAssembler();
        RemoteLogSegmentId id = RemoteLogSegmentId.generateNew(tp);
        assembler.accept(startedSegment(id));

        RemoteLogSegmentMetadataUpdate deleteStarted = new RemoteLogSegmentMetadataUpdate(
                id, 3000L, Optional.empty(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, 1);
        assembler.accept(deleteStarted);

        List<RemoteLogSegmentMetadata> abandoned = assembler.drainAbandoned();
        assertEquals(1, abandoned.size());
        assertEquals(id, abandoned.get(0).remoteLogSegmentId());
        assertEquals(0, assembler.pendingCount());
        assertTrue(assembler.drainAbandoned().isEmpty(), "Draining clears the buffer");
    }

    @Test
    public void retentionDeleteOfUnknownSegmentIsNotAudited() {
        SegmentAssembler assembler = new SegmentAssembler();
        RemoteLogSegmentId id = RemoteLogSegmentId.generateNew(tp);

        // No started record was ever tracked (already-finished-and-dropped, or joined mid-stream):
        // a DELETE must not be mistaken for a deleted-before-ingest segment.
        RemoteLogSegmentMetadataUpdate deleteStarted = new RemoteLogSegmentMetadataUpdate(
                id, 3000L, Optional.empty(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, 1);

        assertTrue(assembler.accept(deleteStarted).isEmpty());
        assertTrue(assembler.drainAbandoned().isEmpty());
    }

    @Test
    public void evictStaleRemovesEntriesOlderThanTimeout() {
        AtomicLong now = new AtomicLong(0);
        SegmentAssembler assembler = new SegmentAssembler(now::get);
        RemoteLogSegmentId id = RemoteLogSegmentId.generateNew(tp);
        assembler.accept(startedSegment(id));

        now.addAndGet(5_000L);
        List<RemoteLogSegmentMetadata> evicted = assembler.evictStale(1_000L);

        assertEquals(1, evicted.size());
        assertEquals(id, evicted.get(0).remoteLogSegmentId());
        assertEquals(0, assembler.pendingCount());
    }

    @Test
    public void evictStaleKeepsEntriesYoungerThanTimeout() {
        AtomicLong now = new AtomicLong(0);
        SegmentAssembler assembler = new SegmentAssembler(now::get);
        assembler.accept(startedSegment(RemoteLogSegmentId.generateNew(tp)));

        now.addAndGet(500L);
        assertTrue(assembler.evictStale(1_000L).isEmpty());
        assertEquals(1, assembler.pendingCount());
    }

    @Test
    public void evictStaleIsDisabledForNonPositiveTimeout() {
        AtomicLong now = new AtomicLong(0);
        SegmentAssembler assembler = new SegmentAssembler(now::get);
        assembler.accept(startedSegment(RemoteLogSegmentId.generateNew(tp)));

        now.addAndGet(1_000_000L);
        assertTrue(assembler.evictStale(0L).isEmpty());
        assertTrue(assembler.evictStale(-1L).isEmpty());
        assertEquals(1, assembler.pendingCount());
    }

    @Test
    public void firstSeenPreservedAcrossReObservation() {
        AtomicLong now = new AtomicLong(0);
        SegmentAssembler assembler = new SegmentAssembler(now::get);
        RemoteLogSegmentId id = RemoteLogSegmentId.generateNew(tp);

        assembler.accept(startedSegment(id));   // first seen at t=0
        now.addAndGet(5_000L);
        assembler.accept(startedSegment(id));   // re-observed at t=5000; first-seen must stay t=0
        now.addAndGet(1_000L);                  // now t=6000

        // Age from the ORIGINAL first-seen (6000) exceeds the timeout; a reset to t=5000 (age 1000)
        // would not. Eviction proves the first-seen time was preserved.
        assertEquals(1, assembler.evictStale(4_000L).size());
        assertEquals(0, assembler.pendingCount());
    }

    private RemoteLogSegmentMetadata startedSegment(RemoteLogSegmentId id) {
        return new RemoteLogSegmentMetadata(
                id, 0L, 99L, -1L, 1, 1000L, 1024,
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                Collections.singletonMap(0, 0L));
    }
}
