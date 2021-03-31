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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.server.log.remote.storage.TestUtils.sameElementsWithOrder;
import static org.apache.kafka.server.log.remote.storage.TestUtils.sameElementsWithoutOrder;

public class InmemoryRemoteLogMetadataManagerTest {

    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition("foo", 0));
    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID_0 = 0;
    private static final int BROKER_ID_1 = 1;

    @Test
    public void testSegmentsWithDifferentStates() throws Exception {
        InmemoryRemoteLogMetadataManager rlmm = new InmemoryRemoteLogMetadataManager();

        // Add segments with different states and check rlmm.remoteLogSegmentMetadata(tp, leaderEpoch, offset)
        // rlmm.listRemoteLogSegments(tp, leaderEpoch), and rlmm.listRemoteLogSegments(tp).

        // =============================================================================================================
        // 1.Create a segment with state COPY_SEGMENT_STARTED, and check for searching that segment and listing the
        // segments.
        // =============================================================================================================
        Map<Integer, Long> seg0LeaderEpochs = Collections.singletonMap(0, 0L);
        RemoteLogSegmentId seg0Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segCopyInProgress = new RemoteLogSegmentMetadata(seg0Id, 0L, 50L, -1L, BROKER_ID_0,
                System.currentTimeMillis(), SEG_SIZE, seg0LeaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(segCopyInProgress);

        // This segment should not be available as the state is not reached to COPY_SEGMENT_FINISHED.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset0Epoch0 = rlmm.remoteLogSegmentMetadata(TP0, 0, 0);
        Assertions.assertFalse(segMetadataForOffset0Epoch0.isPresent());

        // rlmm.listRemoteLogSegments(tp, 0) should contain the above segment.
        List<RemoteLogSegmentMetadata> expectedSegments1 = Collections.singletonList(segCopyInProgress);
        Assertions.assertTrue(sameElementsWithOrder(rlmm.listRemoteLogSegments(TP0, 0),
                expectedSegments1.iterator()));
        // rlmm.listRemoteLogSegments(tp) should contain the above segment.
        Assertions.assertTrue(sameElementsWithoutOrder(rlmm.listRemoteLogSegments(TP0), expectedSegments1.iterator()));

        // =============================================================================================================
        // 2.Create a segment and move it to state COPY_SEGMENT_FINISHED. and check for searching that segment and
        // listing the segments.
        // =============================================================================================================
        Map<Integer, Long> seg1LeaderEpochs = Collections.singletonMap(0, 101L);
        RemoteLogSegmentId seg1Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata seg1 = new RemoteLogSegmentMetadata(seg1Id, 101L, 200L, -1L, BROKER_ID_0,
                System.currentTimeMillis(), SEG_SIZE, seg1LeaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(seg1);
        RemoteLogSegmentMetadataUpdate seg1Update = new RemoteLogSegmentMetadataUpdate(seg1Id,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_0);
        rlmm.updateRemoteLogSegmentMetadata(seg1Update);
        RemoteLogSegmentMetadata segCopyFinished = seg1.createRemoteLogSegmentWithUpdates(seg1Update);

        // Search should return the above segment.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset150 = rlmm.remoteLogSegmentMetadata(TP0, 0, 150);
        Assertions.assertEquals(seg1.createRemoteLogSegmentWithUpdates(seg1Update),
                segMetadataForOffset150.orElse(null));

        // rlmm.listRemoteLogSegments(0) should not contain the above segment.
        List<RemoteLogSegmentMetadata> expectedSegments2 = Arrays.asList(segCopyInProgress, segCopyFinished);
        Assertions.assertTrue(sameElementsWithOrder(rlmm.listRemoteLogSegments(TP0, 0),
                expectedSegments2.iterator()));
        // But rlmm.listRemoteLogSegments() should contain both the segments.
        Assertions.assertTrue(sameElementsWithoutOrder(rlmm.listRemoteLogSegments(TP0), expectedSegments2.iterator()));

        // =============================================================================================================
        // 3.Create a segment and move it to state DELETE_SEGMENT_STARTED, and check for searching that segment and
        // listing the segments.
        // =============================================================================================================
        Map<Integer, Long> seg2leaderEpochs = Collections.singletonMap(0, 201L);
        RemoteLogSegmentId seg2Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata seg2 = new RemoteLogSegmentMetadata(seg2Id, 201L, 300L, -1L, BROKER_ID_0,
                System.currentTimeMillis(), SEG_SIZE, seg2leaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(seg2);
        RemoteLogSegmentMetadataUpdate seg2Update = new RemoteLogSegmentMetadataUpdate(seg2Id,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, BROKER_ID_1);
        rlmm.updateRemoteLogSegmentMetadata(seg2Update);
        RemoteLogSegmentMetadata segDeleteStarted = seg2.createRemoteLogSegmentWithUpdates(seg2Update);

        // Search should not return the above segment as their leader epoch state is cleared.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset250Epoch0 = rlmm.remoteLogSegmentMetadata(TP0, 0, 250);
        Assertions.assertFalse(segMetadataForOffset250Epoch0.isPresent());

        // rlmm.listRemoteLogSegments(0) should contain the above segment.
        List<RemoteLogSegmentMetadata> expectedSegments3 = Arrays.asList(segCopyInProgress, segCopyFinished,
                segDeleteStarted);
        Assertions.assertTrue(sameElementsWithOrder(rlmm.listRemoteLogSegments(TP0, 0), expectedSegments3.iterator()));
        // But rlmm.listRemoteLogSegments() should contain all the segments.
        Assertions.assertTrue(sameElementsWithoutOrder(rlmm.listRemoteLogSegments(TP0), expectedSegments3.iterator()));

        // =============================================================================================================
        // 4.Create a segment and move it to state DELETE_SEGMENT_FINISHED, and check for searching that segment and
        // listing the segments.
        // =============================================================================================================
        Map<Integer, Long> seg3leaderEpochs = Collections.singletonMap(0, 301L);
        RemoteLogSegmentId seg3Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata seg3 = new RemoteLogSegmentMetadata(seg3Id, 301L, 400L, -1L, BROKER_ID_0,
                System.currentTimeMillis(), SEG_SIZE, seg3leaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(seg3);
        RemoteLogSegmentMetadataUpdate seg3Update1 = new RemoteLogSegmentMetadataUpdate(seg3Id,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, BROKER_ID_1);
        rlmm.updateRemoteLogSegmentMetadata(seg3Update1);

        // Search should not return the above segment as their leader epoch state is cleared.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset350Epoch0 = rlmm.remoteLogSegmentMetadata(TP0, 0, 350);
        Assertions.assertFalse(segMetadataForOffset350Epoch0.isPresent());

        RemoteLogSegmentMetadataUpdate seg3Update2 = new RemoteLogSegmentMetadataUpdate(seg3Id,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, BROKER_ID_1);
        rlmm.updateRemoteLogSegmentMetadata(seg3Update2);

        List<RemoteLogSegmentMetadata> expectedSegments4 = Arrays.asList(segCopyInProgress, segCopyFinished,
                segDeleteStarted);
        // rlmm.listRemoteLogSegments(tp, 0) should not contain the above segment.
        Assertions.assertTrue(sameElementsWithOrder(rlmm.listRemoteLogSegments(TP0, 0), expectedSegments4.iterator()));
        // But rlmm.listRemoteLogSegments(tp) should not contain both the segments as it should have been removed.
        Assertions.assertTrue(sameElementsWithoutOrder(rlmm.listRemoteLogSegments(TP0), expectedSegments4.iterator()));

        // Search for highest offset for leader epoch 0.
        Optional<Long> highestOffsetForEpoch0 = rlmm.highestOffsetForEpoch(TP0, 0);
        Assertions.assertEquals(Optional.of(400L), highestOffsetForEpoch0);

        // Search for highest offset for leader epoch 1, that does not exist.
        Optional<Long> highestOffsetForEpoch1 = rlmm.highestOffsetForEpoch(TP0, 1);
        Assertions.assertFalse(highestOffsetForEpoch1.isPresent());
    }

    public void testFetchSegments() throws Exception {
        InmemoryRemoteLogMetadataManager rlmm = new InmemoryRemoteLogMetadataManager();

        // This is a basic test and it doe snot cover all the scenarios. InmemoryRemoteLogMetadataManager is used only
        // in integration tests but not for production.

        // 1.Create a segment with state COPY_SEGMENT_STARTED, and this segment should not be available.
        Map<Integer, Long> seg1LeaderEpochs = Collections.singletonMap(0, 101L);
        RemoteLogSegmentId seg1Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata seg1 = new RemoteLogSegmentMetadata(seg1Id, 101L, 200L, -1L, BROKER_ID_0,
                System.currentTimeMillis(), SEG_SIZE, seg1LeaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(seg1);

        // Search should not return the above segment.
        Assertions.assertFalse(rlmm.remoteLogSegmentMetadata(TP0, 0, 150).isPresent());

        // 2.Move that segment to COPY_SEGMENT_FINISHED state and this segment should be available.
        RemoteLogSegmentMetadataUpdate seg1Update = new RemoteLogSegmentMetadataUpdate(seg1Id,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_0);
        rlmm.updateRemoteLogSegmentMetadata(seg1Update);
        RemoteLogSegmentMetadata segCopyFinished = seg1.createRemoteLogSegmentWithUpdates(seg1Update);

        // Search should return the above segment.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset150 = rlmm.remoteLogSegmentMetadata(TP0, 0, 150);
        Assertions.assertEquals(Optional.of(segCopyFinished), segMetadataForOffset150);
    }

    @Test
    public void testRemotePartitionDeletion() throws Exception {
        InmemoryRemoteLogMetadataManager rlmm = new InmemoryRemoteLogMetadataManager();

        // Create remote log segment metadata and add them to RLMM.

        // segment 0
        // 0-100
        // leader epochs (0,0), (1,20), (2,80)
        Map<Integer, Long> seg0leaderEpochs = new HashMap<>();
        seg0leaderEpochs.put(0, 0L);
        seg0leaderEpochs.put(1, 20L);
        seg0leaderEpochs.put(2, 50L);
        seg0leaderEpochs.put(3, 80L);
        RemoteLogSegmentId segIdStart0End100 = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segMetadataStart0End100 = new RemoteLogSegmentMetadata(segIdStart0End100, 0L, 100L,
                -1L, BROKER_ID_0, System.currentTimeMillis(), SEG_SIZE, seg0leaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(segMetadataStart0End100);
        RemoteLogSegmentMetadataUpdate segMetadataStart0End100Update = new RemoteLogSegmentMetadataUpdate(
                segIdStart0End100,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0);
        rlmm.updateRemoteLogSegmentMetadata(segMetadataStart0End100Update);

        RemoteLogSegmentMetadata expectedSegMetadataStart0End100 = segMetadataStart0End100
                .createRemoteLogSegmentWithUpdates(segMetadataStart0End100Update);

        // Check that the seg exists in RLMM
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset30Epoch1 = rlmm.remoteLogSegmentMetadata(TP0, 1, 30L);
        Assertions.assertEquals(Optional.of(expectedSegMetadataStart0End100), segMetadataForOffset30Epoch1);

        // Mark the partition for deletion. RLMM should clear all its internal state for that partition.
        rlmm.putRemotePartitionDeleteMetadata(
                createRemotePartitionDeleteMetadata(RemotePartitionDeleteState.DELETE_PARTITION_MARKED));

        Optional<RemoteLogSegmentMetadata> segMetadataForStart0End100AfterDelMark = rlmm.remoteLogSegmentMetadata(TP0,
                1, 30L);
        Assertions.assertEquals(Optional.of(expectedSegMetadataStart0End100), segMetadataForStart0End100AfterDelMark);

        // Set the partition deletion state as started. Partition and segments should still be accessible as they are not
        // yet deleted.
        rlmm.putRemotePartitionDeleteMetadata(
                createRemotePartitionDeleteMetadata(RemotePartitionDeleteState.DELETE_PARTITION_STARTED));

        Optional<RemoteLogSegmentMetadata> segMetadataForStart0End100AfterDelStart = rlmm.remoteLogSegmentMetadata(TP0,
                1, 30L);
        Assertions.assertEquals(Optional.of(expectedSegMetadataStart0End100), segMetadataForStart0End100AfterDelStart);

        // Set the partition deletion state as finished. RLMM should clear all its internal state for that partition.
        rlmm.putRemotePartitionDeleteMetadata(
                createRemotePartitionDeleteMetadata(RemotePartitionDeleteState.DELETE_PARTITION_FINISHED));

        Assertions.assertThrows(RemoteResourceNotFoundException.class,
                () -> rlmm.remoteLogSegmentMetadata(TP0, 1, 30L));
    }

    private RemotePartitionDeleteMetadata createRemotePartitionDeleteMetadata(RemotePartitionDeleteState state) {
        return new RemotePartitionDeleteMetadata(TP0, state, System.currentTimeMillis(), BROKER_ID_0);
    }
}
