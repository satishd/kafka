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

import static org.apache.kafka.server.log.remote.storage.TestUtils.sameElementsWithoutOrder;

public class RemoteLogMetadataCacheTest {

    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition("foo", 0));
    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID_0 = 0;
    private static final int BROKER_ID_1 = 1;

    @Test
    public void testSegmentsLifeCycleInCache() throws Exception {
        RemoteLogMetadataCache cache = new RemoteLogMetadataCache();
        // Create remote log segment metadata and add them to RemoteLogMetadataCache.

        // segment 0
        // 0-100
        // leader epochs (0,0), (1,20), (2,80)
        Map<Integer, Long> seg0LeaderEpochs = new HashMap<>();
        seg0LeaderEpochs.put(0, 0L);
        seg0LeaderEpochs.put(1, 20L);
        seg0LeaderEpochs.put(2, 80L);
        RemoteLogSegmentId segIdStart0End100 = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segMetadataStart0End100 = new RemoteLogSegmentMetadata(segIdStart0End100, 0L, 100L,
                -1L, BROKER_ID_0, System.currentTimeMillis(), SEG_SIZE, seg0LeaderEpochs);
        cache.addCopyInProgressSegment(segMetadataStart0End100);

        // We should not get this as the segment is still getting copied and it is not yet considered successful until
        // it reaches RemoteLogSegmentState.COPY_SEGMENT_FINISHED.
        Assertions.assertFalse(cache.remoteLogSegmentMetadata(40, 1).isPresent());

        RemoteLogSegmentMetadataUpdate segMetadataStart0End100Update = new RemoteLogSegmentMetadataUpdate(
                segIdStart0End100,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_0);
        cache.updateRemoteLogSegmentMetadata(segMetadataStart0End100Update);
        RemoteLogSegmentMetadata updatedSegment0 = segMetadataStart0End100
                .createRemoteLogSegmentWithUpdates(segMetadataStart0End100Update);

        // segment 1
        // 101 - 200
        // no changes in leadership with in this segment
        // leader epochs (2, 101)
        Map<Integer, Long> seg1LeaderEpochs = new HashMap<>();
        seg1LeaderEpochs.put(2, 101L);
        RemoteLogSegmentId segIdStart101End200 = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segMetadataStart101End200 = new RemoteLogSegmentMetadata(segIdStart101End200, 101L,
                200L, -1L, BROKER_ID_0, System.currentTimeMillis(), SEG_SIZE, seg1LeaderEpochs);
        cache.addCopyInProgressSegment(segMetadataStart101End200);
        RemoteLogSegmentMetadataUpdate segMetadataStart101End200Update = new RemoteLogSegmentMetadataUpdate(
                segIdStart101End200, System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_0);
        cache.updateRemoteLogSegmentMetadata(segMetadataStart101End200Update);
        RemoteLogSegmentMetadata updatedSegment1 = segMetadataStart101End200
                .createRemoteLogSegmentWithUpdates(segMetadataStart101End200Update);

        // segment 2
        // 201 - 300
        // moved to epoch 3 in between
        // leader epochs (2, 201), (3, 240)
        Map<Integer, Long> seg2LeaderEpochs = new HashMap<>();
        seg2LeaderEpochs.put(2, 201L);
        seg2LeaderEpochs.put(3, 240L);
        RemoteLogSegmentId segIdStart101End300 = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segMetadataStart101End300 = new RemoteLogSegmentMetadata(segIdStart101End300, 201L,
                300L, -1L, 3,
                System.currentTimeMillis(), SEG_SIZE, seg2LeaderEpochs);
        cache.addCopyInProgressSegment(segMetadataStart101End300);
        RemoteLogSegmentMetadataUpdate segMetadataStart101End300Update = new RemoteLogSegmentMetadataUpdate(
                segIdStart101End300,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_0);
        cache.updateRemoteLogSegmentMetadata(segMetadataStart101End300Update);
        RemoteLogSegmentMetadata updatedSegment2 = segMetadataStart101End300
                .createRemoteLogSegmentWithUpdates(segMetadataStart101End300Update);

        // segment 3
        // 250 - 400
        // leader epochs (3, 250), (4, 370)
        Map<Integer, Long> seg3leaderEpochs = new HashMap<>();
        seg3leaderEpochs.put(3, 250L);
        seg3leaderEpochs.put(4, 370L);
        RemoteLogSegmentId segIdStart250End400 = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segMetFooTp0s250e400 = new RemoteLogSegmentMetadata(segIdStart250End400, 250L, 400L,
                -1L,
                BROKER_ID_0, System.currentTimeMillis(), SEG_SIZE, seg3leaderEpochs);
        cache.addCopyInProgressSegment(segMetFooTp0s250e400);
        RemoteLogSegmentMetadataUpdate segMetadataStart250End400Update = new RemoteLogSegmentMetadataUpdate(
                segIdStart250End400,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_0);
        cache.updateRemoteLogSegmentMetadata(segMetadataStart250End400Update);
        RemoteLogSegmentMetadata updatedSegment3 = segMetFooTp0s250e400
                .createRemoteLogSegmentWithUpdates(segMetadataStart250End400Update);

        //////////////////////////////////////////////////////////////////////////////////////////
        // Four segments are added with different boundaries and leader epochs.
        // Search for cache.remoteLogSegmentMetadata(leaderEpoch, offset)  for different
        // epochs and offsets
        //////////////////////////////////////////////////////////////////////////////////////////

        // Search for offset 40, epoch 1
        Optional<RemoteLogSegmentMetadata> segmentForOffset40Epoch1 = cache.remoteLogSegmentMetadata(1, 40);
        Assertions.assertEquals(Optional.of(updatedSegment0), segmentForOffset40Epoch1);

        // Search for offset 110, epoch 2
        Optional<RemoteLogSegmentMetadata> segmentForOffset110Epoch2 = cache.remoteLogSegmentMetadata(2, 110);
        Assertions.assertEquals(Optional.of(updatedSegment1), segmentForOffset110Epoch2);

        // Search for offset 110, epoch 1, and it should not exist
        Optional<RemoteLogSegmentMetadata> segmentForOffset110Epoch1 = cache.remoteLogSegmentMetadata(1, 110);
        Assertions.assertFalse(segmentForOffset110Epoch1.isPresent());

        // Search for offset 240, epoch 3
        Optional<RemoteLogSegmentMetadata> segmentForOffset240Epoch3 = cache.remoteLogSegmentMetadata(3, 240);
        Assertions.assertEquals(Optional.of(updatedSegment2), segmentForOffset240Epoch3);

        // Search for offset 250, epoch 3
        Optional<RemoteLogSegmentMetadata> segmentForOffset250Epoch3 = cache.remoteLogSegmentMetadata(3, 250);
        Assertions.assertEquals(Optional.of(updatedSegment3), segmentForOffset250Epoch3);

        // Search for offset 375, epoch 4
        Optional<RemoteLogSegmentMetadata> segmentForOffset375Epoch4 = cache.remoteLogSegmentMetadata(4, 375);
        Assertions.assertEquals(Optional.of(updatedSegment3), segmentForOffset375Epoch4);

        // Search for offset 401, epoch 4
        Optional<RemoteLogSegmentMetadata> segmentForOffset401Epoch4 = cache.remoteLogSegmentMetadata(4, 401);
        Assertions.assertFalse(segmentForOffset401Epoch4.isPresent());

        // Update segment with state as DELETE_SEGMENT_STARTED.
        // It should not be available when we search for that segment.
        cache.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(segIdStart0End100,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, BROKER_ID_1));
        Assertions.assertFalse(cache.remoteLogSegmentMetadata(10, 0).isPresent());

        // Update segment with state as DELETE_SEGMENT_FINISHED.
        // It should not be available when we search for that segment.
        cache.updateRemoteLogSegmentMetadata(
                new RemoteLogSegmentMetadataUpdate(segIdStart0End100, System.currentTimeMillis(),
                        RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, BROKER_ID_1));
        Assertions.assertFalse(cache.remoteLogSegmentMetadata(10, 0).isPresent());

        //////////////////////////////////////////////////////////////////////////////////////////
        //  Search for cache.highestLogOffset(leaderEpoch) for all the leader epochs
        //////////////////////////////////////////////////////////////////////////////////////////

        //search for highest offset for leader epoch 0
        Optional<Long> highestOffsetForEpoch0 = cache.highestOffsetForEpoch(0);
        Assertions.assertEquals(Optional.of(19L), highestOffsetForEpoch0);

        //search for highest offset for leader epoch 1
        Optional<Long> highestOffsetForEpoch1 = cache.highestOffsetForEpoch(1);
        Assertions.assertEquals(Optional.of(79L), highestOffsetForEpoch1);

        //search for highest offset for leader epoch 2
        Optional<Long> highestOffsetForEpoch2 = cache.highestOffsetForEpoch(2);
        Assertions.assertEquals(Optional.of(239L), highestOffsetForEpoch2);

        //search for highest offset for leader epoch 3
        Optional<Long> highestOffsetForEpoch3 = cache.highestOffsetForEpoch(3);
        Assertions.assertEquals(Optional.of(369L), highestOffsetForEpoch3);

        //search for highest offset for leader epoch 4
        Optional<Long> highestOffsetForEpoch4 = cache.highestOffsetForEpoch(4);
        Assertions.assertEquals(Optional.of(400L), highestOffsetForEpoch4);
    }

    @Test
    public void testCacheSegmentsWithDifferentStates() throws Exception {
        RemoteLogMetadataCache cache = new RemoteLogMetadataCache();

        // Add segments with different states and check cache.remoteLogSegmentMetadata(int leaderEpoch, long offset)
        // cache.listRemoteLogSegments(int leaderEpoch), and cache.listAllRemoteLogSegments().

        // =============================================================================================================
        // 1.Create a segment with state COPY_SEGMENT_STARTED, and check for searching that segment and listing the
        // segments.
        // ==============================================================================================================
        Map<Integer, Long> seg0LeaderEpochs = Collections.singletonMap(0, 0L);
        RemoteLogSegmentId seg0Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segCopyInProgress = new RemoteLogSegmentMetadata(seg0Id, 0L, 50L, -1L, BROKER_ID_0,
                System.currentTimeMillis(), SEG_SIZE, seg0LeaderEpochs);
        cache.addCopyInProgressSegment(segCopyInProgress);

        // This segment should not be available as the state is not reached to COPY_SEGMENT_FINISHED.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset0Epoch0 = cache.remoteLogSegmentMetadata(0, 0);
        Assertions.assertFalse(segMetadataForOffset0Epoch0.isPresent());

        // cache.listRemoteLogSegments(0) should contain the above segment.
        List<RemoteLogSegmentMetadata> expectedSegments1 = Collections.singletonList(segCopyInProgress);
        Assertions.assertTrue(sameElementsWithoutOrder(cache.listRemoteLogSegments(0), expectedSegments1.iterator()));
        // cache.listRemoteLogSegments() should contain the above segment.
        Assertions.assertTrue(sameElementsWithoutOrder(cache.listAllRemoteLogSegments(), expectedSegments1.iterator()));

        // =============================================================================================================
        // 2.Create a segment and move it to state COPY_SEGMENT_FINISHED. and check for searching that segment and
        // listing the segments.
        // ==============================================================================================================
        Map<Integer, Long> seg1LeaderEpochs = Collections.singletonMap(0, 101L);
        RemoteLogSegmentId seg1Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata seg1 = new RemoteLogSegmentMetadata(seg1Id, 101L, 200L, -1L, BROKER_ID_0,
                System.currentTimeMillis(), SEG_SIZE, seg1LeaderEpochs);
        cache.addCopyInProgressSegment(seg1);
        RemoteLogSegmentMetadataUpdate seg1Update = new RemoteLogSegmentMetadataUpdate(seg1Id,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID_0);
        cache.updateRemoteLogSegmentMetadata(seg1Update);
        RemoteLogSegmentMetadata segCopyFinished = seg1.createRemoteLogSegmentWithUpdates(seg1Update);

        // Search should return the above segment.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset150 = cache.remoteLogSegmentMetadata(0, 150);
        Assertions
                .assertEquals(seg1.createRemoteLogSegmentWithUpdates(seg1Update), segMetadataForOffset150.orElse(null));

        // cache.listRemoteLogSegments(0) should not contain the above segment.
        List<RemoteLogSegmentMetadata> expectedSegments2 = Arrays.asList(segCopyInProgress, segCopyFinished);
        Assertions.assertTrue(sameElementsWithoutOrder(cache.listRemoteLogSegments(0),
                expectedSegments2.iterator()));
        // But cache.listRemoteLogSegments() should contain both the segments.
        Assertions.assertTrue(sameElementsWithoutOrder(cache.listAllRemoteLogSegments(),
                expectedSegments2.iterator()));

        // =============================================================================================================
        // 3.Create a segment and move it to state DELETE_SEGMENT_STARTED, and check for searching that segment and
        // listing the segments.
        // ==============================================================================================================
        Map<Integer, Long> seg2leaderEpochs = Collections.singletonMap(0, 201L);
        RemoteLogSegmentId seg2Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata seg2 = new RemoteLogSegmentMetadata(seg2Id, 201L, 300L, -1L, BROKER_ID_0,
                System.currentTimeMillis(), SEG_SIZE, seg2leaderEpochs);
        cache.addCopyInProgressSegment(seg2);
        RemoteLogSegmentMetadataUpdate seg2Update = new RemoteLogSegmentMetadataUpdate(seg2Id,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, BROKER_ID_1);
        cache.updateRemoteLogSegmentMetadata(seg2Update);
        RemoteLogSegmentMetadata segDeleteStarted = seg2.createRemoteLogSegmentWithUpdates(seg2Update);

        // Search should not return the above segment as their leader epoch state is cleared.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset250Epoch0 = cache.remoteLogSegmentMetadata(0, 250);
        Assertions.assertFalse(segMetadataForOffset250Epoch0.isPresent());

        // cache.listRemoteLogSegments(0) should contain the above segment.
        List<RemoteLogSegmentMetadata> expectedSegments3 = Arrays.asList(segCopyInProgress, segCopyFinished,
                segDeleteStarted);
        Assertions.assertTrue(sameElementsWithoutOrder(cache.listRemoteLogSegments(0),
                expectedSegments3.iterator()));
        // But cache.listRemoteLogSegments() should contain all the segments.
        Assertions.assertTrue(sameElementsWithoutOrder(cache.listAllRemoteLogSegments(),
                expectedSegments3.iterator()));

        // =============================================================================================================
        // 4.Create a segment and move it to state DELETE_SEGMENT_FINISHED, and check for searching that segment and
        // listing the segments.
        // ==============================================================================================================
        Map<Integer, Long> seg3leaderEpochs = Collections.singletonMap(0, 301L);
        RemoteLogSegmentId seg3Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata seg3 = new RemoteLogSegmentMetadata(seg3Id, 301L, 400L, -1L, BROKER_ID_0,
                System.currentTimeMillis(), SEG_SIZE, seg3leaderEpochs);
        cache.addCopyInProgressSegment(seg3);
        RemoteLogSegmentMetadataUpdate seg3Update1 = new RemoteLogSegmentMetadataUpdate(seg3Id,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, BROKER_ID_1);
        cache.updateRemoteLogSegmentMetadata(seg3Update1);

        // Search should not return the above segment as their leader epoch state is cleared.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset350Epoch0 = cache.remoteLogSegmentMetadata(0, 350);
        Assertions.assertFalse(segMetadataForOffset350Epoch0.isPresent());

        RemoteLogSegmentMetadataUpdate seg3Update2 = new RemoteLogSegmentMetadataUpdate(seg3Id,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, BROKER_ID_1);
        cache.updateRemoteLogSegmentMetadata(seg3Update2);

        List<RemoteLogSegmentMetadata> expectedSegments4 = Arrays.asList(segCopyInProgress, segCopyFinished,
                segDeleteStarted);
        // cache.listRemoteLogSegments(0) should not contain the above segment.
        Assertions.assertTrue(sameElementsWithoutOrder(cache.listRemoteLogSegments(0),
                expectedSegments4.iterator()));
        // But cache.listRemoteLogSegments() should not contain both the segments as it should have been removed.
        Assertions.assertTrue(sameElementsWithoutOrder(cache.listAllRemoteLogSegments(), expectedSegments4.iterator()));

        // Search for highest offset for leader epoch 0.
        Optional<Long> highestOffsetForEpoch0 = cache.highestOffsetForEpoch(0);
        Assertions.assertEquals(Optional.of(400L), highestOffsetForEpoch0);

        // Search for highest offset for leader epoch 1, that does not exist.
        Optional<Long> highestOffsetForEpoch1 = cache.highestOffsetForEpoch(1);
        Assertions.assertFalse(highestOffsetForEpoch1.isPresent());
    }
}
