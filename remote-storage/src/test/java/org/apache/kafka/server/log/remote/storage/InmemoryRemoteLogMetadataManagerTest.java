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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class InmemoryRemoteLogMetadataManagerTest {

    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition("foo", 0));
    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID = 0;

    @Test
    public void testRLMMFetchSegment() throws Exception {
        InmemoryRemoteLogMetadataManager rlmm = new InmemoryRemoteLogMetadataManager();
        int brokerId = 0;
        // Create remote log segment metadata and add them to RLMM.

        // segment 0
        // 0-100
        // leader epochs (0,0), (1,20), (2,80)
        Map<Integer, Long> seg0LeaderEpochs = new HashMap<>();
        seg0LeaderEpochs.put(0, 0L);
        seg0LeaderEpochs.put(1, 20L);
        seg0LeaderEpochs.put(2, 80L);
        RemoteLogSegmentId segIdStart0End100 = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segMetadataStart0End100 = new RemoteLogSegmentMetadata(segIdStart0End100, 0L, 100L, -1L, BROKER_ID,
                System.currentTimeMillis(), SEG_SIZE, seg0LeaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(segMetadataStart0End100);

        // We should not get this as the segment is still getting copied and it is not yet considered successful until
        // it reaches RemoteLogSegmentState.COPY_SEGMENT_FINISHED.
        Assertions.assertFalse(rlmm.remoteLogSegmentMetadata(TP0, 40, 1).isPresent());

        RemoteLogSegmentMetadataUpdate segMetadataStart0End100Update = new RemoteLogSegmentMetadataUpdate(segIdStart0End100,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);
        rlmm.updateRemoteLogSegmentMetadata(segMetadataStart0End100Update);

        // segment 1
        // 100 - 200
        // no changes in leadership with in this segment
        // leader epochs (2, 101)
        Map<Integer, Long> seg1LeaderEpochs = new HashMap<>();
        seg1LeaderEpochs.put(2, 101L);
        RemoteLogSegmentId segIdStart101End200 = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segMetadataStart101End200 = new RemoteLogSegmentMetadata(segIdStart101End200, 101L, 200L, -1L, BROKER_ID,
                System.currentTimeMillis(), SEG_SIZE, seg1LeaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(segMetadataStart101End200);
        RemoteLogSegmentMetadataUpdate segMetadataStart101End200Update = new RemoteLogSegmentMetadataUpdate(segIdStart101End200,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);
        rlmm.updateRemoteLogSegmentMetadata(segMetadataStart101End200Update);

        // segment 2
        // 201 - 300
        // moved to epoch 3 in between
        // leader epochs (2, 201), (3, 240)
        Map<Integer, Long> seg2LeaderEpochs = new HashMap<>();
        seg2LeaderEpochs.put(2, 201L);
        seg2LeaderEpochs.put(3, 240L);
        RemoteLogSegmentId segIdStart101End300 = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segMetadataStart101End300 = new RemoteLogSegmentMetadata(segIdStart101End300, 201L, 300L, -1L, 3,
                System.currentTimeMillis(), SEG_SIZE, seg2LeaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(segMetadataStart101End300);
        RemoteLogSegmentMetadataUpdate segMetadataStart101End300Update = new RemoteLogSegmentMetadataUpdate(segIdStart101End300,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);
        rlmm.updateRemoteLogSegmentMetadata(segMetadataStart101End300Update);

        // segment 3
        // 250 - 400
        // leader epochs (3, 250), (4, 370)
        Map<Integer, Long> seg3leaderEpochs = new HashMap<>();
        seg3leaderEpochs.put(3, 250L);
        seg3leaderEpochs.put(4, 370L);
        RemoteLogSegmentId segIdStart250End400 = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segMetFooTp0s250e400 = new RemoteLogSegmentMetadata(segIdStart250End400, 250L, 400L, -1L, BROKER_ID,
                System.currentTimeMillis(), SEG_SIZE, seg3leaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(segMetFooTp0s250e400);
        RemoteLogSegmentMetadataUpdate segMetadataStart250End400Update = new RemoteLogSegmentMetadataUpdate(segIdStart250End400,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);
        rlmm.updateRemoteLogSegmentMetadata(segMetadataStart250End400Update);

        //////////////////////////////////////////////////////////////////////////////////////////
        //  Search for RLMM.remoteLogSegmentMetadata(TP, offset, leaderEpoch)  for different
        // epochs and offsets
        //////////////////////////////////////////////////////////////////////////////////////////

        // Search for offset 40, epoch 1
        Optional<RemoteLogSegmentMetadata> segmentForOffset40Epoch1 = rlmm.remoteLogSegmentMetadata(TP0, 40, 1);
        Assertions.assertEquals(
                Optional.of(segMetadataStart0End100.createRemoteLogSegmentWithUpdates(segMetadataStart0End100Update)),
                segmentForOffset40Epoch1);

        // Search for offset 110, epoch 2
        Optional<RemoteLogSegmentMetadata> segmentForOffset110Epoch2 = rlmm.remoteLogSegmentMetadata(TP0, 110, 2);
        Assertions.assertEquals(
                Optional.of(segMetadataStart101End200.createRemoteLogSegmentWithUpdates(segMetadataStart101End200Update)),
                segmentForOffset110Epoch2);

        // Search for offset 110, epoch 1, and it should not exist
        Optional<RemoteLogSegmentMetadata> segmentForOffset110Epoch1 = rlmm.remoteLogSegmentMetadata(TP0, 110, 1);
        Assertions.assertFalse(segmentForOffset110Epoch1.isPresent());

        // Search for offset 240, epoch 3
        Optional<RemoteLogSegmentMetadata> segmentForOffset240Epoch3 = rlmm.remoteLogSegmentMetadata(TP0, 240, 3);
        Assertions.assertEquals(
                Optional.of(segMetadataStart101End300.createRemoteLogSegmentWithUpdates(segMetadataStart101End300Update)),
                segmentForOffset240Epoch3);

        // Search for offset 250, epoch 3
        Optional<RemoteLogSegmentMetadata> segmentForOffset250Epoch3 = rlmm.remoteLogSegmentMetadata(TP0, 250, 3);
        Assertions.assertEquals(
                Optional.of(segMetFooTp0s250e400.createRemoteLogSegmentWithUpdates(segMetadataStart250End400Update)),
                segmentForOffset250Epoch3);

        //search for highest offset for leader epoch 3
        Optional<Long> highestOffsetForEpoch3 = rlmm.highestLogOffset(TP0, 3);
        Assertions.assertEquals(Optional.of(369L), highestOffsetForEpoch3);

        // Search for offset 375, epoch 4
        Optional<RemoteLogSegmentMetadata> segmentForOffset375Epoch4 = rlmm.remoteLogSegmentMetadata(TP0, 375, 4);
        Assertions.assertEquals(
                Optional.of(segMetFooTp0s250e400.createRemoteLogSegmentWithUpdates(segMetadataStart250End400Update)),
                segmentForOffset375Epoch4);

        // Search for offset 401, epoch 4
        Optional<RemoteLogSegmentMetadata> segmentForOffset401Epoch4 = rlmm.remoteLogSegmentMetadata(TP0, 401, 4);
        Assertions.assertFalse(segmentForOffset401Epoch4.isPresent());

        //////////////////////////////////////////////////////////////////////////////////////////
        //  Search for RLMM.highestLogOffset(TP, leaderEpoch)  for all the leader epochs
        //////////////////////////////////////////////////////////////////////////////////////////

        //search for highest offset for leader epoch 0
        Optional<Long> highestOffsetForEpoch0 = rlmm.highestLogOffset(TP0, 0);
        Assertions.assertEquals(Optional.of(19L), highestOffsetForEpoch0);

        //search for highest offset for leader epoch 1
        Optional<Long> highestOffsetForEpoch1 = rlmm.highestLogOffset(TP0, 1);
        Assertions.assertEquals(Optional.of(79L), highestOffsetForEpoch1);

        //search for highest offset for leader epoch 2
        Optional<Long> highestOffsetForEpoch2 = rlmm.highestLogOffset(TP0, 2);
        Assertions.assertEquals(Optional.of(239L), highestOffsetForEpoch2);

        //search for highest offset for leader epoch 4
        Optional<Long> highestOffsetForEpoch4 = rlmm.highestLogOffset(TP0, 4);
        Assertions.assertEquals(Optional.of(400L), highestOffsetForEpoch4);

        // Update segment with state as DELETE_SEGMENT_STARTED.
        // It should not be available when we search for that segment.
        RemoteLogSegmentMetadataUpdate rlsmUpdate = new RemoteLogSegmentMetadataUpdate(segIdStart0End100,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, 0);
        rlmm.updateRemoteLogSegmentMetadata(rlsmUpdate);

        Assertions.assertFalse(rlmm.remoteLogSegmentMetadata(TP0, 10, 0).isPresent());

        // Update segment with state as DELETE_SEGMENT_FINISHED.
        // It should not be available when we search for that segment.
        rlmm.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(segIdStart0End100, System.currentTimeMillis(),
                RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, 0));
        Assertions.assertFalse(rlmm.remoteLogSegmentMetadata(TP0, 10, 0).isPresent());
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
        RemoteLogSegmentMetadata segMetadataStart0End100 = new RemoteLogSegmentMetadata(segIdStart0End100, 0L, 100L, -1L, BROKER_ID,
                System.currentTimeMillis(), SEG_SIZE, seg0leaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(segMetadataStart0End100);
        RemoteLogSegmentMetadataUpdate segMetadataStart0End100Update = new RemoteLogSegmentMetadataUpdate(segIdStart0End100,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0);
        rlmm.updateRemoteLogSegmentMetadata(segMetadataStart0End100Update);

        RemoteLogSegmentMetadata expectedSegMetadataStart0End100 = segMetadataStart0End100.createRemoteLogSegmentWithUpdates(segMetadataStart0End100Update);

        // Check that the seg exists in RLMM
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset30Epoch1 = rlmm.remoteLogSegmentMetadata(TP0, 30L, 1);
        Assertions.assertEquals(Optional.of(expectedSegMetadataStart0End100), segMetadataForOffset30Epoch1);

        // Mark the partition for deletion. RLMM should clear all its internal state for that partition.
        rlmm.putRemotePartitionDeleteMetadata(createRemotePartitionDeleteMetadata(RemotePartitionDeleteState.DELETE_PARTITION_MARKED));

        Optional<RemoteLogSegmentMetadata> segMetadataForStart0End100AfterDelMark = rlmm.remoteLogSegmentMetadata(TP0, 30L, 1);
        Assertions.assertEquals(Optional.of(expectedSegMetadataStart0End100), segMetadataForStart0End100AfterDelMark);

        // Set the partition deletion state as started. Partition and segments should still be accessible as they are not
        // yet deleted.
        rlmm.putRemotePartitionDeleteMetadata(createRemotePartitionDeleteMetadata(RemotePartitionDeleteState.DELETE_PARTITION_STARTED));

        Optional<RemoteLogSegmentMetadata> segMetadataForStart0End100AfterDelStart = rlmm.remoteLogSegmentMetadata(TP0, 30L, 1);
        Assertions.assertEquals(Optional.of(expectedSegMetadataStart0End100), segMetadataForStart0End100AfterDelStart);

        // Set the partition deletion state as finished. RLMM should clear all its internal state for that partition.
        rlmm.putRemotePartitionDeleteMetadata(createRemotePartitionDeleteMetadata(RemotePartitionDeleteState.DELETE_PARTITION_FINISHED));

        Assertions.assertThrows(RemoteResourceNotFoundException.class,
            () -> rlmm.remoteLogSegmentMetadata(TP0, 30L, 1));
    }

    private RemotePartitionDeleteMetadata createRemotePartitionDeleteMetadata(RemotePartitionDeleteState state) {
        return new RemotePartitionDeleteMetadata(TP0, state, System.currentTimeMillis(), BROKER_ID);
    }
}
