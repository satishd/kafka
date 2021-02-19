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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class InmemoryRemoteLogMetadataManagerTest {

    private final TopicIdPartition fooTp0 = new TopicIdPartition(UUID.randomUUID(), new TopicPartition("foo", 0));

    private final int segSize = 1024 * 1024;

    @Test
    public void testRLMMFetchSegment() throws Exception {
        InmemoryRemoteLogMetadataManager rlmm = new InmemoryRemoteLogMetadataManager();

        // create remote log segment metadata and add them to RLMM.

        // segment 0
        // 0-100
        // leader epochs (0,0), (1,20), (2,80)
        Map<Integer, Long> seg0leaderEpochs = new HashMap<>();
        seg0leaderEpochs.put(0, 0L);
        seg0leaderEpochs.put(1, 20L);
        seg0leaderEpochs.put(2, 80L);
        RemoteLogSegmentId segIdFooTp0s0e100 = new RemoteLogSegmentId(fooTp0, UUID.randomUUID());
        RemoteLogSegmentMetadata segMetFooTp0s0e100 = new RemoteLogSegmentMetadata(segIdFooTp0s0e100, 0L, 100L, -1L, 2,
                System.currentTimeMillis(), segSize, RemoteLogSegmentState.COPY_SEGMENT_STARTED, seg0leaderEpochs);
        rlmm.putRemoteLogSegmentMetadata(segMetFooTp0s0e100);

        // segment 1
        // 100 - 200
        // no changes in leadership with in this segment
        // leader epochs (2, 101)
        Map<Integer, Long> seg1leaderEpochs = new HashMap<>();
        seg1leaderEpochs.put(2, 101L);
        RemoteLogSegmentId segIdFooTp0s101e200 = new RemoteLogSegmentId(fooTp0, UUID.randomUUID());
        RemoteLogSegmentMetadata segMetFooTp0s101e200 = new RemoteLogSegmentMetadata(segIdFooTp0s101e200, 101L, 200L, -1L, 2,
                System.currentTimeMillis(), segSize, RemoteLogSegmentState.COPY_SEGMENT_STARTED, seg1leaderEpochs);
        rlmm.putRemoteLogSegmentMetadata(segMetFooTp0s101e200);

        // segment 2
        // 201 - 300
        // moved to epoch 3 in between
        // leader epochs (2, 201), (3, 240)
        Map<Integer, Long> seg2leaderEpochs = new HashMap<>();
        seg2leaderEpochs.put(2, 201L);
        seg2leaderEpochs.put(3, 240L);
        RemoteLogSegmentId segIdFooTp0s101e300 = new RemoteLogSegmentId(fooTp0, UUID.randomUUID());
        RemoteLogSegmentMetadata segMetFooTp0s101e300 = new RemoteLogSegmentMetadata(segIdFooTp0s101e300, 201L, 300L, -1L, 3,
                System.currentTimeMillis(), segSize, RemoteLogSegmentState.COPY_SEGMENT_STARTED, seg2leaderEpochs);
        rlmm.putRemoteLogSegmentMetadata(segMetFooTp0s101e300);

        // segment 3
        // 250 - 400
        // leader epochs (3, 250), (4, 370)
        Map<Integer, Long> seg3leaderEpochs = new HashMap<>();
        seg3leaderEpochs.put(3, 250L);
        seg3leaderEpochs.put(4, 370L);
        RemoteLogSegmentId segIdFooTp0s250e400 = new RemoteLogSegmentId(fooTp0, UUID.randomUUID());
        RemoteLogSegmentMetadata segMetFooTp0s250e400 = new RemoteLogSegmentMetadata(segIdFooTp0s250e400, 250L, 400L, -1L, 3,
                System.currentTimeMillis(), segSize, RemoteLogSegmentState.COPY_SEGMENT_STARTED, seg3leaderEpochs);
        rlmm.putRemoteLogSegmentMetadata(segMetFooTp0s250e400);

        // search for offset 40, epoch 1
        Optional<RemoteLogSegmentMetadata> segO40E1 = rlmm.remoteLogSegmentMetadata(fooTp0, 40, 1);
        Assertions.assertEquals(segMetFooTp0s0e100, segO40E1.orElse(null));

        // search for offset 110, epoch 2
        Optional<RemoteLogSegmentMetadata> segO110E2 = rlmm.remoteLogSegmentMetadata(fooTp0, 110, 2);
        Assertions.assertEquals(segMetFooTp0s101e200, segO110E2.orElse(null));

        // search for offset 110, epoch 1, and it should not exist
        Optional<RemoteLogSegmentMetadata> segO110E1 = rlmm.remoteLogSegmentMetadata(fooTp0, 110, 1);
        Assertions.assertFalse(segO110E1.isPresent());

        //search for offset 240, epoch 3
        Optional<RemoteLogSegmentMetadata> segO2400E3 = rlmm.remoteLogSegmentMetadata(fooTp0, 240, 3);
        Assertions.assertEquals(segMetFooTp0s101e300, segO2400E3.orElse(null));

        // search for offset 250, epoch 3
        Optional<RemoteLogSegmentMetadata> segO2500E3 = rlmm.remoteLogSegmentMetadata(fooTp0, 250, 3);
        Assertions.assertEquals(segMetFooTp0s250e400, segO2500E3.orElse(null));

        // search for offset 375, epoch 4
        Optional<RemoteLogSegmentMetadata> segO3750E4 = rlmm.remoteLogSegmentMetadata(fooTp0, 375, 4);
        Assertions.assertEquals(segMetFooTp0s250e400, segO3750E4.orElse(null));

        // search for offset 401, epoch 4
        Optional<RemoteLogSegmentMetadata> segO4010E4 = rlmm.remoteLogSegmentMetadata(fooTp0, 401, 4);
        Assertions.assertFalse(segO4010E4.isPresent());
    }

    @Test
    public void testRemotePartitionDeletion() throws Exception {
        InmemoryRemoteLogMetadataManager rlmm = new InmemoryRemoteLogMetadataManager();

        // create remote log segment metadata and add them to RLMM.

        // segment 0
        // 0-100
        // leader epochs (0,0), (1,20), (2,80)
        Map<Integer, Long> seg0leaderEpochs = new HashMap<>();
        seg0leaderEpochs.put(0, 0L);
        seg0leaderEpochs.put(1, 20L);
        seg0leaderEpochs.put(2, 50L);
        seg0leaderEpochs.put(3, 80L);
        RemoteLogSegmentId segIdFooTp0s0e100 = new RemoteLogSegmentId(fooTp0, UUID.randomUUID());
        RemoteLogSegmentMetadata segMetFooTp0s0e100 = new RemoteLogSegmentMetadata(segIdFooTp0s0e100, 0L, 100L, -1L, 3,
                System.currentTimeMillis(), segSize, RemoteLogSegmentState.COPY_SEGMENT_STARTED, seg0leaderEpochs);
        rlmm.putRemoteLogSegmentMetadata(segMetFooTp0s0e100);

        // check that the seg exists in RLMM
        Optional<RemoteLogSegmentMetadata> seg0s0e100 = rlmm.remoteLogSegmentMetadata(fooTp0, 30L, 1);
        Assertions.assertEquals(segMetFooTp0s0e100, seg0s0e100.orElse(null));

        // mark the partition for deletion. partition and segments should still be accessible.
        rlmm.putRemotePartitionDeleteMetadata(new RemotePartitionDeleteMetadata(fooTp0,
                RemotePartitionDeleteState.DELETE_PARTITION_MARKED, System.currentTimeMillis(), 1));

        Optional<RemoteLogSegmentMetadata> seg0s0e100AfterDelMark = rlmm.remoteLogSegmentMetadata(fooTp0, 30L, 1);
        Assertions.assertEquals(segMetFooTp0s0e100, seg0s0e100AfterDelMark.orElse(null));

        // set the partition deletion state as started. Partition and segments should still be accessible as they are not
        // yet deleted.
        rlmm.putRemotePartitionDeleteMetadata(new RemotePartitionDeleteMetadata(fooTp0,
                RemotePartitionDeleteState.DELETE_PARTITION_STARTED, System.currentTimeMillis(), 1));

        Optional<RemoteLogSegmentMetadata> seg0s0e100AfterDelStart = rlmm.remoteLogSegmentMetadata(fooTp0, 30L, 1);
        Assertions.assertEquals(segMetFooTp0s0e100, seg0s0e100AfterDelStart.orElse(null));

        // set the partition deletion state as finished. RLMM should clear all its internal state for that partition.
        rlmm.putRemotePartitionDeleteMetadata(new RemotePartitionDeleteMetadata(fooTp0,
                RemotePartitionDeleteState.DELETE_PARTITION_FINISHED, System.currentTimeMillis(), 1));

        Assertions.assertThrows(RemoteResourceNotFoundException.class,
            () -> rlmm.remoteLogSegmentMetadata(fooTp0, 30L, 1));
    }
}
