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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This class covers basic unit tests for {@link InmemoryRemoteLogMetadataManager}. InmemoryRemoteLogMetadataManager is
 * used only in integration tests but not in production code. It mostly uses {@link RemoteLogMetadataCache} and it has
 * broad test coverage with {@link RemoteLogMetadataCacheTest}.
 */
public class InmemoryRemoteLogMetadataManagerTest {

    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition("foo", 0));
    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID_0 = 0;

    @Test
    public void testFetchSegments() throws Exception {
        InmemoryRemoteLogMetadataManager rlmm = new InmemoryRemoteLogMetadataManager();

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
        RemoteLogSegmentMetadata segCopyFinished = seg1.createWithUpdates(seg1Update);

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
                .createWithUpdates(segMetadataStart0End100Update);

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