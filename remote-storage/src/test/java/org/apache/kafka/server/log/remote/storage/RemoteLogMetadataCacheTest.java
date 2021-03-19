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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RemoteLogMetadataCacheTest {

    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
            new TopicPartition("foo", 0));
    private static final int SEG_SIZE = 1024 * 1024;
    private static final int BROKER_ID = 0;

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
        RemoteLogSegmentMetadata segCopyInProgress = new RemoteLogSegmentMetadata(seg0Id, 0L, 50L, -1L, BROKER_ID,
                System.currentTimeMillis(), SEG_SIZE, seg0LeaderEpochs);
        cache.addCopyInProgressSegment(segCopyInProgress);

        // This segment should not be available as the state is not reached to COPY_SEGMENT_FINISHED.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset0Epoch0 = cache.remoteLogSegmentMetadata(0, 0);
        Assertions.assertFalse(segMetadataForOffset0Epoch0.isPresent());

        // cache.listRemoteLogSegments(0) should contain the above segment.
        checkContainsAll(cache.listRemoteLogSegments(0), Collections.singletonList(segCopyInProgress).iterator(), true);
        // But cache.listRemoteLogSegments() should contain the above segment.
        checkContainsAll(cache.listAllRemoteLogSegments(), Collections.singletonList(segCopyInProgress).iterator());

        // =============================================================================================================
        // 2.Create a segment and move it to state COPY_SEGMENT_FINISHED. and check for searching that segment and
        // listing the segments.
        // ==============================================================================================================
        Map<Integer, Long> seg1LeaderEpochs = Collections.singletonMap(0, 101L);
        RemoteLogSegmentId seg1Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata seg1 = new RemoteLogSegmentMetadata(seg1Id, 101L, 200L, -1L, BROKER_ID,
                System.currentTimeMillis(), SEG_SIZE, seg1LeaderEpochs);
        cache.addCopyInProgressSegment(seg1);
        RemoteLogSegmentMetadataUpdate seg1Update = new RemoteLogSegmentMetadataUpdate(seg1Id,
                System.currentTimeMillis(), RemoteLogSegmentState.COPY_SEGMENT_FINISHED, BROKER_ID);
        cache.updateRemoteLogSegmentMetadata(seg1Update);
        RemoteLogSegmentMetadata segCopyFinished = seg1.createRemoteLogSegmentWithUpdates(seg1Update);

        // Search should return the above segment.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset150 = cache.remoteLogSegmentMetadata(0, 150);
        Assertions.assertEquals(seg1.createRemoteLogSegmentWithUpdates(seg1Update), segMetadataForOffset150.orElse(null));

        // cache.listRemoteLogSegments(0) should not contain the above segment.
        checkContainsAll(cache.listRemoteLogSegments(0),
                Arrays.asList(segCopyInProgress, segCopyFinished).iterator(), true);
        // But cache.listRemoteLogSegments() should contain both the segments.
        checkContainsAll(cache.listAllRemoteLogSegments(), Arrays.asList(segCopyInProgress, segCopyFinished).iterator());

        // =============================================================================================================
        // 3.Create a segment and move it to state DELETE_SEGMENT_STARTED, and check for searching that segment and
        // listing the segments.
        // ==============================================================================================================
        Map<Integer, Long> seg2leaderEpochs = Collections.singletonMap(0, 201L);
        RemoteLogSegmentId seg2Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata seg2 = new RemoteLogSegmentMetadata(seg2Id, 201L, 300L, -1L, BROKER_ID,
                System.currentTimeMillis(), SEG_SIZE, seg2leaderEpochs);
        cache.addCopyInProgressSegment(seg2);
        RemoteLogSegmentMetadataUpdate seg2Update = new RemoteLogSegmentMetadataUpdate(seg2Id,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, BROKER_ID);
        cache.updateRemoteLogSegmentMetadata(seg2Update);
        RemoteLogSegmentMetadata segDeleteStarted = seg2.createRemoteLogSegmentWithUpdates(seg2Update);

        // Search should not return the above segment as their leader epoch state is cleared.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset250Epoch0 = cache.remoteLogSegmentMetadata(0, 250);
        Assertions.assertFalse(segMetadataForOffset250Epoch0.isPresent());

        // cache.listRemoteLogSegments(0) should contain the above segment.
        checkContainsAll(cache.listRemoteLogSegments(0),
                Arrays.asList(segCopyInProgress, segCopyFinished, segDeleteStarted).iterator(), true);
        // But cache.listRemoteLogSegments() should contain all the segments.
        checkContainsAll(cache.listAllRemoteLogSegments(),
                Arrays.asList(segCopyInProgress, segCopyFinished, segDeleteStarted).iterator());

        // =============================================================================================================
        // 4.Create a segment and move it to state DELETE_SEGMENT_FINISHED, and check for searching that segment and
        // listing the segments.
        // ==============================================================================================================
        Map<Integer, Long> seg3leaderEpochs = Collections.singletonMap(0, 301L);
        RemoteLogSegmentId seg3Id = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata seg3 = new RemoteLogSegmentMetadata(seg3Id, 301L, 400L, -1L, BROKER_ID,
                System.currentTimeMillis(), SEG_SIZE, seg3leaderEpochs);
        cache.addCopyInProgressSegment(seg3);
        RemoteLogSegmentMetadataUpdate seg3Update1 = new RemoteLogSegmentMetadataUpdate(seg3Id,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, BROKER_ID);
        cache.updateRemoteLogSegmentMetadata(seg3Update1);

        // Search should not return the above segment as their leader epoch state is cleared.
        Optional<RemoteLogSegmentMetadata> segMetadataForOffset350Epoch0 = cache.remoteLogSegmentMetadata(0, 350);
        Assertions.assertFalse(segMetadataForOffset350Epoch0.isPresent());

        RemoteLogSegmentMetadataUpdate seg3Update2 = new RemoteLogSegmentMetadataUpdate(seg3Id,
                System.currentTimeMillis(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, BROKER_ID);
        cache.updateRemoteLogSegmentMetadata(seg3Update2);

        // cache.listRemoteLogSegments(0) should not contain the above segment.
        checkContainsAll(cache.listRemoteLogSegments(0),
                Arrays.asList(segCopyInProgress, segCopyFinished, segDeleteStarted).iterator(), true);
        // But cache.listRemoteLogSegments() should not contain both the segments as it should have been removed.
        checkContainsAll(cache.listAllRemoteLogSegments(),
                Arrays.asList(segCopyInProgress, segCopyFinished, segDeleteStarted).iterator());
    }
    private void checkContainsAll(Iterator<RemoteLogSegmentMetadata> allSegments,
                                  Iterator<RemoteLogSegmentMetadata> expectedSegments) {
        checkContainsAll(allSegments, expectedSegments, false);
    }

    private void checkContainsAll(Iterator<RemoteLogSegmentMetadata> allSegments,
                                  Iterator<RemoteLogSegmentMetadata> expectedSegments,
                                  boolean checkOrder) {
        boolean sameElements;
        if (checkOrder) {
            sameElements = iteratorsEqual(allSegments, expectedSegments);
        } else {
            Set<RemoteLogSegmentMetadata> allSegmentsSet = new HashSet<>();
            allSegments.forEachRemaining(allSegmentsSet::add);
            Set<RemoteLogSegmentMetadata> expectedSegmentsSet = new HashSet<>();
            expectedSegments.forEachRemaining(expectedSegmentsSet::add);
            sameElements = allSegmentsSet.equals(expectedSegmentsSet);
        }
        Assertions.assertTrue(sameElements);
    }

    public static boolean iteratorsEqual(Iterator<?> iterator1, Iterator<?> iterator2) {
        while (iterator1.hasNext()) {
            if (!iterator2.hasNext()) {
                return false;
            }
            Object o1 = iterator1.next();
            Object o2 = iterator2.next();
            if (!Objects.equals(o1, o2)) {
                return false;
            }
        }

        return !iterator2.hasNext();
    }

}
