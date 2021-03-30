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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * This class represents the in-memory state of segments associated with a leader epoch. This includes the mapping of offset to
 * segment ids and unreferenced segments which are not mapped to any offset but they exist in remote storage.
 * <p>
 * This is used by {@link RemoteLogMetadataCache} to track the segments for each leader epoch.
 */
class RemoteLogLeaderEpochState {

    // It contains offset to segment ids mapping with the segment state as COPY_SEGMENT_FINISHED.
    private final NavigableMap<Long, RemoteLogSegmentId> offsetToId = new ConcurrentSkipListMap<>();

    /**
     * It represents unreferenced segments for this leader epoch. It contains the segments still in COPY_SEGMENT_STARTED
     * and DELETE_SEGMENT_STARTED state or these have been replaced by callers with other segments having the same
     * start offset for the leader epoch. These will be returned by {@link RemoteLogMetadataCache#listAllRemoteLogSegments()}
     * and {@link RemoteLogMetadataCache#listRemoteLogSegments(int leaderEpoch)} so that callers can clean them up if
     * they still exist. These will be cleaned from the cache once they reach DELETE_SEGMENT_FINISHED state.
     */
    private final Set<RemoteLogSegmentId> unreferencedSegmentIds = ConcurrentHashMap.newKeySet();

    // It represents the highest log offset of the segments that were updated with updateHighestLogOffset.
    private volatile Long highestLogOffset;

    /**
     * Returns all the segments associated with this leader epoch sorted by start offset in ascending order.
     *
     * @param idToSegmentMetadata mapping of id to segment metadata. This will be used to get RemoteLogSegmentMetadata
     *                            for an id to be used for sorting.
     * @return
     */
    Iterator<RemoteLogSegmentMetadata> listAllRemoteLogSegments(Map<RemoteLogSegmentId, RemoteLogSegmentMetadata> idToSegmentMetadata) {
        // Return all the segments including unreferenced metadata.
        int size = offsetToId.size() + unreferencedSegmentIds.size();
        if (size == 0) {
            return Collections.emptyIterator();
        }

        ArrayList<RemoteLogSegmentMetadata> metadataList = new ArrayList<>(size);
        for (RemoteLogSegmentId id : offsetToId.values()) {
            metadataList.add(idToSegmentMetadata.get(id));
        }

        if (!unreferencedSegmentIds.isEmpty()) {
            for (RemoteLogSegmentId id : unreferencedSegmentIds) {
                metadataList.add(idToSegmentMetadata.get(id));
            }

            // sort only when unreferenced entries exist as they are already sorted in offsetToId.
            metadataList.sort(Comparator.comparingLong(RemoteLogSegmentMetadata::startOffset));
        }

        return metadataList.iterator();
    }

    void handleSegmentWithCopySegmentFinishedState(Long startOffset, RemoteLogSegmentId remoteLogSegmentId,
                                                   Long leaderEpochEndOffset) {
        // Add the segment epochs mapping as the segment is copied successfully.
        RemoteLogSegmentId oldEntry = offsetToId.put(startOffset, remoteLogSegmentId);

        // Remove the metadata from unreferenced entries as it is successfully copied and added to the offset mapping.
        unreferencedSegmentIds.remove(remoteLogSegmentId);

        // Add the old entry to unreferenced entries as the mapping is removed for the old entry.
        if (oldEntry != null) {
            unreferencedSegmentIds.add(oldEntry);
        }

        // update the highest offset entry for this leader epoch as we added a new mapping.
        updateHighestLogOffset(leaderEpochEndOffset);
    }

    void handleSegmentWithDeleteSegmentStartedState(Long startOffset, RemoteLogSegmentId remoteLogSegmentId,
                                                    Long leaderEpochEndOffset) {
        // Remove the offset mappings as this segment is getting deleted.
        offsetToId.remove(startOffset, remoteLogSegmentId);

        // Add this entry to unreferenced set for the leader epoch as it is being deleted.
        // This allows any retries of deletion as these are returned from listAllSegments and listSegments(leaderEpoch).
        unreferencedSegmentIds.add(remoteLogSegmentId);

        // Update the highest offset entry for this leader epoch. This needs to be done as a segment can reach this
        // state without going through COPY_SEGMENT_FINISHED state.
        updateHighestLogOffset(leaderEpochEndOffset);
    }

    void handleSegmentWithDeleteSegmentFinishedState(long startOffset, RemoteLogSegmentId remoteLogSegmentId,
                                                     Long leaderEpochEndOffset) {
        // It completely removes the tracking of this segment as it is considered as deleted.
        unreferencedSegmentIds.remove(remoteLogSegmentId);

        // We do not need to update the highest offset entry with this segment as it would have already been processed
        // as part of the earlier state of DELETE_SEGMENT_STARTED.
    }

    void handleSegmentWithCopySegmentStartedState(RemoteLogSegmentId remoteLogSegmentId) {
        unreferencedSegmentIds.add(remoteLogSegmentId);
    }

    Long highestLogOffset() {
        return highestLogOffset;
    }

    private void updateHighestLogOffset(long offset) {
        if (highestLogOffset == null || offset > highestLogOffset) {
            highestLogOffset = offset;
        }
    }

    /**
     * Returns the RemoteLogSegmentId of a segment for the given offset, if there exists a mapping associated with
     * the greatest offset less than or equal to the given offset, or null if there is no such mapping.
     *
     * @param offset offset
     * @return
     */
    RemoteLogSegmentId floorEntry(long offset) {
        Map.Entry<Long, RemoteLogSegmentId> entry = offsetToId.floorEntry(offset);

        return entry == null ? null : entry.getValue();
    }

    /**
     * Action interface to act on remote log segment transition for the given {@link RemoteLogLeaderEpochState}.
     */
    @FunctionalInterface
    interface Action {

        /**
         * Performs this operation with the given {@code remoteLogLeaderEpochState}.
         *
         * @param remoteLogLeaderEpochState In-memory state of the segments for a leader epoch.
         * @param startOffset               start offset of the segment.
         * @param segmentId                 segment id.
         * @param leaderEpochEndOffset      end offset for the given leader epoch.
         */
        void accept(RemoteLogLeaderEpochState remoteLogLeaderEpochState,
                    Long startOffset,
                    RemoteLogSegmentId segmentId,
                    Long leaderEpochEndOffset);
    }

}
