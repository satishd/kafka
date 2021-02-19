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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InmemoryRemoteLogMetadataManager implements RemoteLogMetadataManager {

    private final ConcurrentMap<TopicIdPartition, RemotePartitionDeleteMetadata> idToPartitionDeleteMetadata =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<TopicIdPartition, RemoteLogSegmentInfo> partitionToRemoteLogSegmentInfo =
            new ConcurrentHashMap<>();

    private static class RemoteLogSegmentInfo {
        private final ConcurrentMap<RemoteLogSegmentId, RemoteLogSegmentMetadata> idToSegmentMetadata
                = new ConcurrentHashMap<>();

        private final ConcurrentMap<Integer, NavigableMap<Long, RemoteLogSegmentId>> leaderEpochToOffsets =
                new ConcurrentHashMap<>();

        public RemoteLogSegmentInfo() {
        }

        public RemoteLogSegmentMetadata remoteLogSegmentMetadataForId(RemoteLogSegmentId id) {
            return idToSegmentMetadata.get(id);
        }

        private void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
            idToSegmentMetadata.put(remoteLogSegmentMetadata.remoteLogSegmentId(), remoteLogSegmentMetadata);
            Map<Integer, Long> leaderEpochToOffset = remoteLogSegmentMetadata.segmentLeaderEpochs();
            for (Map.Entry<Integer, Long> entry : leaderEpochToOffset.entrySet()) {
                leaderEpochToOffsets.computeIfAbsent(entry.getKey(), k -> new ConcurrentSkipListMap<>())
                        .put(entry.getValue(), remoteLogSegmentMetadata.remoteLogSegmentId());
            }
        }

        private Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(int leaderEpoch, long offset) {
            NavigableMap<Long, RemoteLogSegmentId> offsetToId = leaderEpochToOffsets.get(leaderEpoch);
            if (offsetToId == null || offsetToId.isEmpty()) {
                return Optional.empty();
            }

            // look for floor entry as the given offset may exist in this entry.
            Map.Entry<Long, RemoteLogSegmentId> entry = offsetToId.floorEntry(offset);
            if (entry == null) {
                // if the offset is lower than the minimum offset available in metadata then return null.
                return Optional.empty();
            }

            RemoteLogSegmentMetadata remoteLogSegmentMetadata = idToSegmentMetadata.get(entry.getValue());
            // check whether the given offset with leaderEpoch exists in this segment.
            // check for epoch's offset boundaries with in this segment.
            //      1. get the next epoch's start offset -1 if exists
            //      2. if no next epoch exists, then segment end offset can be considered as epoch's relative end offset.
            Map.Entry<Integer, Long> nextEntry = remoteLogSegmentMetadata.segmentLeaderEpochs()
                    .higherEntry(leaderEpoch);
            long epochEndOffset = (nextEntry != null) ? nextEntry.getValue() - 1 : remoteLogSegmentMetadata.endOffset();

            // seek offset should be <= epoch's end offset.
            return (offset > epochEndOffset) ? Optional.empty() : Optional.of(remoteLogSegmentMetadata);
        }

        public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate rlsmUpdate) {
            RemoteLogSegmentId remoteLogSegmentId = rlsmUpdate.remoteLogSegmentId();
            RemoteLogSegmentMetadata rlsm = idToSegmentMetadata.get(remoteLogSegmentId);

            RemoteLogSegmentMetadata updatedRlsm = new RemoteLogSegmentMetadata(remoteLogSegmentId, rlsm.startOffset(),
                    rlsm.endOffset(), rlsm.maxTimestamp(), rlsm.leaderEpoch(), rlsmUpdate.eventTimestamp(),
                    rlsm.segmentSizeInBytes(), rlsmUpdate.state(), rlsm.segmentLeaderEpochs());
            idToSegmentMetadata.put(remoteLogSegmentId, updatedRlsm);

            // remove this entry when the state is moved to delete_segment_started
            if (rlsmUpdate.state() == RemoteLogSegmentState.DELETE_SEGMENT_FINISHED) {
                Map<Integer, Long> leaderEpochs = rlsm.segmentLeaderEpochs();
                for (Map.Entry<Integer, Long> entry : leaderEpochs.entrySet()) {
                    NavigableMap<Long, RemoteLogSegmentId> offsetToIds = leaderEpochToOffsets.get(entry.getKey());
                    // remove the mappings where this segment is deleted.
                    offsetToIds.values().remove(remoteLogSegmentId);
                }

                // remove the segment-id mapping.
                idToSegmentMetadata.remove(remoteLogSegmentId);
            }
        }

        public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments() {
            return leaderEpochToOffsets.values().stream()
                    .flatMap(map -> map.values().stream())
                    .map(id -> idToSegmentMetadata.get(id))
                    .iterator();
        }

        public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(int leaderEpoch) {
            return leaderEpochToOffsets.get(leaderEpoch)
                    .values().stream()
                    .map(id -> idToSegmentMetadata.get(id)).iterator();
        }
    }

    @Override
    public void putRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();

        RemoteLogSegmentInfo remoteLogSegmentInfo = partitionToRemoteLogSegmentInfo
                .computeIfAbsent(remoteLogSegmentId.topicIdPartition(), id -> new RemoteLogSegmentInfo());
        remoteLogSegmentInfo.addRemoteLogSegmentMetadata(remoteLogSegmentMetadata);
    }

    @Override
    public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate rlsmUpdate)
            throws RemoteStorageException {
        RemoteLogSegmentId remoteLogSegmentId = rlsmUpdate.remoteLogSegmentId();
        TopicIdPartition topicIdPartition = remoteLogSegmentId.topicIdPartition();
        RemoteLogSegmentInfo remoteLogSegmentInfo = partitionToRemoteLogSegmentInfo.get(topicIdPartition);
        if (remoteLogSegmentInfo == null) {
            throw new RemoteResourceNotFoundException("No partition metadata found for : " + topicIdPartition);
        }

        RemoteLogSegmentMetadata rlsm = remoteLogSegmentInfo.remoteLogSegmentMetadataForId(remoteLogSegmentId);
        if (rlsm == null) {
            throw new RemoteResourceNotFoundException("No remote log segment metadata found for : "
                                                      + remoteLogSegmentId);
        }

        remoteLogSegmentInfo.updateRemoteLogSegmentMetadata(rlsmUpdate);

    }

    @Override
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                       long offset,
                                                                       int epochForOffset)
            throws RemoteStorageException {
        RemoteLogSegmentInfo remoteLogSegmentInfo = partitionToRemoteLogSegmentInfo.get(topicIdPartition);
        if (remoteLogSegmentInfo == null) {
            throw new RemoteResourceNotFoundException("No metadata found for the given partition: " + topicIdPartition);
        }

        return remoteLogSegmentInfo.remoteLogSegmentMetadata(epochForOffset, offset);
    }

    @Override
    public Optional<Long> highestLogOffset(TopicIdPartition topicIdPartition,
                                           int leaderEpoch) throws RemoteStorageException {
        RemoteLogSegmentInfo remoteLogSegmentInfo = partitionToRemoteLogSegmentInfo.get(topicIdPartition);
        if (remoteLogSegmentInfo == null) {
            throw new RemoteResourceNotFoundException("No resource found for partition: " + topicIdPartition);
        }

        Long highestKey = remoteLogSegmentInfo.leaderEpochToOffsets.get(leaderEpoch).lastKey();
        return Optional.ofNullable(highestKey);
    }

    @Override
    public void putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata)
            throws RemoteStorageException {
        TopicIdPartition topicIdPartition = remotePartitionDeleteMetadata.topicIdPartition();
        idToPartitionDeleteMetadata.put(topicIdPartition, remotePartitionDeleteMetadata);
        // there will be a trigger to receive delete partition marker and act on that to delete all the segments.

        if (remotePartitionDeleteMetadata.state() == RemotePartitionDeleteState.DELETE_PARTITION_FINISHED) {
            // remove the association for the partition.
            partitionToRemoteLogSegmentInfo.remove(topicIdPartition);
            idToPartitionDeleteMetadata.remove(topicIdPartition);
        }
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition)
            throws RemoteStorageException {
        RemoteLogSegmentInfo remoteLogSegmentInfo = partitionToRemoteLogSegmentInfo.get(topicIdPartition);
        if (remoteLogSegmentInfo == null) {
            throw new RemoteResourceNotFoundException("No resource found for partition: " + topicIdPartition);
        }

        return remoteLogSegmentInfo.listRemoteLogSegments();
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition, int leaderEpoch)
            throws RemoteStorageException {
        RemoteLogSegmentInfo remoteLogSegmentInfo = partitionToRemoteLogSegmentInfo.get(topicIdPartition);
        if (remoteLogSegmentInfo == null) {
            throw new RemoteResourceNotFoundException("No resource found for partition: " + topicIdPartition);
        }

        return remoteLogSegmentInfo.listRemoteLogSegments(leaderEpoch);
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                             Set<TopicIdPartition> followerPartitions) {
    }

    @Override
    public void onStopPartitions(Set<TopicIdPartition> partitions) {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
