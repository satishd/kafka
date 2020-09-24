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

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * It describes the metadata about a topic partition's log segment in the remote storage. This is uniquely represented
 * with {@link RemoteLogSegmentId}.
 */
public class RemoteLogSegmentMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Universally unique remote log segment id.
     */
    private final RemoteLogSegmentId remoteLogSegmentId;

    /**
     * Start offset of this segment.
     */
    private final long startOffset;

    /**
     * End offset of this segment.
     */
    private final long endOffset;

    /**
     * Leader epoch of the broker.
     */
    private final int leaderEpoch;

    /**
     * Maximum timestamp in the segment
     */
    private final long maxTimestamp;

    /**
     * Epoch time at which the respective {@link #state} is set.
     */
    private final long eventTimestamp;

    /**
     * LeaderEpoch vs offset for messages with in this segment.
     */
    private final Map<Integer, Long> segmentLeaderEpochs;

    /**
     * Size of the segment in bytes.
     */
    private final int segmentSizeInBytes;

    /**
     * It indicates the state in which the action is executed on this segment.
     */
    private final RemoteLogSegmentState state;

    /**
     * @param remoteLogSegmentId  Universally unique remote log segment id.
     * @param startOffset         Start offset of this segment.
     * @param endOffset           End offset of this segment.
     * @param maxTimestamp        Maximum timestamp in this segment
     * @param leaderEpoch         Leader epoch of the broker.
     * @param eventTimestamp      Epoch time at which the remote log segment is copied to the remote tier storage.
     * @param segmentSizeInBytes  Size of this segment in bytes.
     * @param state               State of the respective segment of remoteLogSegmentId.
     * @param segmentLeaderEpochs leader epochs occurred with in this segment
     */
    public RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId, long startOffset, long endOffset,
                                    long maxTimestamp, int leaderEpoch, long eventTimestamp,
                                    int segmentSizeInBytes, RemoteLogSegmentState state, Map<Integer, Long> segmentLeaderEpochs) {
        this.remoteLogSegmentId = remoteLogSegmentId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.leaderEpoch = leaderEpoch;
        this.maxTimestamp = maxTimestamp;
        this.eventTimestamp = eventTimestamp;
        this.segmentLeaderEpochs = segmentLeaderEpochs;
        this.state = state;
        this.segmentSizeInBytes = segmentSizeInBytes;
    }

    /**
     * @return unique id of this segment.
     */
    public RemoteLogSegmentId remoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    /**
     * @return Start offset of this segment(inclusive).
     */
    public long startOffset() {
        return startOffset;
    }

    /**
     * @return End offset of this segment(inclusive).
     */
    public long endOffset() {
        return endOffset;
    }

    /**
     * @return Epoch time at which this evcent is occurred.
     */
    public long eventTimestamp() {
        return eventTimestamp;
    }

    /**
     * @return total size of this segment in bytes.
     */
    public int segmentSizeInBytes() {
        return segmentSizeInBytes;
    }

    /**
     * @return maximum timestamp of a record with in this segment.
     */
    public long maxTimestamp() {
        return maxTimestamp;
    }

    /**
     * @return Map of leader epoch vs offset for the records available in this segment.
     */
    public Map<Integer, Long> segmentLeaderEpochs() {
        return segmentLeaderEpochs;
    }

    /**
     * @return Leader or controller epoch of the broker from where this event occurred.
     */
    public int leaderEpoch() {
        return leaderEpoch;
    }

    /**
     * Returns the current state of this remote log segment. It can be any of the below
     * <ul>
     *     {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED}
     *     {@link RemoteLogSegmentState#COPY_SEGMENT_FINISHED}
     *     {@link RemoteLogSegmentState#DELETE_SEGMENT_STARTED}
     *     {@link RemoteLogSegmentState#DELETE_SEGMENT_FINISHED}
     * </ul>
     */
    public RemoteLogSegmentState state() {
        return state;
    }

    //todo-tier remove this!!
    public boolean markedForDeletion() {
        return state == RemoteLogSegmentState.DELETE_SEGMENT_STARTED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteLogSegmentMetadata that = (RemoteLogSegmentMetadata) o;
        return startOffset == that.startOffset && endOffset == that.endOffset && leaderEpoch == that.leaderEpoch && maxTimestamp == that.maxTimestamp && eventTimestamp == that.eventTimestamp && segmentSizeInBytes == that.segmentSizeInBytes && Objects.equals(remoteLogSegmentId, that.remoteLogSegmentId) && Objects.equals(segmentLeaderEpochs, that.segmentLeaderEpochs) && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentId, startOffset, endOffset, leaderEpoch, maxTimestamp, eventTimestamp, segmentLeaderEpochs, segmentSizeInBytes, state);
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentMetadata{" +
                "remoteLogSegmentId=" + remoteLogSegmentId +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", leaderEpoch=" + leaderEpoch +
                ", maxTimestamp=" + maxTimestamp +
                ", eventTimestamp=" + eventTimestamp +
                ", segmentLeaderEpochs=" + segmentLeaderEpochs +
                ", segmentSizeInBytes=" + segmentSizeInBytes +
                ", state=" + state +
                '}';
    }

    public static RemoteLogSegmentId remoteLogSegmentId(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return remoteLogSegmentMetadata != null ? remoteLogSegmentMetadata.remoteLogSegmentId() : null;
    }

}
