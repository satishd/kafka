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
 * It describes the metadata about the log segment in the remote storage.
 */
public class RemoteLogSegmentMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Universally unique remote log segment id.
     */
    private final RemoteLogSegmentId remoteLogSegmentId;

    /**
     * Start offset of this segment(inclusive).
     */
    private final long startOffset;

    /**
     * End offset of this segment(inclusive).
     */
    private final long endOffset;

    /**
     * Leader or controller epoch of the broker from where this event occurred.
     */
    private final int brokerEpoch;

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
    private final Map<Long, Long> segmentLeaderEpochs;

    /**
     * Size of the segment in bytes.
     */
    private final long segmentSizeInBytes;

    /**
     * It indicates the state in which the action is executed on this segment.
     */
    private final RemoteLogState state;

    /**
     * @param remoteLogSegmentId  Universally unique remote log segment id.
     * @param startOffset         Start offset of this segment.
     * @param endOffset           End offset of this segment.
     * @param maxTimestamp        maximum timestamp in this segment
     * @param brokerEpoch         Leader or controller epoch of the broker from where this event occurred.
     * @param eventTimestamp      Epoch time at which the remote log segment is copied to the remote tier storage.
     * @param segmentSizeInBytes  size of this segment in bytes.
     * @param state   The respective segment of remoteLogSegmentId is marked fro deletion.
     * @param segmentLeaderEpochs leader epochs occurred with in this segment
     */
    public RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId, long startOffset, long endOffset,
                                    long maxTimestamp, int brokerEpoch, long eventTimestamp,
                                    long segmentSizeInBytes, RemoteLogState state, Map<Long, Long> segmentLeaderEpochs) {
        this.remoteLogSegmentId = remoteLogSegmentId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.brokerEpoch = brokerEpoch;
        this.maxTimestamp = maxTimestamp;
        this.eventTimestamp = eventTimestamp;
        this.segmentLeaderEpochs = segmentLeaderEpochs;
        this.state = state;
        this.segmentSizeInBytes = segmentSizeInBytes;
    }

    /**
     * @param remoteLogSegmentId  Universally unique remote log segment id.
     * @param startOffset         Start offset of this segment.
     * @param endOffset           End offset of this segment.
     * @param maxTimeStamp        maximum timestamp with in this segment
     * @param brokerEpoch         Leader or controller epoch of the broker from where this event occurred.
     * @param segmentSizeInBytes  size of this segment in bytes.
     * @param segmentLeaderEpochs leader epochs occurred with in this segment
     */
    public RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId, long startOffset, long endOffset,
                                    long maxTimeStamp, int brokerEpoch, long segmentSizeInBytes, Map<Long, Long> segmentLeaderEpochs) {
        this(remoteLogSegmentId,
                startOffset,
                endOffset,
                maxTimeStamp,
                brokerEpoch,
                System.currentTimeMillis(),
                segmentSizeInBytes, RemoteLogState.COPY_SEGMENT_STARTED, segmentLeaderEpochs
        );
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
     * @return Leader or controller epoch of the broker from where this event occurred.
     */
    public int brokerEpoch() {
        return brokerEpoch;
    }

    /**
     * @return Epoch time at which this evcent is occurred.
     */
    public long eventTimestamp() {
        return eventTimestamp;
    }

    /**
     * @return
     */
    public long segmentSizeInBytes() {
        return segmentSizeInBytes;
    }

    public RemoteLogState state() {
        return state;
    }

    public boolean markedForDeletion() {
        return state == RemoteLogState.DELETE_SEGMENT_STARTED;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public Map<Long, Long> segmentLeaderEpochs() {
        return segmentLeaderEpochs;
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentMetadata{" +
                "remoteLogSegmentId=" + remoteLogSegmentId +
                ", startOffset=" + startOffset +
                ", endOffset=" + endOffset +
                ", leaderEpoch=" + brokerEpoch +
                ", maxTimestamp=" + maxTimestamp +
                ", eventTimestamp=" + eventTimestamp +
                ", segmentLeaderEpochs=" + segmentLeaderEpochs +
                ", segmentSizeInBytes=" + segmentSizeInBytes +
                ", state=" + state +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteLogSegmentMetadata metadata = (RemoteLogSegmentMetadata) o;
        return startOffset == metadata.startOffset &&
                endOffset == metadata.endOffset &&
                brokerEpoch == metadata.brokerEpoch &&
                maxTimestamp == metadata.maxTimestamp &&
                eventTimestamp == metadata.eventTimestamp &&
                segmentSizeInBytes == metadata.segmentSizeInBytes &&
                Objects.equals(remoteLogSegmentId, metadata.remoteLogSegmentId) &&
                Objects.equals(segmentLeaderEpochs, metadata.segmentLeaderEpochs) &&
                state == metadata.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentId, startOffset, endOffset, brokerEpoch, maxTimestamp, eventTimestamp, segmentLeaderEpochs, segmentSizeInBytes, state);
    }

    public static RemoteLogSegmentMetadata markForDeletion(RemoteLogSegmentMetadata original) {
        return new RemoteLogSegmentMetadata(original.remoteLogSegmentId, original.startOffset, original.endOffset,
                original.maxTimestamp, original.brokerEpoch, original.eventTimestamp, original.segmentSizeInBytes, RemoteLogState.DELETE_SEGMENT_STARTED, original.segmentLeaderEpochs
        );
    }

    public static RemoteLogSegmentId remoteLogSegmentId(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return remoteLogSegmentMetadata != null ? remoteLogSegmentMetadata.remoteLogSegmentId() : null;
    }

}
