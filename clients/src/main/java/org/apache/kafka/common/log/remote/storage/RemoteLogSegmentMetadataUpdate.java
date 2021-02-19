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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.io.Serializable;
import java.util.Objects;

/**
 * It describes the metadata update about the log segment in the remote storage. This is used to update the state of
 * the remote log segment by using {@link RemoteLogMetadataManager#updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate)}.
 * This also includes the timestamp of this event and leader epoch in which it
 */
@InterfaceStability.Evolving
public class RemoteLogSegmentMetadataUpdate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Universally unique remote log segment id.
     */
    private final RemoteLogSegmentId remoteLogSegmentId;

    /**
     * Epoch time at which this event is generated.
     */
    private final long eventTimestamp;

    /**
     * It indicates the state in which the action is executed on this segment.
     */
    private final RemoteLogSegmentState state;

    /**
     * @param remoteLogSegmentId  Universally unique remote log segment id.
     * @param eventTimestamp      Epoch time at which the remote log segment is copied to the remote tier storage.
     * @param state               state of the remote log segment.
     */
    public RemoteLogSegmentMetadataUpdate(RemoteLogSegmentId remoteLogSegmentId,
                                          long eventTimestamp,
                                          RemoteLogSegmentState state) {
        this.remoteLogSegmentId = remoteLogSegmentId;
        this.eventTimestamp = eventTimestamp;
        this.state = state;
    }

    /**
     * @return Universally unique id of this remote log segment.
     */
    public RemoteLogSegmentId remoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    /**
     * @return Epoch time at which this event is generated.
     */
    public long eventTimestamp() {
        return eventTimestamp;
    }

    /**
     * It represents the state of the remote log segment. It can be one of the values of {@link RemoteLogSegmentState}.
     */
    public RemoteLogSegmentState state() {
        return state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteLogSegmentMetadataUpdate that = (RemoteLogSegmentMetadataUpdate) o;
        return eventTimestamp == that.eventTimestamp && Objects.equals(remoteLogSegmentId, that.remoteLogSegmentId) && state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentId, eventTimestamp, state);
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentMetadataUpdate{" +
                "remoteLogSegmentId=" + remoteLogSegmentId +
                ", eventTimestamp=" + eventTimestamp +
                ", state=" + state +
                '}';
    }
}
