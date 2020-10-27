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

import java.util.Objects;

/**
 *
 */
public class DeletePartitionUpdate {

    private final TopicIdPartition topicPartition;
    private final RemoteLogState state;
    private final long eventTimestamp;
    private final int epoch;

    public DeletePartitionUpdate(TopicIdPartition topicPartition, RemoteLogState state, long eventTimestamp, int epoch) {
        Objects.requireNonNull(topicPartition);
        Objects.requireNonNull(state);
        if(state != RemoteLogState.DELETE_PARTITION_MARKED && state != RemoteLogState.DELETE_PARTITION_STARTED
                && state != RemoteLogState.DELETE_PARTITION_FINISHED) {
            throw new IllegalArgumentException("state should be one of the delete partition states");
        }
        this.topicPartition = topicPartition;
        this.state = state;
        this.eventTimestamp = eventTimestamp;
        this.epoch = epoch;
    }

    public TopicIdPartition topicPartition() {
        return topicPartition;
    }

    public RemoteLogState state() {
        return state;
    }

    public long eventTimestamp() {
        return eventTimestamp;
    }

    public int epoch() {
        return epoch;
    }

    @Override
    public String toString() {
        return "DeletePartitionState{" +
                "topicPartition=" + topicPartition +
                ", state=" + state +
                ", eventTimestamp=" + eventTimestamp +
                ", epoch=" + epoch +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeletePartitionUpdate that = (DeletePartitionUpdate) o;
        return eventTimestamp == that.eventTimestamp &&
                epoch == that.epoch &&
                Objects.equals(topicPartition, that.topicPartition) &&
                state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, state, eventTimestamp, epoch);
    }
}
