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
package org.apache.kafka.server.log.remote.metadata.storage.serde;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.server.log.remote.metadata.storage.generated.RemoteLogSegmentMetadataRecordUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;

import java.nio.ByteBuffer;

public class RemoteLogSegmentMetadataUpdateSerde implements RemoteLogMetadataSerdes<RemoteLogSegmentMetadataUpdate> {

    public Message serialize(RemoteLogSegmentMetadataUpdate data) {
        RemoteLogSegmentMetadataRecordUpdate recordUpdate = new RemoteLogSegmentMetadataRecordUpdate()
                .setRemoteLogSegmentId(new RemoteLogSegmentMetadataRecordUpdate.RemoteLogSegmentIdEntry()
                        .setId(data.remoteLogSegmentId().id())
                        .setTopicIdPartition(new RemoteLogSegmentMetadataRecordUpdate.TopicIdPartitionEntry()
                                .setName(data.remoteLogSegmentId().topicIdPartition().topicPartition().topic())
                                .setPartition(
                                        data.remoteLogSegmentId().topicIdPartition().topicPartition().partition())
                                .setId(data.remoteLogSegmentId().topicIdPartition().topicId())))
                .setEventTimestampMs(data.eventTimestampMs())
                .setRemoteLogSegmentState(data.state().id());
        return recordUpdate;
    }

    public RemoteLogSegmentMetadataUpdate deserialize(byte version, ByteBuffer byteBuffer) {
        RemoteLogSegmentMetadataRecordUpdate record = new RemoteLogSegmentMetadataRecordUpdate(
                new ByteBufferAccessor(byteBuffer), version);
        RemoteLogSegmentMetadataRecordUpdate.RemoteLogSegmentIdEntry entry = record.remoteLogSegmentId();
        TopicIdPartition topicIdPartition = new TopicIdPartition(entry.topicIdPartition().id(),
                new TopicPartition(entry.topicIdPartition().name(), entry.topicIdPartition().partition()));

        return new RemoteLogSegmentMetadataUpdate(new RemoteLogSegmentId(topicIdPartition, entry.id()),
                record.eventTimestampMs(), RemoteLogSegmentState.forId(record.remoteLogSegmentState()), record.brokerId());
    }
}
