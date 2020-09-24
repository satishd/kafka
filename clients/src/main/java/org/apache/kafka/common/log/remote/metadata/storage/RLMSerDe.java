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
package org.apache.kafka.common.log.remote.metadata.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.log.remote.storage.*;
import org.apache.kafka.common.message.RemoteLogSegmentMetadataRecord;
import org.apache.kafka.common.message.RemotePartitionDeleteMetadataRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class RLMSerDe extends Serdes.WrapperSerde<RemoteLogSegmentMetadata> {

    public static final Short CURRENT_SCHEMA_VERSION = 0;

    public RLMSerDe() {
        super(new RLSMSerializer(), new RLSMDeserializer());
    }

    public static class RLSMSerializer implements Serializer<RemoteLogSegmentMetadata> {

        @Override
        public byte[] serialize(String topic, RemoteLogSegmentMetadata data) {
            return serialize(topic, data, true);
        }

        public byte[] serialize(String topic, RemotePartitionDeleteMetadata data, boolean includeVersion) {
            RemotePartitionDeleteMetadataRecord record = new RemotePartitionDeleteMetadataRecord()
                    .setTopicIdPartition(new RemotePartitionDeleteMetadataRecord.TopicIdPartitionEntry()
                            .setName(data.topicIdPartition().topicPartition().topic())
                            .setPartition(data.topicIdPartition().topicPartition().partition())
                            .setId(Uuid.fromUUID(data.topicIdPartition().topicId())))
                    .setEventTimestamp(data.eventTimestamp())
                    .setEpoch(data.epoch())
                    .setRemotePartitionDeleteState(data.state().id());
            return getBytes(includeVersion, record.toStruct(CURRENT_SCHEMA_VERSION));
        }

        private byte[] getBytes(boolean includeVersion, Struct struct) {
            int size = struct.sizeOf();
            ByteBuffer byteBuffer;
            if (includeVersion) {
                byteBuffer = ByteBuffer.allocate(size + 2);
                byteBuffer.putShort(CURRENT_SCHEMA_VERSION);
            } else {
                byteBuffer = ByteBuffer.allocate(size);
            }
            struct.writeTo(byteBuffer);

            return byteBuffer.array();
        }

        public byte[] serialize(String topic, RemoteLogSegmentMetadata data, boolean includeVersion) {

            RemoteLogSegmentMetadataRecord remoteLogSegmentMetadataRecord = new RemoteLogSegmentMetadataRecord()
                    .setRemoteLogSegmentId(
                            new RemoteLogSegmentMetadataRecord.RemoteLogSegmentIdEntry()
                                    .setTopicIdPartition(new RemoteLogSegmentMetadataRecord.TopicIdPartitionEntry()
                                    .setId(Uuid.fromUUID(data.remoteLogSegmentId().topicIdPartition().topicId()))
                                    .setName(data.remoteLogSegmentId().topicIdPartition().topicPartition().topic())
                                    .setPartition(data.remoteLogSegmentId().topicIdPartition().topicPartition().partition()))
                                    .setId(Uuid.fromUUID(data.remoteLogSegmentId().id())))
                    .setStartOffset(data.startOffset())
                    .setEndOffset(data.endOffset())
                    .setLeaderEpoch(data.leaderEpoch())
                    .setEventTimestamp(data.eventTimestamp())
                    .setMaxTimestamp(data.maxTimestamp())
                    .setSegmentSizeInBytes(data.segmentSizeInBytes())
                    .setSegmentLeaderEpochs(data.segmentLeaderEpochs().entrySet().stream()
                            .map(entry -> new RemoteLogSegmentMetadataRecord.SegmentLeaderEpochEntry()
                                    .setLeaderEpoch(entry.getKey())
                                    .setOffset(entry.getValue())).collect(Collectors.toList()))
                    .setRemoteLogSegmentState(data.state().id());

            return getBytes(includeVersion, remoteLogSegmentMetadataRecord.toStruct(CURRENT_SCHEMA_VERSION));
        }
    }

    public static final class RLSMDeserializer implements Deserializer<RemoteLogSegmentMetadata> {

        @Override
        public RemoteLogSegmentMetadata deserialize(String topic, byte[] data) {
            final ByteBuffer byteBuffer = ByteBuffer.wrap(data);
            short version = byteBuffer.getShort();
            return deserialize(topic, version, byteBuffer);
        }

        public RemoteLogSegmentMetadata deserialize(String topic, short version, ByteBuffer byteBuffer) {

            RemoteLogSegmentMetadataRecord record = new RemoteLogSegmentMetadataRecord(new ByteBufferAccessor(byteBuffer), version);

            RemoteLogSegmentId remoteLogSegmentId = buildRemoteLogSegmentId(record.remoteLogSegmentId());
            Map<Integer, Long> segmentLeaderEpochs = new HashMap<>();
            for (RemoteLogSegmentMetadataRecord.SegmentLeaderEpochEntry segmentLeaderEpoch : record.segmentLeaderEpochs()) {
                segmentLeaderEpochs.put(segmentLeaderEpoch.leaderEpoch(), segmentLeaderEpoch.offset());
            }
            return new RemoteLogSegmentMetadata(remoteLogSegmentId, record.startOffset(), record.endOffset(), record.maxTimestamp(),
                    record.leaderEpoch(), record.eventTimestamp(), record.segmentSizeInBytes(),
                    RemoteLogSegmentState.forId(record.remoteLogSegmentState()), segmentLeaderEpochs);
        }

        public RemotePartitionDeleteMetadata deserializeDeleteMetadata(String topic, byte[] data) {
            final ByteBuffer byteBuffer = ByteBuffer.wrap(data);
            short version = byteBuffer.getShort();
            RemotePartitionDeleteMetadataRecord record = new RemotePartitionDeleteMetadataRecord(new ByteBufferAccessor(byteBuffer), version);
            TopicIdPartition topicIdPartition = new TopicIdPartition(record.topicIdPartition().id().toUUID(),
                    new TopicPartition(record.topicIdPartition().name(), record.topicIdPartition().partition()));

            return new RemotePartitionDeleteMetadata(topicIdPartition, RemotePartitionDeleteState.forId(record.remotePartitionDeleteState()),
                    record.eventTimestamp(), record.epoch());
        }

            private RemoteLogSegmentId buildRemoteLogSegmentId(RemoteLogSegmentMetadataRecord.RemoteLogSegmentIdEntry entry) {
            TopicIdPartition topicIdPartition = new TopicIdPartition(entry.topicIdPartition().id().toUUID(),
                    new TopicPartition(entry.topicIdPartition().name(), entry.topicIdPartition().partition()));
            return new RemoteLogSegmentId(topicIdPartition, entry.id().toUUID());
        }
    }
}
