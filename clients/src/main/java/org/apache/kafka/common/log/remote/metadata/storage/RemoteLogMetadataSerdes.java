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
import org.apache.kafka.common.message.RemoteLogSegmentMetadataRecordUpdate;
import org.apache.kafka.common.message.RemotePartitionDeleteMetadataRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class RemoteLogMetadataSerdes {

    private interface Serde {
        Struct serialize(byte version, Object metadata);

        Object deserialize(byte version, ByteBuffer payload);
    }

    public static final byte REMOTE_LOG_SEGMENT_METADATA_API_KEY = (byte) new RemoteLogSegmentMetadataRecord().apiKey();
    public static final byte REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY = (byte) new RemoteLogSegmentMetadataRecordUpdate().apiKey();
    public static final byte REMOTE_PARTITION_DELETE_API_KEY = (byte) new RemotePartitionDeleteMetadataRecord().apiKey();

    private final Map<Byte, Serde> keyWithSerde;

    public RemoteLogMetadataSerdes() {
        Map<Byte, RemoteLogMetadataSerdes.Serde> serdes = new HashMap<>();
        serdes.put(REMOTE_LOG_SEGMENT_METADATA_API_KEY, new RemoteLogMetadataSerdes.RemoteLogSegmentMetadataSerde());
        serdes.put(REMOTE_LOG_SEGMENT_METADATA_UPDATE_API_KEY, new RemoteLogMetadataSerdes.RemoteLogSegmentMetadataUpdateSerde());
        serdes.put(REMOTE_PARTITION_DELETE_API_KEY, new RemoteLogMetadataSerdes.RemotePartitionMetadataSerde());
        keyWithSerde = serdes;
    }

    public byte[] serialize(RemoteLogMetadataContext remoteLogMetadataContext) {
        Serde serDe = keyWithSerde.get(remoteLogMetadataContext.apiKey());
        if (serDe == null) {
            throw new IllegalArgumentException("apikey: " + remoteLogMetadataContext.apiKey() + " serializer does not exist.");
        }

        Struct struct = serDe.serialize(remoteLogMetadataContext.version(), remoteLogMetadataContext.payload());
        return transformToBytes(remoteLogMetadataContext.apiKey(), remoteLogMetadataContext.version(), struct);
    }

    private static byte[] transformToBytes(short apiKey, short version, Struct struct) {
        int size = struct.sizeOf();
        ByteBuffer byteBuffer;
        byteBuffer = ByteBuffer.allocate(2 + 2 + size);
        byteBuffer.putShort(apiKey);
        byteBuffer.putShort(version);
        struct.writeTo(byteBuffer);

        return byteBuffer.array();
    }

    public RemoteLogMetadataContext deserialize(byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        byte apiKey = byteBuffer.get();
        byte version = byteBuffer.get();
        Serde serDe = keyWithSerde.get(apiKey);
        if (serDe == null) {
            throw new IllegalArgumentException("apikey: " + apiKey + " serializer does not exist.");
        }

        Object deserializedObj = serDe.deserialize(version, byteBuffer);
        return new RemoteLogMetadataContext(apiKey, version, deserializedObj);
    }

    private static class RemoteLogSegmentMetadataUpdateSerde implements Serde {

        public Struct serialize(byte version, Object payload) {
            RemoteLogSegmentMetadataUpdate data = (RemoteLogSegmentMetadataUpdate) payload;
            RemoteLogSegmentMetadataRecordUpdate recordUpdate = new RemoteLogSegmentMetadataRecordUpdate()
                    .setRemoteLogSegmentId(new RemoteLogSegmentMetadataRecordUpdate.RemoteLogSegmentIdEntry()
                            .setId(Uuid.fromUUID(data.remoteLogSegmentId().id()))
                            .setTopicIdPartition(new RemoteLogSegmentMetadataRecordUpdate.TopicIdPartitionEntry()
                                    .setName(data.remoteLogSegmentId().topicIdPartition().topicPartition().topic())
                                    .setPartition(data.remoteLogSegmentId().topicIdPartition().topicPartition().partition())
                                    .setId(Uuid.fromUUID(data.remoteLogSegmentId().topicIdPartition().topicId()))))
                    .setLeaderEpoch(data.leaderEpoch())
                    .setEventTimestamp(data.eventTimestamp())
                    .setRemoteLogSegmentState(data.state().id());
            return recordUpdate.toStruct(version);
        }

        public RemoteLogSegmentMetadataUpdate deserialize(byte version, ByteBuffer byteBuffer) {
            RemoteLogSegmentMetadataRecordUpdate record = new RemoteLogSegmentMetadataRecordUpdate(new ByteBufferAccessor(byteBuffer), version);
            RemoteLogSegmentMetadataRecordUpdate.RemoteLogSegmentIdEntry entry = record.remoteLogSegmentId();
            TopicIdPartition topicIdPartition = new TopicIdPartition(entry.topicIdPartition().id().toUUID(),
                    new TopicPartition(entry.topicIdPartition().name(), entry.topicIdPartition().partition()));

            return new RemoteLogSegmentMetadataUpdate(new RemoteLogSegmentId(topicIdPartition, entry.id().toUUID()),
                    record.eventTimestamp(), record.leaderEpoch(), RemoteLogSegmentState.forId(record.remoteLogSegmentState()));
        }
    }

    private static class RemoteLogSegmentMetadataSerde implements Serde {

        public Struct serialize(byte version, Object payload) {
            RemoteLogSegmentMetadata data = (RemoteLogSegmentMetadata) payload;
            RemoteLogSegmentMetadataRecord record = new RemoteLogSegmentMetadataRecord()
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

            return record.toStruct(version);
        }

        @Override
        public RemoteLogSegmentMetadata deserialize(byte version, ByteBuffer byteBuffer) {

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

        private RemoteLogSegmentId buildRemoteLogSegmentId(RemoteLogSegmentMetadataRecord.RemoteLogSegmentIdEntry entry) {
            TopicIdPartition topicIdPartition = new TopicIdPartition(entry.topicIdPartition().id().toUUID(),
                    new TopicPartition(entry.topicIdPartition().name(), entry.topicIdPartition().partition()));
            return new RemoteLogSegmentId(topicIdPartition, entry.id().toUUID());
        }


    }

    private static final class RemotePartitionMetadataSerde implements Serde {

        @Override
        public Struct serialize(byte version, Object payload) {
            RemotePartitionDeleteMetadata data = (RemotePartitionDeleteMetadata) payload;
            RemotePartitionDeleteMetadataRecord record = new RemotePartitionDeleteMetadataRecord()
                    .setTopicIdPartition(new RemotePartitionDeleteMetadataRecord.TopicIdPartitionEntry()
                            .setName(data.topicIdPartition().topicPartition().topic())
                            .setPartition(data.topicIdPartition().topicPartition().partition())
                            .setId(Uuid.fromUUID(data.topicIdPartition().topicId())))
                    .setEventTimestamp(data.eventTimestamp())
                    .setEpoch(data.epoch())
                    .setRemotePartitionDeleteState(data.state().id());
            return record.toStruct(version);
        }

        public RemotePartitionDeleteMetadata deserialize(byte version, ByteBuffer byteBuffer) {
            RemotePartitionDeleteMetadataRecord record = new RemotePartitionDeleteMetadataRecord(new ByteBufferAccessor(byteBuffer), version);
            TopicIdPartition topicIdPartition = new TopicIdPartition(record.topicIdPartition().id().toUUID(),
                    new TopicPartition(record.topicIdPartition().name(), record.topicIdPartition().partition()));

            return new RemotePartitionDeleteMetadata(topicIdPartition, RemotePartitionDeleteState.forId(record.remotePartitionDeleteState()),
                    record.eventTimestamp(), record.epoch());
        }

    }

}
