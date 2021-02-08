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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;

public class RLSMSerDe extends Serdes.WrapperSerde<RemoteLogSegmentMetadata> {

    public static final Byte CURRENT_SCHEMA_VERSION = 0;

    public static final Field.Str TOPIC_NAME_FIELD = new Field.Str("topic-name", "Topic name");
    public static final Field.UUID TOPIC_ID_FIELD = new Field.UUID("topic-id", "UUID of the topic");
    public static final Field.Int32 PARTITION_FIELD = new Field.Int32("partition", "Partition number");
    public static final Schema TOPIC_PARTITION_SCHEMA = new Schema(TOPIC_ID_FIELD, TOPIC_NAME_FIELD, PARTITION_FIELD);

    private static final String TOPIC_PARTITION = "topic-partition";
    public static final String ID = "id";
    public static final Field.UUID ID_FIELD = new Field.UUID(ID, "UUID of this entry");
    private static final Schema REMOTE_LOG_SEGMENT_ID_SCHEMA_V0 = new Schema(
            new Field(TOPIC_PARTITION, TOPIC_PARTITION_SCHEMA, "Topic partition"),
            ID_FIELD);

    public static final String REMOTE_LOG_SEGMENT_ID_NAME = "remote-log-segment-id";
    private static final Field REMOTE_LOG_SEGMENT_ID_FIELD = new Field(REMOTE_LOG_SEGMENT_ID_NAME,
            REMOTE_LOG_SEGMENT_ID_SCHEMA_V0, "Remote log segment id");

    private static final Field.Int64 START_OFFSET_FIELD = new Field.Int64("start-offset",
            "Start offset of the remote log segment");
    private static final Field.Int64 END_OFFSET_FIELD = new Field.Int64("end-offset",
            "End offset of the remote log segment");
    private static final Field.Int32 LEADER_EPOCH_FIELD = new Field.Int32("leader-epoch",
            "Leader epoch value of the remote log segment");
    private static final Field.Int64 MAX_TIMESTAMP_FIELD = new Field.Int64("max-timestamp",
            "Max time stamp in the remote log segment");
    private static final Field.Int64 EVENT_TIMESTAMP_FIELD = new Field.Int64("event-timestamp",
            "Created timestamp value of the remote log segment");
    private static final Field.Int8 STATE_FIELD = new Field.Int8("state",
            "State of this segment");
    private static final Field.Int32 SEGMENT_SIZE_FIELD = new Field.Int32("segment-size",
            "Size of the remote log segment");

    private static final Schema SCHEMA_V0 = new Schema(
            REMOTE_LOG_SEGMENT_ID_FIELD,
            START_OFFSET_FIELD,
            END_OFFSET_FIELD,
            LEADER_EPOCH_FIELD,
            MAX_TIMESTAMP_FIELD,
            EVENT_TIMESTAMP_FIELD,
            STATE_FIELD,
            SEGMENT_SIZE_FIELD);

    private static final Schema[] SCHEMAS = {SCHEMA_V0};

    public RLSMSerDe() {
        super(new RLSMSerializer(), new RLSMDeserializer());
    }

    public static class RLSMSerializer implements Serializer<RemoteLogSegmentMetadata> {

        @Override
        public byte[] serialize(String topic, RemoteLogSegmentMetadata data) {
            return serialize(topic, data, true);
        }

        public byte[] serialize(String topic, RemoteLogSegmentMetadata data, boolean includeVersion) {
            Struct tpStruct = new Struct(TOPIC_PARTITION_SCHEMA);
            tpStruct.set(TOPIC_NAME_FIELD, data.remoteLogSegmentId().topicPartition().topic());
            tpStruct.set(PARTITION_FIELD, data.remoteLogSegmentId().topicPartition().partition());

            Struct rlsIdStruct = new Struct(REMOTE_LOG_SEGMENT_ID_SCHEMA_V0);
            rlsIdStruct.set(TOPIC_PARTITION, tpStruct);
            UUID uuid = data.remoteLogSegmentId().id();
            rlsIdStruct.set(ID, new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));

            Struct rlsmStruct = new Struct(SCHEMA_V0);
            rlsmStruct.set(REMOTE_LOG_SEGMENT_ID_NAME, rlsIdStruct);
            rlsmStruct.set(START_OFFSET_FIELD, data.startOffset());
            rlsmStruct.set(END_OFFSET_FIELD, data.endOffset());
            rlsmStruct.set(LEADER_EPOCH_FIELD, data.leaderEpoch());
            rlsmStruct.set(MAX_TIMESTAMP_FIELD, data.maxTimestamp());
            rlsmStruct.set(EVENT_TIMESTAMP_FIELD, data.eventTimestamp());
            rlsmStruct.set(STATE_FIELD, (byte) data.state().ordinal());
            rlsmStruct.set(SEGMENT_SIZE_FIELD, data.segmentSizeInBytes());

            final int size = SCHEMA_V0.sizeOf(rlsmStruct);
            ByteBuffer byteBuffer;
            if (includeVersion) {
                byteBuffer = ByteBuffer.allocate(size + 2);
                byteBuffer.putShort((short) 0);
            } else {
                byteBuffer = ByteBuffer.allocate(size);
            }

            SCHEMA_V0.write(byteBuffer, rlsmStruct);

            return byteBuffer.array();
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
            final Struct struct = SCHEMAS[version].read(byteBuffer);
            final Struct rlsIdStruct = (Struct) struct.get(REMOTE_LOG_SEGMENT_ID_NAME);
            final Struct tpStruct = (Struct) rlsIdStruct.get(TOPIC_PARTITION);

            Uuid uuid = rlsIdStruct.get(ID_FIELD);
            RemoteLogSegmentId rlsId = new RemoteLogSegmentId(
                    new TopicPartition(tpStruct.get(TOPIC_NAME_FIELD), tpStruct.get(PARTITION_FIELD)),
                    new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));

            return new RemoteLogSegmentMetadata(rlsId,
                    struct.get(START_OFFSET_FIELD),
                    struct.get(END_OFFSET_FIELD),
                    struct.get(MAX_TIMESTAMP_FIELD),
                    struct.get(LEADER_EPOCH_FIELD),
                    struct.get(EVENT_TIMESTAMP_FIELD),
                    struct.get(SEGMENT_SIZE_FIELD),
                    RemoteLogSegmentState.values()[struct.get(STATE_FIELD)],
                    Collections.emptyMap()
            );
        }
    }
}
