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

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package org.apache.kafka.common.metadata;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;


public class IsrChangeRecord implements ApiMessage {
    int partitionId;
    Uuid topicId;
    List<Integer> isr;
    int leader;
    int leaderEpoch;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("partition_id", Type.INT32, "The partition id."),
            new Field("topic_id", Type.UUID, "The unique ID of this topic."),
            new Field("isr", new ArrayOf(Type.INT32), "The in-sync replicas of this partition"),
            new Field("leader", Type.INT32, "The lead replica, or -1 if there is no leader."),
            new Field("leader_epoch", Type.INT32, "An epoch that gets incremented each time we change the ISR.")
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public IsrChangeRecord(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public IsrChangeRecord() {
        this.partitionId = -1;
        this.topicId = Uuid.ZERO_UUID;
        this.isr = new ArrayList<Integer>(0);
        this.leader = -1;
        this.leaderEpoch = -1;
    }
    
    @Override
    public short apiKey() {
        return 5;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 0;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        this.partitionId = _readable.readInt();
        this.topicId = _readable.readUuid();
        {
            int arrayLength;
            arrayLength = _readable.readInt();
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field isr was serialized as null");
            } else {
                ArrayList<Integer> newCollection = new ArrayList<Integer>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(_readable.readInt());
                }
                this.isr = newCollection;
            }
        }
        this.leader = _readable.readInt();
        this.leaderEpoch = _readable.readInt();
        this._unknownTaggedFields = null;
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeInt(partitionId);
        _writable.writeUuid(topicId);
        _writable.writeInt(isr.size());
        for (Integer isrElement : isr) {
            _writable.writeInt(isrElement);
        }
        _writable.writeInt(leader);
        _writable.writeInt(leaderEpoch);
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(4);
        _size.addBytes(16);
        {
            _size.addBytes(4);
            _size.addBytes(isr.size() * 4);
        }
        _size.addBytes(4);
        _size.addBytes(4);
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                _size.addBytes(_field.size());
            }
        }
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IsrChangeRecord)) return false;
        IsrChangeRecord other = (IsrChangeRecord) obj;
        if (partitionId != other.partitionId) return false;
        if (!this.topicId.equals(other.topicId)) return false;
        if (this.isr == null) {
            if (other.isr != null) return false;
        } else {
            if (!this.isr.equals(other.isr)) return false;
        }
        if (leader != other.leader) return false;
        if (leaderEpoch != other.leaderEpoch) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + partitionId;
        hashCode = 31 * hashCode + topicId.hashCode();
        hashCode = 31 * hashCode + (isr == null ? 0 : isr.hashCode());
        hashCode = 31 * hashCode + leader;
        hashCode = 31 * hashCode + leaderEpoch;
        return hashCode;
    }
    
    @Override
    public IsrChangeRecord duplicate() {
        IsrChangeRecord _duplicate = new IsrChangeRecord();
        _duplicate.partitionId = partitionId;
        _duplicate.topicId = topicId;
        ArrayList<Integer> newIsr = new ArrayList<Integer>(isr.size());
        for (Integer _element : isr) {
            newIsr.add(_element);
        }
        _duplicate.isr = newIsr;
        _duplicate.leader = leader;
        _duplicate.leaderEpoch = leaderEpoch;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "IsrChangeRecord("
            + "partitionId=" + partitionId
            + ", topicId=" + topicId.toString()
            + ", isr=" + MessageUtil.deepToString(isr.iterator())
            + ", leader=" + leader
            + ", leaderEpoch=" + leaderEpoch
            + ")";
    }
    
    public int partitionId() {
        return this.partitionId;
    }
    
    public Uuid topicId() {
        return this.topicId;
    }
    
    public List<Integer> isr() {
        return this.isr;
    }
    
    public int leader() {
        return this.leader;
    }
    
    public int leaderEpoch() {
        return this.leaderEpoch;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public IsrChangeRecord setPartitionId(int v) {
        this.partitionId = v;
        return this;
    }
    
    public IsrChangeRecord setTopicId(Uuid v) {
        this.topicId = v;
        return this;
    }
    
    public IsrChangeRecord setIsr(List<Integer> v) {
        this.isr = v;
        return this;
    }
    
    public IsrChangeRecord setLeader(int v) {
        this.leader = v;
        return this;
    }
    
    public IsrChangeRecord setLeaderEpoch(int v) {
        this.leaderEpoch = v;
        return this;
    }
}
