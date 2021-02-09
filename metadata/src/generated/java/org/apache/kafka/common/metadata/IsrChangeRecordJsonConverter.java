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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.metadata.IsrChangeRecord.*;

public class IsrChangeRecordJsonConverter {
    public static IsrChangeRecord read(JsonNode _node, short _version) {
        IsrChangeRecord _object = new IsrChangeRecord();
        JsonNode _partitionIdNode = _node.get("partitionId");
        if (_partitionIdNode == null) {
            throw new RuntimeException("IsrChangeRecord: unable to locate field 'partitionId', which is mandatory in version " + _version);
        } else {
            _object.partitionId = MessageUtil.jsonNodeToInt(_partitionIdNode, "IsrChangeRecord");
        }
        JsonNode _topicIdNode = _node.get("topicId");
        if (_topicIdNode == null) {
            throw new RuntimeException("IsrChangeRecord: unable to locate field 'topicId', which is mandatory in version " + _version);
        } else {
            if (!_topicIdNode.isTextual()) {
                throw new RuntimeException("IsrChangeRecord expected a JSON string type, but got " + _node.getNodeType());
            }
            _object.topicId = Uuid.fromString(_topicIdNode.asText());
        }
        JsonNode _isrNode = _node.get("isr");
        if (_isrNode == null) {
            throw new RuntimeException("IsrChangeRecord: unable to locate field 'isr', which is mandatory in version " + _version);
        } else {
            if (!_isrNode.isArray()) {
                throw new RuntimeException("IsrChangeRecord expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<Integer> _collection = new ArrayList<Integer>(_isrNode.size());
            _object.isr = _collection;
            for (JsonNode _element : _isrNode) {
                _collection.add(MessageUtil.jsonNodeToInt(_element, "IsrChangeRecord element"));
            }
        }
        JsonNode _leaderNode = _node.get("leader");
        if (_leaderNode == null) {
            throw new RuntimeException("IsrChangeRecord: unable to locate field 'leader', which is mandatory in version " + _version);
        } else {
            _object.leader = MessageUtil.jsonNodeToInt(_leaderNode, "IsrChangeRecord");
        }
        JsonNode _leaderEpochNode = _node.get("leaderEpoch");
        if (_leaderEpochNode == null) {
            throw new RuntimeException("IsrChangeRecord: unable to locate field 'leaderEpoch', which is mandatory in version " + _version);
        } else {
            _object.leaderEpoch = MessageUtil.jsonNodeToInt(_leaderEpochNode, "IsrChangeRecord");
        }
        return _object;
    }
    public static JsonNode write(IsrChangeRecord _object, short _version, boolean _serializeRecords) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("partitionId", new IntNode(_object.partitionId));
        _node.set("topicId", new TextNode(_object.topicId.toString()));
        ArrayNode _isrArray = new ArrayNode(JsonNodeFactory.instance);
        for (Integer _element : _object.isr) {
            _isrArray.add(new IntNode(_element));
        }
        _node.set("isr", _isrArray);
        _node.set("leader", new IntNode(_object.leader));
        _node.set("leaderEpoch", new IntNode(_object.leaderEpoch));
        return _node;
    }
    public static JsonNode write(IsrChangeRecord _object, short _version) {
        return write(_object, _version, true);
    }
}
