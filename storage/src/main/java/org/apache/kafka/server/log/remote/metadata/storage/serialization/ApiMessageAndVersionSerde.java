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
package org.apache.kafka.server.log.remote.metadata.storage.serialization;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.metadata.ApiMessageAndVersion;

import java.nio.ByteBuffer;

/**
 *
 */
public abstract class ApiMessageAndVersionSerde implements Serde<ApiMessageAndVersion> {

    private final Serializer<ApiMessageAndVersion> serializer;
    private final Deserializer<ApiMessageAndVersion> deserializer;

    public ApiMessageAndVersionSerde() {
        serializer = (topic, data) -> serialize(data);
        deserializer = (topic, data) -> deserialize(data);
    }

    private byte[] serialize(ApiMessageAndVersion messageAndVersion) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        short version = messageAndVersion.version();
        ApiMessage message = messageAndVersion.message();

        // Add header containing apiKey and apiVersion,
        // headerSize is 1 byte for apiKey and 1 byte for apiVersion
        int headerSize = 1 + 1;
        int messageSize = message.size(cache, version);
        ByteBufferAccessor writable = new ByteBufferAccessor(ByteBuffer.allocate(headerSize + messageSize));

        // Write apiKey and apiVersion
        writable.writeUnsignedVarint(message.apiKey());
        writable.writeUnsignedVarint(version);

        // Write the message
        message.write(writable, cache, version);

        return writable.buffer().array();
    }

    private ApiMessageAndVersion deserialize(byte[] data) {

        ByteBufferAccessor readable = new ByteBufferAccessor(ByteBuffer.wrap(data));

        short apiKey = (short) readable.readUnsignedVarint();
        short version = (short) readable.readUnsignedVarint();

        ApiMessage message = apiMessageFor(apiKey);
        message.read(readable, version);

        return new ApiMessageAndVersion(message, version);
    }

    /**
     * Return {@code ApiMessage} instance for the given {@code apiKey}. This is used while deserializing
     *
     * @param apiKey
     */
    public abstract ApiMessage apiMessageFor(short apiKey);

    @Override
    public Serializer<ApiMessageAndVersion> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<ApiMessageAndVersion> deserializer() {
        return deserializer;
    }

}
