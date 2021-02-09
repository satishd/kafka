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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
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
import org.apache.kafka.common.utils.Bytes;


public class UserScramCredentialRecord implements ApiMessage {
    String name;
    List<CredentialInfo> credentialInfos;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("name", Type.STRING, "The user name."),
            new Field("credential_infos", new ArrayOf(CredentialInfo.SCHEMA_0), "The mechanism and related information associated with the user's SCRAM credential.")
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public UserScramCredentialRecord(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public UserScramCredentialRecord() {
        this.name = "";
        this.credentialInfos = new ArrayList<CredentialInfo>(0);
    }
    
    @Override
    public short apiKey() {
        return 11;
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
        {
            int length;
            length = _readable.readShort();
            if (length < 0) {
                throw new RuntimeException("non-nullable field name was serialized as null");
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field name had invalid length " + length);
            } else {
                this.name = _readable.readString(length);
            }
        }
        {
            int arrayLength;
            arrayLength = _readable.readInt();
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field credentialInfos was serialized as null");
            } else {
                ArrayList<CredentialInfo> newCollection = new ArrayList<CredentialInfo>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new CredentialInfo(_readable, _version));
                }
                this.credentialInfos = newCollection;
            }
        }
        this._unknownTaggedFields = null;
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        {
            byte[] _stringBytes = _cache.getSerializedValue(name);
            _writable.writeShort((short) _stringBytes.length);
            _writable.writeByteArray(_stringBytes);
        }
        _writable.writeInt(credentialInfos.size());
        for (CredentialInfo credentialInfosElement : credentialInfos) {
            credentialInfosElement.write(_writable, _cache, _version);
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        {
            byte[] _stringBytes = name.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'name' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(name, _stringBytes);
            _size.addBytes(_stringBytes.length + 2);
        }
        {
            _size.addBytes(4);
            for (CredentialInfo credentialInfosElement : credentialInfos) {
                credentialInfosElement.addSize(_size, _cache, _version);
            }
        }
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
        if (!(obj instanceof UserScramCredentialRecord)) return false;
        UserScramCredentialRecord other = (UserScramCredentialRecord) obj;
        if (this.name == null) {
            if (other.name != null) return false;
        } else {
            if (!this.name.equals(other.name)) return false;
        }
        if (this.credentialInfos == null) {
            if (other.credentialInfos != null) return false;
        } else {
            if (!this.credentialInfos.equals(other.credentialInfos)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
        hashCode = 31 * hashCode + (credentialInfos == null ? 0 : credentialInfos.hashCode());
        return hashCode;
    }
    
    @Override
    public UserScramCredentialRecord duplicate() {
        UserScramCredentialRecord _duplicate = new UserScramCredentialRecord();
        _duplicate.name = name;
        ArrayList<CredentialInfo> newCredentialInfos = new ArrayList<CredentialInfo>(credentialInfos.size());
        for (CredentialInfo _element : credentialInfos) {
            newCredentialInfos.add(_element.duplicate());
        }
        _duplicate.credentialInfos = newCredentialInfos;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "UserScramCredentialRecord("
            + "name=" + ((name == null) ? "null" : "'" + name.toString() + "'")
            + ", credentialInfos=" + MessageUtil.deepToString(credentialInfos.iterator())
            + ")";
    }
    
    public String name() {
        return this.name;
    }
    
    public List<CredentialInfo> credentialInfos() {
        return this.credentialInfos;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public UserScramCredentialRecord setName(String v) {
        this.name = v;
        return this;
    }
    
    public UserScramCredentialRecord setCredentialInfos(List<CredentialInfo> v) {
        this.credentialInfos = v;
        return this;
    }
    
    public static class CredentialInfo implements Message {
        byte mechanism;
        byte[] salt;
        byte[] saltedPassword;
        int iterations;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("mechanism", Type.INT8, "The SCRAM mechanism."),
                new Field("salt", Type.BYTES, "A random salt generated by the client."),
                new Field("salted_password", Type.BYTES, "The salted password."),
                new Field("iterations", Type.INT32, "The number of iterations used in the SCRAM credential.")
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public CredentialInfo(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public CredentialInfo() {
            this.mechanism = (byte) 0;
            this.salt = Bytes.EMPTY;
            this.saltedPassword = Bytes.EMPTY;
            this.iterations = 0;
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
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of CredentialInfo");
            }
            this.mechanism = _readable.readByte();
            {
                int length;
                length = _readable.readInt();
                if (length < 0) {
                    throw new RuntimeException("non-nullable field salt was serialized as null");
                } else {
                    byte[] newBytes = new byte[length];
                    _readable.readArray(newBytes);
                    this.salt = newBytes;
                }
            }
            {
                int length;
                length = _readable.readInt();
                if (length < 0) {
                    throw new RuntimeException("non-nullable field saltedPassword was serialized as null");
                } else {
                    byte[] newBytes = new byte[length];
                    _readable.readArray(newBytes);
                    this.saltedPassword = newBytes;
                }
            }
            this.iterations = _readable.readInt();
            this._unknownTaggedFields = null;
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _writable.writeByte(mechanism);
            _writable.writeInt(salt.length);
            _writable.writeByteArray(salt);
            _writable.writeInt(saltedPassword.length);
            _writable.writeByteArray(saltedPassword);
            _writable.writeInt(iterations);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of CredentialInfo");
            }
            _size.addBytes(1);
            {
                _size.addBytes(salt.length);
                _size.addBytes(4);
            }
            {
                _size.addBytes(saltedPassword.length);
                _size.addBytes(4);
            }
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
            if (!(obj instanceof CredentialInfo)) return false;
            CredentialInfo other = (CredentialInfo) obj;
            if (mechanism != other.mechanism) return false;
            if (!Arrays.equals(this.salt, other.salt)) return false;
            if (!Arrays.equals(this.saltedPassword, other.saltedPassword)) return false;
            if (iterations != other.iterations) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + mechanism;
            hashCode = 31 * hashCode + Arrays.hashCode(salt);
            hashCode = 31 * hashCode + Arrays.hashCode(saltedPassword);
            hashCode = 31 * hashCode + iterations;
            return hashCode;
        }
        
        @Override
        public CredentialInfo duplicate() {
            CredentialInfo _duplicate = new CredentialInfo();
            _duplicate.mechanism = mechanism;
            _duplicate.salt = MessageUtil.duplicate(salt);
            _duplicate.saltedPassword = MessageUtil.duplicate(saltedPassword);
            _duplicate.iterations = iterations;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "CredentialInfo("
                + "mechanism=" + mechanism
                + ", salt=" + Arrays.toString(salt)
                + ", saltedPassword=" + Arrays.toString(saltedPassword)
                + ", iterations=" + iterations
                + ")";
        }
        
        public byte mechanism() {
            return this.mechanism;
        }
        
        public byte[] salt() {
            return this.salt;
        }
        
        public byte[] saltedPassword() {
            return this.saltedPassword;
        }
        
        public int iterations() {
            return this.iterations;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public CredentialInfo setMechanism(byte v) {
            this.mechanism = v;
            return this;
        }
        
        public CredentialInfo setSalt(byte[] v) {
            this.salt = v;
            return this;
        }
        
        public CredentialInfo setSaltedPassword(byte[] v) {
            this.saltedPassword = v;
            return this;
        }
        
        public CredentialInfo setIterations(int v) {
            this.iterations = v;
            return this;
        }
    }
}
