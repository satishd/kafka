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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class NoOpRemoteStorageManager implements RemoteStorageManager {
    @Override
    public Optional<CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                       LogSegmentData logSegmentData) {
        return Optional.empty();
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       int startPosition) {
        return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       int startPosition,
                                       int endPosition) {
        return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  IndexType indexType) {
        return new ByteArrayInputStream(new byte[0]);
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
    }

    @Override
    public void deletePartition(TopicIdPartition partition, List<RemoteLogSegmentMetadata> segmentMetadataList) {
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Collections.emptySet();
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
    }
}
