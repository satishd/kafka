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

package org.apache.kafka.rsm.hdfs;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.rsm.hdfs.prefetch.RemoteDataPrefetcher;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteReadContext;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PrefetchEnabledHDFSRemoteStorageManager implements RemoteStorageManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrefetchEnabledHDFSRemoteStorageManager.class);

    private final HDFSRemoteStorageManager hdfsRemoteStorageManager;
    private final RemoteDataPrefetcher remoteDataPrefetcher;

    public PrefetchEnabledHDFSRemoteStorageManager(HDFSRemoteStorageManager hdfsRemoteStorageManager,
                                                   RemoteDataPrefetcher remoteDataPrefetcher) {
        this.hdfsRemoteStorageManager = hdfsRemoteStorageManager;
        this.remoteDataPrefetcher = remoteDataPrefetcher;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        hdfsRemoteStorageManager.configure(configs);
        remoteDataPrefetcher.configure(configs);
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return hdfsRemoteStorageManager.reconfigurableConfigs();
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        hdfsRemoteStorageManager.validateReconfiguration(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        hdfsRemoteStorageManager.reconfigure(configs);
    }

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                                                LogSegmentData logSegmentData) throws RemoteStorageException {
        return hdfsRemoteStorageManager.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       int startPosition) throws RemoteStorageException {
        return fetchLogSegmentInternal(remoteLogSegmentMetadata, null, startPosition, Integer.MAX_VALUE);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       int startPosition,
                                       int endPosition) throws RemoteStorageException {
        return fetchLogSegmentInternal(remoteLogSegmentMetadata, null, startPosition, endPosition);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       RemoteReadContext readContext,
                                       int startPosition) throws RemoteStorageException {
        return fetchLogSegmentInternal(remoteLogSegmentMetadata, readContext, startPosition, Integer.MAX_VALUE);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       RemoteReadContext readContext,
                                       int startPosition,
                                       int endPosition) throws RemoteStorageException {
        return fetchLogSegmentInternal(remoteLogSegmentMetadata, readContext, startPosition, endPosition);
    }

    private InputStream fetchLogSegmentInternal(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                RemoteReadContext readContext,
                                                int startPosition,
                                                int endPosition) throws RemoteStorageException {
        try {
            if (readContext != null) {
                remoteDataPrefetcher.signalSegmentRead(remoteLogSegmentMetadata, startPosition, readContext.getNextSegmentOffsetAndEpoch());
            }
        } catch (Exception e) {
            // ignore
            LOGGER.warn("Failed to submit prefetch request for segment: {}", remoteLogSegmentMetadata, e);
        }

        InputStream is = remoteDataPrefetcher.fetchLogSegment(remoteLogSegmentMetadata, startPosition, endPosition);
        if (is != null) {
            return is;
        }

        return readContext == null
                ? hdfsRemoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, startPosition, endPosition)
                : hdfsRemoteStorageManager.fetchLogSegment(remoteLogSegmentMetadata, readContext, startPosition, endPosition);
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  IndexType indexType) throws RemoteStorageException {
        return hdfsRemoteStorageManager.fetchIndex(remoteLogSegmentMetadata, indexType);
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        hdfsRemoteStorageManager.deleteLogSegmentData(remoteLogSegmentMetadata);
    }

    @Override
    public void deletePartition(TopicIdPartition partition,
                                List<RemoteLogSegmentMetadata> segmentMetadataList) throws RemoteStorageException {
        hdfsRemoteStorageManager.deletePartition(partition, segmentMetadataList);
    }

    @Override
    public void close() {
        try {
            remoteDataPrefetcher.cleanup();
        } catch (Exception e) {
            LOGGER.warn("Failed to cleanup prefetcher", e);
        } finally {
            hdfsRemoteStorageManager.close();
        }
    }
}
