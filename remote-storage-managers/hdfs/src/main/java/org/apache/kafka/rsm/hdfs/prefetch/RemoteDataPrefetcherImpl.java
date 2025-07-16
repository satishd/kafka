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

package org.apache.kafka.rsm.hdfs.prefetch;

import org.apache.kafka.rsm.hdfs.FileSystemManager;
import org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.kafka.server.log.remote.storage.RemoteStorageManagerConfig.REMOTE_LOG_METADATA_MANAGER_SUPPLIER;

public class RemoteDataPrefetcherImpl implements RemoteDataPrefetcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteDataPrefetcherImpl.class);

    private final PrefetchEvaluator prefetchEvaluator;
    private final PrefetchSegmentManager prefetchSegmentManager;

    private Supplier<RemoteLogMetadataManager> rlmmSupplier;

    public RemoteDataPrefetcherImpl(PrefetchEvaluator prefetchEvaluator,
                                    FileSystemManager fileSystemManager,
                                    HDFSRemoteStorageManagerMetrics metrics) {
        this(prefetchEvaluator, new PrefetchSegmentManager(fileSystemManager, metrics));
    }

    public RemoteDataPrefetcherImpl(PrefetchEvaluator prefetchEvaluator,
                                    PrefetchSegmentManager prefetchSegmentManager) {
        this.prefetchEvaluator = prefetchEvaluator;
        this.prefetchSegmentManager = prefetchSegmentManager;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
        this.rlmmSupplier = (Supplier<RemoteLogMetadataManager>) configs.get(REMOTE_LOG_METADATA_MANAGER_SUPPLIER);
        this.prefetchSegmentManager.configure(configs);
    }

    @Override
    public void signalSegmentRead(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  int currentPosition,
                                  OffsetAndEpoch nextSegmentOffsetAndEpoch) {
        LOGGER.debug("Signaling segment read for segmentId: {}", remoteLogSegmentMetadata.remoteLogSegmentId());
        if (prefetchEvaluator.shouldPrefetch(remoteLogSegmentMetadata, currentPosition)) {
            try {
                // Prefetch the data for the given remoteLogSegmentMetadata
                Optional<RemoteLogSegmentMetadata> segmentToFetch = nextSegmentToPrefetch(remoteLogSegmentMetadata, nextSegmentOffsetAndEpoch);
                LOGGER.debug("Next segment to prefetch for segmentId: {} is {}", remoteLogSegmentMetadata.remoteLogSegmentId(), segmentToFetch);
                segmentToFetch.ifPresent(prefetchSegmentManager::downloadSegment);
            } catch (RemoteStorageException e) {
                LOGGER.warn("Failed to fetch next segment metadata for segmentId: {}", remoteLogSegmentMetadata.remoteLogSegmentId(), e);
            }
        } else {
            LOGGER.debug("Skipping prefetch for segmentId: {}", remoteLogSegmentMetadata.remoteLogSegmentId());
        }
    }

    private Optional<RemoteLogSegmentMetadata> nextSegmentToPrefetch(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                                     OffsetAndEpoch nextSegmentOffsetAndEpoch)
            throws RemoteStorageException {
        if (nextSegmentOffsetAndEpoch == null) {
            LOGGER.debug("No next segment offset and epoch provided for segmentId: {}", remoteLogSegmentMetadata.remoteLogSegmentId());
            return Optional.empty();
        }

        try {
            Optional<RemoteLogSegmentMetadata> result = rlmmSupplier.get().remoteLogSegmentMetadata(
                    remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition(),
                    nextSegmentOffsetAndEpoch.leaderEpoch(),
                    nextSegmentOffsetAndEpoch.offset());
            // Log result
            if (result.isPresent()) {
                LOGGER.debug("Next segment to prefetch for segmentId: {} is: {}",
                        remoteLogSegmentMetadata.remoteLogSegmentId(), result.get().remoteLogSegmentId());
            } else {
                LOGGER.debug("No next segment to prefetch for segmentId: {}", remoteLogSegmentMetadata.remoteLogSegmentId());
            }
            return result;
        } catch (RemoteStorageException e) {
            LOGGER.warn("Failed to fetch next segment metadata for segmentId: {}", remoteLogSegmentMetadata.remoteLogSegmentId(), e);
            throw e;
        }
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata, int startPosition, int endPosition) {
        RemoteLogSegmentId segmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
        try {
            InputStream inputStream = prefetchSegmentManager.fetchLogSegment(segmentId, startPosition, endPosition);
            if (inputStream == null) {
                LOGGER.debug("Segment data not prefetched for segmentId: {}", segmentId);
                return null;
            }

            LOGGER.debug("Returning prefetched segment data for segmentId: {}", segmentId);
            return inputStream;
        } catch (Exception e) {
            LOGGER.warn("Failed to fetch segment data for segmentId: {}", segmentId, e);
            return null;
        }
    }

    @Override
    public void cleanup() {
        prefetchSegmentManager.cleanup();
    }
}
