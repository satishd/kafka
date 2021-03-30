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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is an implementation of {@link RemoteStorageManager} backed by in-memory store.
 */
public class InmemoryRemoteStorageManager implements RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(InmemoryRemoteStorageManager.class);

    // Map of key to log data, which can be segment or any of its indexes.
    private Map<String, byte[]> keyToLogData = new ConcurrentHashMap<>();

    static String generateKeyForSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        return remoteLogSegmentMetadata.remoteLogSegmentId().id().toString() + ".segment";
    }

    static String generateKeyForIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                      IndexType indexType) {
        return remoteLogSegmentMetadata.remoteLogSegmentId().id().toString() + "." + indexType.toString();
    }

    // visible for testing.
    boolean containsKey(String key) {
        return keyToLogData.containsKey(key);
    }

    @Override
    public void copyLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   LogSegmentData logSegmentData)
            throws RemoteStorageException {
        log.debug("copying log segment and indexes for : {}", remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData can not be null");

        if (keyToLogData.containsKey(generateKeyForSegment(remoteLogSegmentMetadata))) {
            throw new RemoteStorageException();
        }

        try {
            keyToLogData.put(generateKeyForSegment(remoteLogSegmentMetadata),
                    Files.readAllBytes(logSegmentData.logSegment().toPath()));
            keyToLogData.put(generateKeyForIndex(remoteLogSegmentMetadata, IndexType.Transaction),
                    Files.readAllBytes(logSegmentData.txnIndex().toPath()));
            keyToLogData.put(generateKeyForIndex(remoteLogSegmentMetadata, IndexType.LeaderEpoch),
                    logSegmentData.leaderEpochIndex().array());
            keyToLogData.put(generateKeyForIndex(remoteLogSegmentMetadata, IndexType.ProducerSnapshot),
                    Files.readAllBytes(logSegmentData.producerSnapshotIndex().toPath()));
            keyToLogData.put(generateKeyForIndex(remoteLogSegmentMetadata, IndexType.Offset),
                    Files.readAllBytes(logSegmentData.offsetIndex().toPath()));
            keyToLogData.put(generateKeyForIndex(remoteLogSegmentMetadata, IndexType.Timestamp),
                    Files.readAllBytes(logSegmentData.timeIndex().toPath()));
        } catch (Exception e) {
            throw new RemoteStorageException(e.getMessage(), e);
        }
        log.debug("copied log segment and indexes for : {} successfully.", remoteLogSegmentMetadata);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       int startPosition)
            throws RemoteStorageException {
        log.debug("Received fetch segment request at start position: [{}] for [{}]", startPosition, remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        return fetchLogSegment(remoteLogSegmentMetadata, startPosition, Integer.MAX_VALUE);
    }

    @Override
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       int startPosition,
                                       int endPosition) throws RemoteStorageException {
        log.debug("Received fetch segment request at start position: [{}] and end position: [{}] for segment [{}]",
                startPosition, endPosition, remoteLogSegmentMetadata);

        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        if (startPosition < 0 || endPosition < 0) {
            throw new IllegalArgumentException("Given start position or end position must not be negative.");
        }

        if (endPosition < startPosition) {
            throw new IllegalArgumentException("end position must be greater than start position");
        }

        String key = generateKeyForSegment(remoteLogSegmentMetadata);
        byte[] segment = keyToLogData.get(key);

        if (segment == null) {
            throw new RemoteResourceNotFoundException("No remote log segment found with start offset:"
                                                      + remoteLogSegmentMetadata.startOffset() + " and id: "
                                                      + remoteLogSegmentMetadata.remoteLogSegmentId());
        }

        if (startPosition >= segment.length) {
            throw new IllegalArgumentException("start position: " + startPosition
                                               + " must be less than the length of the segment: " + segment.length);
        }

        // check for boundaries like given end position is more than the length, length should never be more than the
        // existing segment size.
        int length = Math.min(segment.length - 1, endPosition) - startPosition + 1;
        log.debug("Length of the segment to be sent: [{}], for segment: [{}]", length, remoteLogSegmentMetadata);

        return new ByteArrayInputStream(segment, startPosition, length);
    }

    @Override
    public InputStream fetchIndex(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  IndexType indexType) throws RemoteStorageException {
        log.debug("Received fetch request for index type: [{}], segment [{}]", indexType, remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");
        Objects.requireNonNull(indexType, "indexType can not be null");

        String key = generateKeyForIndex(remoteLogSegmentMetadata, indexType);
        byte[] index = keyToLogData.get(key);
        if (index == null) {
            throw new RemoteResourceNotFoundException("No remote log segment index found with start offset:"
                                                      + remoteLogSegmentMetadata.startOffset() + " and id: "
                                                      + remoteLogSegmentMetadata.remoteLogSegmentId());
        }

        return new ByteArrayInputStream(index);
    }

    @Override
    public void deleteLogSegmentData(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException {
        log.info("Deleting log segment for: [{}]", remoteLogSegmentMetadata);
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");
        String segmentKey = generateKeyForSegment(remoteLogSegmentMetadata);
        keyToLogData.remove(segmentKey);
        for (IndexType indexType : IndexType.values()) {
            String key = generateKeyForIndex(remoteLogSegmentMetadata, indexType);
            keyToLogData.remove(key);
        }
        log.info("Deleted log segment successfully for: [{}]", remoteLogSegmentMetadata);
    }

    @Override
    public void close() throws IOException {
        keyToLogData = Collections.emptyMap();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Intentionally left blank here as nothing to be initialized here.
    }
}
