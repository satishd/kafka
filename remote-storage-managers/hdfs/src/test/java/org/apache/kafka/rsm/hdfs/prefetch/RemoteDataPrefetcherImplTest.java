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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteDataPrefetcherImplTest {

    private RemoteDataPrefetcherImpl prefetcher;
    private PrefetchEvaluator mockEvaluator;
    private PrefetchSegmentManager mockSegmentManager;
    private RemoteLogMetadataManager mockRlmm;
    private Supplier<RemoteLogMetadataManager> mockRlmmSupplier;
    private RemoteLogSegmentMetadata metadata;
    private RemoteLogSegmentMetadata nextMetadata;
    private OffsetAndEpoch nextSegmentOffsetAndEpoch;

    @BeforeEach
    public void setup() throws Exception {
        // Create mocks
        mockEvaluator = mock(PrefetchEvaluator.class);
        mockRlmm = mock(RemoteLogMetadataManager.class);
        mockRlmmSupplier = () -> mockRlmm;
        mockSegmentManager = mock(PrefetchSegmentManager.class);

        // Create test metadata
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        Map<Integer, Long> segmentLeaderEpochs = Collections.singletonMap(0, 0L);

        metadata = new RemoteLogSegmentMetadata(
            segmentId,
            0L,
            100L,
            1000L,
            1,
            System.currentTimeMillis(),
            1024,
            Optional.empty(),
            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
            segmentLeaderEpochs
        );

        RemoteLogSegmentId nextSegmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        nextMetadata = new RemoteLogSegmentMetadata(
            nextSegmentId,
            101L,
            200L,
            2000L,
            2,
            System.currentTimeMillis(),
            1024,
            Optional.empty(),
            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
            Collections.singletonMap(1, 101L)
        );

        nextSegmentOffsetAndEpoch = new OffsetAndEpoch(101L, 1);

        // Create a real prefetcher but with a mock segment manager
        prefetcher = new RemoteDataPrefetcherImpl(
            mockEvaluator,
            mockSegmentManager,
            mockRlmmSupplier
        );
    }

    @Test
    public void testConfigure() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("key1", "value1");

        prefetcher.configure(configs);

        // Currently configure does nothing, so there's nothing to verify
    }

    @Test
    public void testSignalSegmentReadWithPrefetch() throws RemoteStorageException {
        // Set up the evaluator to recommend prefetching
        when(mockEvaluator.shouldPrefetch(metadata, 10)).thenReturn(true);

        // Set up the RLMM to return the next segment metadata
        when(mockRlmm.remoteLogSegmentMetadata(
                eq(metadata.remoteLogSegmentId().topicIdPartition()),
                eq(nextSegmentOffsetAndEpoch.leaderEpoch()),
                eq(nextSegmentOffsetAndEpoch.offset())))
            .thenReturn(Optional.of(nextMetadata));

        // Call the method under test
        prefetcher.signalSegmentRead(metadata, 10, nextSegmentOffsetAndEpoch);

        // Verify that the segment manager was asked to download the next segment
        verify(mockSegmentManager).downloadSegment(nextMetadata);
    }

    @Test
    public void testSignalSegmentReadWithoutPrefetch() throws RemoteStorageException {
        // Set up the evaluator to not recommend prefetching
        when(mockEvaluator.shouldPrefetch(metadata, 10)).thenReturn(false);

        // Call the method under test
        prefetcher.signalSegmentRead(metadata, 10, nextSegmentOffsetAndEpoch);

        // Verify that the segment manager was not asked to download any segment
        verify(mockSegmentManager, never()).downloadSegment(any());
        // Verify that RLMM was not called
        verify(mockRlmm, never()).remoteLogSegmentMetadata(any(), anyInt(), anyInt());
    }

    @Test
    public void testSignalSegmentReadWithNullNextSegment() throws RemoteStorageException {
        // Set up the evaluator to recommend prefetching
        when(mockEvaluator.shouldPrefetch(metadata, 10)).thenReturn(true);

        // Call the method under test with null nextSegmentOffsetAndEpoch
        prefetcher.signalSegmentRead(metadata, 10, null);

        // Verify that the segment manager was not asked to download any segment
        verify(mockSegmentManager, never()).downloadSegment(any());
        // Verify that RLMM was not called
        verify(mockRlmm, never()).remoteLogSegmentMetadata(any(), anyInt(), anyInt());
    }

    @Test
    public void testSignalSegmentReadWithNoNextSegmentFound() throws RemoteStorageException {
        // Set up the evaluator to recommend prefetching
        when(mockEvaluator.shouldPrefetch(metadata, 10)).thenReturn(true);

        // Set up the RLMM to return no next segment metadata
        when(mockRlmm.remoteLogSegmentMetadata(
                eq(metadata.remoteLogSegmentId().topicIdPartition()),
                eq(nextSegmentOffsetAndEpoch.leaderEpoch()),
                eq(nextSegmentOffsetAndEpoch.offset())))
            .thenReturn(Optional.empty());

        // Call the method under test
        prefetcher.signalSegmentRead(metadata, 10, nextSegmentOffsetAndEpoch);

        // Verify that the segment manager was not asked to download any segment
        verify(mockSegmentManager, never()).downloadSegment(any());
    }

    @Test
    public void testSignalSegmentReadWithRlmmException() throws RemoteStorageException {
        // Set up the evaluator to recommend prefetching
        when(mockEvaluator.shouldPrefetch(metadata, 10)).thenReturn(true);

        // Set up the RLMM to throw an exception
        when(mockRlmm.remoteLogSegmentMetadata(
                eq(metadata.remoteLogSegmentId().topicIdPartition()),
                eq(nextSegmentOffsetAndEpoch.leaderEpoch()),
                eq(nextSegmentOffsetAndEpoch.offset())))
            .thenThrow(new RemoteStorageException("Test exception"));

        // Call the method under test
        prefetcher.signalSegmentRead(metadata, 10, nextSegmentOffsetAndEpoch);

        // Verify that the segment manager was not asked to download any segment
        verify(mockSegmentManager, never()).downloadSegment(any());
    }

    @Test
    public void testFetchLogSegmentWithPrefetchedData() throws IOException {
        // Set up the segment manager to return a prefetched segment
        InputStream expectedStream = new ByteArrayInputStream(new byte[]{1, 2, 3});
        when(mockSegmentManager.fetchLogSegment(metadata.remoteLogSegmentId(), 10, 50))
            .thenReturn(expectedStream);

        // Call the method under test
        InputStream result = prefetcher.fetchLogSegment(metadata, 10, 50);

        // Verify the result
        assertEquals(expectedStream, result);
    }

    @Test
    public void testFetchLogSegmentWithNoPrefetchedData() throws IOException {
        // Set up the segment manager to return no prefetched segment
        when(mockSegmentManager.fetchLogSegment(metadata.remoteLogSegmentId(), 10, 50))
            .thenReturn(null);

        // Call the method under test
        InputStream result = prefetcher.fetchLogSegment(metadata, 10, 50);

        // Verify the result
        assertNull(result);
    }

    @Test
    public void testFetchLogSegmentWithException() throws IOException {
        // Set up the segment manager to throw an exception
        when(mockSegmentManager.fetchLogSegment(metadata.remoteLogSegmentId(), 10, 50))
            .thenThrow(new RuntimeException("Test exception"));

        // Call the method under test
        InputStream result = prefetcher.fetchLogSegment(metadata, 10, 50);

        // Verify the result
        assertNull(result);
    }

    @Test
    public void testCleanup() {
        // Call the method under test
        prefetcher.cleanup();

        // Verify that the segment manager's cleanup method was called
        verify(mockSegmentManager).cleanup();
    }
}
