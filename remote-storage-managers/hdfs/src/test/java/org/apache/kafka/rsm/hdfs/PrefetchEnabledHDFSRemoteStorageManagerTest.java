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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.rsm.hdfs.prefetch.RemoteDataPrefetcher;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteReadContext;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrefetchEnabledHDFSRemoteStorageManagerTest {

    private HDFSRemoteStorageManager mockHdfsRsm;
    private RemoteDataPrefetcher mockPrefetcher;
    private PrefetchEnabledHDFSRemoteStorageManager manager;
    private RemoteLogSegmentMetadata metadata;
    private OffsetAndEpoch nextSegmentOffsetAndEpoch;

    @BeforeEach
    public void setup() {
        // Create mocks
        mockHdfsRsm = mock(HDFSRemoteStorageManager.class);
        mockPrefetcher = mock(RemoteDataPrefetcher.class);

        // Create a real manager
        manager = new PrefetchEnabledHDFSRemoteStorageManager(mockHdfsRsm, mockPrefetcher);

        // Create test metadata
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        Map<Integer, Long> segmentLeaderEpochs = Collections.singletonMap(5, 100L);

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

        nextSegmentOffsetAndEpoch = new OffsetAndEpoch(101L, 10);
    }

    @Test
    public void testConfigure() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("key1", "value1");

        manager.configure(configs);

        verify(mockHdfsRsm).configure(configs);
        verify(mockPrefetcher).configure(configs);
    }

    @Test
    public void testReconfigurableConfigs() {
        Set<String> expectedConfigs = Collections.singleton("config1");
        when(mockHdfsRsm.reconfigurableConfigs()).thenReturn(expectedConfigs);

        Set<String> result = manager.reconfigurableConfigs();

        assertEquals(expectedConfigs, result);
        verify(mockHdfsRsm).reconfigurableConfigs();
    }

    @Test
    public void testValidateReconfiguration() throws ConfigException {
        Map<String, Object> configs = new HashMap<>();
        configs.put("key1", "value1");

        manager.validateReconfiguration(configs);

        verify(mockHdfsRsm).validateReconfiguration(configs);
    }

    @Test
    public void testReconfigure() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("key1", "value1");

        manager.reconfigure(configs);

        verify(mockHdfsRsm).reconfigure(configs);
    }

    @Test
    public void testCopyLogSegmentData() throws RemoteStorageException {
        LogSegmentData logSegmentData = mock(LogSegmentData.class);
        RemoteLogSegmentMetadata.CustomMetadata expectedMetadata = new RemoteLogSegmentMetadata.CustomMetadata(new byte[]{1, 2, 3});
        when(mockHdfsRsm.copyLogSegmentData(metadata, logSegmentData)).thenReturn(Optional.of(expectedMetadata));

        Optional<RemoteLogSegmentMetadata.CustomMetadata> result = manager.copyLogSegmentData(metadata, logSegmentData);

        assertEquals(Optional.of(expectedMetadata), result);
        verify(mockHdfsRsm).copyLogSegmentData(metadata, logSegmentData);
    }

    @Test
    public void testFetchLogSegmentWithoutReadContext() throws RemoteStorageException {
        InputStream expectedStream = new ByteArrayInputStream(new byte[]{1, 2, 3});
        when(mockPrefetcher.fetchLogSegment(metadata, 10, 50)).thenReturn(null);
        when(mockHdfsRsm.fetchLogSegment(metadata, 10, 50)).thenReturn(expectedStream);

        InputStream result = manager.fetchLogSegment(metadata, 10, 50);

        assertSame(expectedStream, result);
        verify(mockPrefetcher).fetchLogSegment(metadata, 10, 50);
        verify(mockHdfsRsm).fetchLogSegment(metadata, 10, 50);
        verify(mockPrefetcher, never()).signalSegmentRead(any(), anyInt(), any());
    }

    @Test
    public void testFetchLogSegmentWithReadContextAndPrefetchedData() throws RemoteStorageException {
        RemoteReadContext readContext = RemoteReadContext.builder()
                .withBlockPrefetchEnabled(true)
                .withHedgedReadsEnabled(false)
                .withNextSegmentOffsetAndEpoch(nextSegmentOffsetAndEpoch)
                .withSegmentPrefetchEnabled(false)
                .build();
        InputStream expectedStream = new ByteArrayInputStream(new byte[]{1, 2, 3});
        when(mockPrefetcher.fetchLogSegment(metadata, 10, 50)).thenReturn(expectedStream);

        InputStream result = manager.fetchLogSegment(metadata, readContext, 10, 50);

        assertSame(expectedStream, result);
        verify(mockPrefetcher).signalSegmentRead(metadata, 10, nextSegmentOffsetAndEpoch);
        verify(mockPrefetcher).fetchLogSegment(metadata, 10, 50);
        verify(mockHdfsRsm, never()).fetchLogSegment(any(), any(), anyInt(), anyInt());
    }

    @Test
    public void testFetchLogSegmentWithReadContextNoPrefetchedData() throws RemoteStorageException {
        RemoteReadContext readContext = RemoteReadContext.builder()
                .withBlockPrefetchEnabled(true)
                .withHedgedReadsEnabled(false)
                .withNextSegmentOffsetAndEpoch(nextSegmentOffsetAndEpoch)
                .withSegmentPrefetchEnabled(false)
                .build();
        InputStream expectedStream = new ByteArrayInputStream(new byte[]{1, 2, 3});
        when(mockPrefetcher.fetchLogSegment(metadata, 10, 50)).thenReturn(null);
        when(mockHdfsRsm.fetchLogSegment(metadata, readContext, 10, 50)).thenReturn(expectedStream);

        InputStream result = manager.fetchLogSegment(metadata, readContext, 10, 50);

        assertSame(expectedStream, result);
        verify(mockPrefetcher).signalSegmentRead(metadata, 10, nextSegmentOffsetAndEpoch);
        verify(mockPrefetcher).fetchLogSegment(metadata, 10, 50);
        verify(mockHdfsRsm).fetchLogSegment(metadata, readContext, 10, 50);
    }

    @Test
    public void testFetchLogSegmentWithReadContextNullNextSegment() throws RemoteStorageException {
        RemoteReadContext readContext = RemoteReadContext.builder()
                .withBlockPrefetchEnabled(true)
                .withHedgedReadsEnabled(false)
                .withNextSegmentOffsetAndEpoch(null)
                .withSegmentPrefetchEnabled(false)
                .build();
        InputStream expectedStream = new ByteArrayInputStream(new byte[]{1, 2, 3});
        when(mockPrefetcher.fetchLogSegment(metadata, 10, 50)).thenReturn(null);
        when(mockHdfsRsm.fetchLogSegment(metadata, readContext, 10, 50)).thenReturn(expectedStream);

        InputStream result = manager.fetchLogSegment(metadata, readContext, 10, 50);

        assertSame(expectedStream, result);
        verify(mockPrefetcher).signalSegmentRead(metadata, 10, null);
        verify(mockPrefetcher).fetchLogSegment(metadata, 10, 50);
        verify(mockHdfsRsm).fetchLogSegment(metadata, readContext, 10, 50);
    }

    @Test
    public void testFetchLogSegmentWithReadContextSignalException() throws RemoteStorageException {
        RemoteReadContext readContext = RemoteReadContext.builder()
                .withBlockPrefetchEnabled(true)
                .withHedgedReadsEnabled(false)
                .withNextSegmentOffsetAndEpoch(nextSegmentOffsetAndEpoch)
                .withSegmentPrefetchEnabled(false)
                .build();
        InputStream expectedStream = new ByteArrayInputStream(new byte[]{1, 2, 3});
        Mockito.doThrow(new RuntimeException("Test exception")).when(mockPrefetcher).signalSegmentRead(metadata, 10, nextSegmentOffsetAndEpoch);
        when(mockPrefetcher.fetchLogSegment(metadata, 10, 50)).thenReturn(null);
        when(mockHdfsRsm.fetchLogSegment(metadata, readContext, 10, 50)).thenReturn(expectedStream);

        InputStream result = manager.fetchLogSegment(metadata, readContext, 10, 50);

        assertSame(expectedStream, result);
        verify(mockPrefetcher).signalSegmentRead(metadata, 10, nextSegmentOffsetAndEpoch);
        verify(mockPrefetcher).fetchLogSegment(metadata, 10, 50);
        verify(mockHdfsRsm).fetchLogSegment(metadata, readContext, 10, 50);
    }

    @Test
    public void testFetchIndex() throws RemoteStorageException {
        IndexType indexType = IndexType.OFFSET;
        InputStream expectedStream = new ByteArrayInputStream(new byte[]{1, 2, 3});
        when(mockHdfsRsm.fetchIndex(metadata, indexType)).thenReturn(expectedStream);

        InputStream result = manager.fetchIndex(metadata, indexType);

        assertSame(expectedStream, result);
        verify(mockHdfsRsm).fetchIndex(metadata, indexType);
    }

    @Test
    public void testDeleteLogSegmentData() throws RemoteStorageException {
        manager.deleteLogSegmentData(metadata);

        verify(mockHdfsRsm).deleteLogSegmentData(metadata);
    }

    @Test
    public void testDeletePartition() throws RemoteStorageException {
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
        List<RemoteLogSegmentMetadata> segmentMetadataList = Collections.singletonList(metadata);

        manager.deletePartition(topicIdPartition, segmentMetadataList);

        verify(mockHdfsRsm).deletePartition(topicIdPartition, segmentMetadataList);
    }

    @Test
    public void testClose() {
        manager.close();

        verify(mockPrefetcher).cleanup();
        verify(mockHdfsRsm).close();
    }
}
