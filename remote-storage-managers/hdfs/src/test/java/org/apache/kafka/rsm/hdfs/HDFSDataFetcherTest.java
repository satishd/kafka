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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageProvider;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HDFSDataFetcherTest {

    private static final String HADOOP_BASE_DIR = "/kafka-remote-logs";
    private FileSystemManager mockFileSystemManager;
    private FileSystem mockFileSystem;
    private HDFSDataFetcher dataFetcher;
    private RemoteLogSegmentMetadata metadata;
    private RemoteLogSegmentId segmentId;
    private String expectedFilePath;
    private ArgumentCaptor<FileSystemOptions> fileSystemOptionsCapture;

    @BeforeEach
    public void setup() {
        // Create mocks
        mockFileSystemManager = mock(FileSystemManager.class);
        mockFileSystem = mock(FileSystem.class);

        // Create the data fetcher
        dataFetcher = new HDFSDataFetcher(HADOOP_BASE_DIR, mockFileSystemManager);

        // Create test metadata
        TopicIdPartition topicIdPartition = new TopicIdPartition(
            Uuid.randomUuid(), 
            new TopicPartition("test-topic", 0)
        );
        segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        metadata = new RemoteLogSegmentMetadata(
            segmentId,
            0L,
            100L,
            -1L,
            1,
            System.currentTimeMillis(),
            1024,
            Optional.empty(),
            RemoteLogSegmentState.COPY_SEGMENT_FINISHED,
            Collections.singletonMap(0, 0L)
        );

        // Set up the expected file path
        String bucket = "hdfs://localhost:9000";
        expectedFilePath = bucket + RSMUtils.getSegmentRemoteDir(HADOOP_BASE_DIR, segmentId);

        // Set up the mock FileSystemManager to return our mock FileSystem
        when(mockFileSystemManager.getBucket(metadata)).thenReturn("hdfs://localhost:9000");
        fileSystemOptionsCapture = ArgumentCaptor.forClass(FileSystemOptions.class);
        when(mockFileSystemManager.getFS(fileSystemOptionsCapture.capture())).thenReturn(mockFileSystem);
    }

    @Test
    public void testFetchSegmentData() throws IOException {
        // Set up the mock FileSystem to return a mock FSDataInputStream
        FSDataInputStream mockInputStream = mock(FSDataInputStream.class);
        when(mockFileSystem.open(new Path(expectedFilePath))).thenReturn(mockInputStream);

        // Call the method under test
        FSDataInputStream result = dataFetcher.fetchSegmentData(metadata);

        // Verify the result
        assertNotNull(result);
        assertEquals(mockInputStream, result);

        // Verify that the correct methods were called on the mocks
        verify(mockFileSystemManager, times(2)).getBucket(metadata);
        verify(mockFileSystemManager).getFS(fileSystemOptionsCapture.capture());
        verify(mockFileSystem).open(new Path(expectedFilePath));

        // Verify FileSystemOptions had the right configurations
        FileSystemOptions fileSystemOptions = fileSystemOptionsCapture.getValue();
        assertEquals("hdfs://localhost:9000", fileSystemOptions.bucket());
        assertTrue(fileSystemOptions.hedgedReadsEnabled());
        assertTrue(fileSystemOptions.readAheadEnabled());
    }

    @Test
    public void testFileLength() throws IOException {
        // Set up the mock FileSystem to return a mock FileStatus
        FileStatus mockFileStatus = mock(FileStatus.class);
        when(mockFileStatus.getLen()).thenReturn(1024L);
        when(mockFileSystem.getFileStatus(new Path(expectedFilePath))).thenReturn(mockFileStatus);

        // Call the method under test
        long result = dataFetcher.fileLength(metadata);

        // Verify the result
        assertEquals(1024L, result);

        // Verify that the correct methods were called on the mocks
        verify(mockFileSystemManager, times(2)).getBucket(metadata);
        verify(mockFileSystemManager).getFS(fileSystemOptionsCapture.capture());
        verify(mockFileSystem).getFileStatus(new Path(expectedFilePath));
        verify(mockFileStatus).getLen();

        // Verify FileSystemOptions had the right configurations
        FileSystemOptions fileSystemOptions = fileSystemOptionsCapture.getValue();
        assertEquals("hdfs://localhost:9000", fileSystemOptions.bucket());
        assertTrue(fileSystemOptions.hedgedReadsEnabled());
        assertTrue(fileSystemOptions.readAheadEnabled());
    }

    @Test
    public void testStorageProvider() {
        // Set up the mock FileSystemManager to return a mock RemoteStorageProvider
        RemoteStorageProvider hdfsProvider = RemoteStorageProvider.HDFS;
        when(mockFileSystemManager.getRemoteStorageProvider("hdfs://localhost:9000"))
                .thenReturn(hdfsProvider);
        assertEquals(hdfsProvider, dataFetcher.storageProvider(metadata));

        RemoteStorageProvider ociProvider = RemoteStorageProvider.OCI;
        when(mockFileSystemManager.getRemoteStorageProvider("oci://bucket-name"))
                .thenReturn(ociProvider);
        when(mockFileSystemManager.getBucket(metadata)).thenReturn("oci://bucket-name");
        assertEquals(ociProvider, dataFetcher.storageProvider(metadata));
    }
}
