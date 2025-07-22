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
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;

import com.github.benmanes.caffeine.cache.RemovalCause;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class CacheRemovalListenerTest {

    private CacheRemovalListener listener;
    private RemoteLogSegmentId segmentId;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void setup() {
        listener = new CacheRemovalListener();

        // Create test segment ID
        TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
        segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
    }

    @Test
    public void testOnRemovalWithValidCacheValue() throws IOException {
        // Create a test file
        Path testFilePath = tempDir.resolve("test-file.log");
        Files.createFile(testFilePath);
        assertTrue(Files.exists(testFilePath), "Test file should exist");

        // Create a FileChannel for the test file
        FileChannel fileChannel = FileChannel.open(testFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        assertTrue(fileChannel.isOpen(), "FileChannel should be open");

        // Create a CacheValue with the test file and channel
        CacheValue cacheValue = new CacheValue(PrefetchStatus.SUCCESS, testFilePath, fileChannel);

        // Call the method under test
        listener.onRemoval(segmentId, cacheValue, RemovalCause.EXPLICIT);

        // Verify that the file was deleted and the channel was closed
        assertFalse(Files.exists(testFilePath), "File should be deleted");
        assertFalse(fileChannel.isOpen(), "FileChannel should be closed");
    }

    @Test
    public void testOnRemovalWithNullCacheValue() {
        // Call the method under test with null CacheValue
        listener.onRemoval(segmentId, null, RemovalCause.EXPLICIT);

        // No exception should be thrown
    }

    @Test
    public void testOnRemovalWithNullPath() throws IOException {
        // Create a mock FileChannel
        FileChannel mockFileChannel = mock(FileChannel.class);

        // Create a CacheValue with null path
        CacheValue cacheValue = new CacheValue(PrefetchStatus.SUCCESS, null, mockFileChannel);

        // Call the method under test
        listener.onRemoval(segmentId, cacheValue, RemovalCause.EXPLICIT);

        // Verify that close was not called on the FileChannel
        verifyNoInteractions(mockFileChannel);
    }

    @Test
    public void testOnRemovalWithIOExceptionOnClose() throws IOException {
        // Create a test file
        Path testFilePath = tempDir.resolve("test-file.log");
        Files.createFile(testFilePath);

        // Create a mock FileChannel that throws IOException on close
        FileChannel mockFileChannel = mock(FileChannel.class);
        doThrow(new IOException("Test exception")).when(mockFileChannel).close();

        // Create a CacheValue with the test file and mock channel
        CacheValue cacheValue = new CacheValue(PrefetchStatus.SUCCESS, testFilePath, mockFileChannel);

        // Call the method under test
        listener.onRemoval(segmentId, cacheValue, RemovalCause.EXPLICIT);

        // Verify that close was called on the FileChannel
        verify(mockFileChannel).close();

        // The file should still exist because deletion is skipped when close fails
        assertTrue(Files.exists(testFilePath), "File should still exist when close fails");
    }

    @Test
    public void testOnRemovalWithIOExceptionOnDelete() throws IOException {
        // Create a test file in a non-existent directory to cause deletion to fail
        Path nonExistentDir = Paths.get("/non-existent-dir");
        Path testFilePath = nonExistentDir.resolve("test-file.log");

        // Create a mock FileChannel
        FileChannel mockFileChannel = mock(FileChannel.class);

        // Create a CacheValue with the non-existent file path
        CacheValue cacheValue = new CacheValue(PrefetchStatus.SUCCESS, testFilePath, mockFileChannel);

        // Call the method under test
        listener.onRemoval(segmentId, cacheValue, RemovalCause.EXPLICIT);

        // Verify that close was called on the FileChannel
        verify(mockFileChannel).close();

        // No exception should be thrown
    }
}
