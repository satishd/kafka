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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.rsm.hdfs.prefetch.RSMTestUtils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.test.TestUtils;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.kafka.rsm.hdfs.RSMUtils.DELIMITER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RSMUtilsTest {

    private File tempDir;
    private TopicIdPartition topicIdPartition;
    private RemoteLogSegmentId segmentId;
    private RemoteLogSegmentMetadata segmentMetadata;

    @BeforeEach
    public void setup() {
        tempDir = TestUtils.tempDirectory();
        TopicPartition topicPartition = new TopicPartition("test-topic", 0);
        Uuid topicId = Uuid.randomUuid();
        topicIdPartition = new TopicIdPartition(topicId, topicPartition);
        segmentId = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
        segmentMetadata = new RemoteLogSegmentMetadata(
                segmentId,
                0L,
                100L,
                -1L,
                0, // brokerId
                System.currentTimeMillis(), // eventTimestampMs
                1000, // segmentSizeInBytes
                Optional.empty(), // customMetadata
                RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                Collections.singletonMap(0, 0L) // segmentLeaderEpochs
        );
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (tempDir != null) {
            Utils.delete(tempDir);
        }
    }

    @Test
    public void testSegmentPrefetchPath() {
        String downloadDirectory = "/tmp/download";
        String expected = downloadDirectory + Path.SEPARATOR + segmentId.topicIdPartition() + DELIMITER + segmentId.id() + DELIMITER + segmentMetadata.startOffset();
        String actual = RSMUtils.segmentPrefetchPath(downloadDirectory, segmentMetadata);
        assertEquals(expected, actual);
    }

    @Test
    public void testGetSegmentRemoteDir() {
        String baseDir = "/tmp/remote";
        String expected = baseDir + Path.SEPARATOR + segmentId.topicIdPartition().topicPartition() + "-" + segmentId.topicIdPartition().topicId() + Path.SEPARATOR + segmentId.id();
        String actual = RSMUtils.getSegmentRemoteDir(baseDir, segmentId);
        assertEquals(expected, actual);
    }

    @Test
    public void testGetPartitionRemoteDir() {
        String baseDir = "/tmp/remote";
        String expected = baseDir + Path.SEPARATOR + topicIdPartition.topicPartition() + "-" + topicIdPartition.topicId();
        String actual = RSMUtils.getPartitionRemoteDir(baseDir, topicIdPartition);
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @CsvSource({
        "100, 10, 0, 100, 10",
        "100, 100, 0, 100, 100",
        "100, 200, 0, 100, 100",
        "100, 10, 50, 100, 10",
        "100, 100, 50, 100, 50",
        "100, 200, 50, 100, 50",
        "100, 10, 50, 1000, 10",
        "100, 100, 50, 1000, 50",
        "100, 200, 50, 1000, 50",
    })
    public void testGetInputStreamFromChannelWithEndPosition(int segmentSize, int readBufLen, int startPos, int endPos, int expectedBytesRead) throws IOException {
        testGetInputStreamFromChannelInternal(segmentSize, readBufLen, startPos, endPos, expectedBytesRead);
    }

    @ParameterizedTest
    @CsvSource({
        "100, 10, 0, 10",
        "100, 100, 0, 100",
        "100, 200, 0, 100",
        "100, 10, 50, 10",
        "100, 100, 50, 50",
        "100, 200, 50, 50",
        "100, 10, 50, 10",
        "100, 100, 50, 50",
        "100, 200, 50, 50",
    })
    public void testGetInputStreamFromChannelWithoutEndPosition(int segmentSize, int readBufLen, int startPos, int expectedBytesRead) throws IOException {
        testGetInputStreamFromChannelInternal(segmentSize, readBufLen, startPos, Integer.MAX_VALUE, expectedBytesRead);
    }

    private void testGetInputStreamFromChannelInternal(int segmentSize, int readBufLen, int startPos, int endPos, int expectedBytesRead) throws IOException {
        // Create a simple test file with a valid header and some data
        File testFile = new File(tempDir, "test-file");
        byte[] testData = TestUtils.randomBytes(segmentSize);

        try (FileChannel channel = FileChannel.open(testFile.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            RSMTestUtils.writeData(channel, testData);
        }

        // Test that we can create an InputStream from the channel
        try (FileChannel channel = FileChannel.open(testFile.toPath(), StandardOpenOption.READ)) {
            InputStream inputStream = RSMUtils.getInputStreamFromChannel(channel, startPos, endPos);
            byte[] buffer = new byte[readBufLen];
            int bytesRead = inputStream.read(buffer);

            assertEquals(expectedBytesRead, bytesRead);
            // Verify the read data matches the original data
            byte[] expectedData = new byte[expectedBytesRead];
            System.arraycopy(testData, startPos, expectedData, 0, expectedBytesRead);
            assertArrayEquals(expectedData, Arrays.copyOfRange(buffer, 0, bytesRead));
        }
    }

    @Test
    public void testGetInputStreamFromChannelWithInvalidHeader() throws IOException {
        // Create a test file with invalid header (using an unsupported version)
        File testFile = new File(tempDir, "invalid-header-file");

        // Create a header with an invalid version
        byte[] invalidHeader = new byte[LogSegmentDataHeader.LENGTH];
        // Set the first byte (version) to an invalid value
        invalidHeader[0] = (byte) (LogSegmentDataHeader.CURRENT_VERSION + 1);

        try (FileChannel channel = FileChannel.open(testFile.toPath(), 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(invalidHeader));
        }

        // Test that an UnsupportedVersionException is thrown when reading an invalid header
        try (FileChannel channel = FileChannel.open(testFile.toPath(), StandardOpenOption.READ)) {
            assertThrows(UnsupportedVersionException.class, () -> RSMUtils.getInputStreamFromChannel(channel, 0, Integer.MAX_VALUE));
        }
    }
}
