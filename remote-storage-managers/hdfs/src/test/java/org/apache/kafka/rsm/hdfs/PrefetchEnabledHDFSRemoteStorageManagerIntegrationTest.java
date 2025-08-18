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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.rsm.hdfs.prefetch.RSMTestUtils;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteReadContext;
import org.apache.kafka.test.TestUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DEFAULT_FS_URI_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_REQUESTS_PER_SEC;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_REQUEST_SUCCESS_PER_SEC;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.PREFETCH_SEGMENT_READS_PER_SEC;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManagerConfig.METRICS;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageManagerConfig.REMOTE_LOG_METADATA_MANAGER_SUPPLIER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrefetchEnabledHDFSRemoteStorageManagerIntegrationTest {

    /**
     * Validates the behavior of prefetching and reading log segments in the {@link PrefetchEnabledHDFSRemoteStorageManager}.
     * <p>
     * This method performs the following:
     * 1. Sets up a simulated environment using an HDFS MiniDFSCluster to mimic remote storage behavior.
     * 2. Creates mock metadata for remote log segments and initializes the remote storage manager.
     * 3. Prepares and configures the remote storage manager with required configuration properties.
     * 4. Copies log segment data to simulate log segment availability on remote storage.
     * 5. Establishes a read context with prefetching enabled.
     * 6. Performs a fetch operation on the first log segment, which triggers prefetching for the next segment.
     * 7. Validates prefetch metrics to ensure the prefetching mechanism is triggered and successful.
     * 8. Reads the prefetched segment data to validate that the prefetched data is served correctly.
     * 9. Confirms data integrity by comparing prefetched data against original segment data.
     *
     * @throws Exception if an error occurs during the setup, configuration, or validation process.
     */
    @Test
    public void testPrefetchRead() throws Exception {
        // Constants
        final int segmentSize = 10 * 1024 * 1024;
        final int bufferSize = 1024 * 1000; // 1000 KB read buffer
        final TopicIdPartition topicPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test", 1));

        // Setup test directories
        File localLogDir = TestUtils.tempDirectory();
        File remoteStorageDir = TestUtils.tempDirectory();

        // Setup HDFS cluster and configuration
        MiniDFSCluster hdfsCluster = setupHdfsCluster(remoteStorageDir);
        Configuration hadoopConfig = hdfsCluster.getConfiguration(0);
        String hdfsUri = hdfsCluster.getFileSystem().getDefaultUri().toString();

        // Setup segment metadata
        RemoteLogSegmentMetadata firstSegmentMetadata = createSegmentMetadata(topicPartition, 0, 100, segmentSize);
        RemoteLogSegmentMetadata secondSegmentMetadata = createSegmentMetadata(topicPartition, 101, 200, segmentSize);

        // Setup mock metadata manager
        RemoteLogMetadataManager remoteLogMetadataManager = mock(RemoteLogMetadataManager.class);
        when(remoteLogMetadataManager.remoteLogSegmentMetadata(topicPartition, 0, 101L))
            .thenReturn(Optional.of(secondSegmentMetadata));
        Supplier<RemoteLogMetadataManager> rlmmSupplier = () -> remoteLogMetadataManager;

        // Setup metrics for Quota Manager instantiation
        Metrics metrics = new Metrics();

        try (PrefetchEnabledHDFSRemoteStorageManager rsm = new PrefetchEnabledHDFSRemoteStorageManager()) {
            // Configure storage manager
            rsm.setDefaultHadoopConfiguration(hadoopConfig);
            RSMTestUtils.clearKafkaMetrics();
            Map<String, Object> configs = new HashMap<>();
            configs.put(HDFS_BASE_DIR_PROP, "kafka-remote-logs");
            configs.put(HDFS_DEFAULT_FS_URI_PROP, hdfsUri);
            configs.put(REMOTE_LOG_METADATA_MANAGER_SUPPLIER, rlmmSupplier);
            configs.put(METRICS, metrics);
            rsm.configure(configs);

            // Verify initial metrics
            verifyMeter(PREFETCH_REQUESTS_PER_SEC, 0);
            verifyMeter(PREFETCH_REQUEST_SUCCESS_PER_SEC, 0);

            // Copy test segments to remote storage
            LogSegmentData ignored = copyLogSegmentData(rsm, localLogDir, firstSegmentMetadata, 0, segmentSize);
            LogSegmentData secondSegmentData = copyLogSegmentData(rsm, localLogDir, secondSegmentMetadata, 101, segmentSize);

            // Read first segment to trigger prefetch of second segment
            RemoteReadContext firstReadContext = buildRemoteReadContext(new OffsetAndEpoch(101L, 0));
            rsm.fetchLogSegment(firstSegmentMetadata, firstReadContext, 10, 20);

            // Verify prefetch was triggered
            verifyMeterWithTimeout(Duration.ofSeconds(1), PREFETCH_REQUESTS_PER_SEC, 1);
            // Verify prefetch succeeded
            verifyMeterWithTimeout(Duration.ofSeconds(5), PREFETCH_REQUEST_SUCCESS_PER_SEC, 1);

            // Read and verify the prefetched segment
            int startPosition = 0;
            int endPosition = bufferSize - 1;
            int prefetchReadCount = 0;
            // Verify no reads of the prefetched segment yet.
            verifyMeter(PREFETCH_SEGMENT_READS_PER_SEC, 0);

            RemoteReadContext secondReadContext = buildRemoteReadContext(new OffsetAndEpoch(201L, 0));
            while (startPosition < segmentSize) {
                try (InputStream prefetchStream = rsm.fetchLogSegment(
                    secondSegmentMetadata, secondReadContext, startPosition, endPosition)) {
                    verifyMeter(PREFETCH_SEGMENT_READS_PER_SEC, ++prefetchReadCount);

                    ByteBuffer buffer = ByteBuffer.wrap(new byte[endPosition - startPosition + 1]);
                    try (SeekableByteChannel byteChannel = Files.newByteChannel(secondSegmentData.logSegment())) {
                        byteChannel.position(startPosition);
                        byteChannel.read(buffer);
                        buffer.rewind();
                        assertDataEquals(buffer, prefetchStream);
                    }
                }

                startPosition = endPosition + 1;
                endPosition = Math.min(segmentSize - 1, endPosition + bufferSize);
            }

            // Verify no more prefetches were carried out (metrics are unchanged)
            verifyMeter(PREFETCH_REQUESTS_PER_SEC, 1);
            verifyMeter(PREFETCH_REQUEST_SUCCESS_PER_SEC, 1);
        }
    }

    private RemoteReadContext buildRemoteReadContext(OffsetAndEpoch nextSegmentOffsetAndEpoch) {
        return RemoteReadContext.builder()
            .withSegmentPrefetchEnabled(true)
            .withNextSegmentOffsetAndEpoch(nextSegmentOffsetAndEpoch)
            .build();
    }

    private LogSegmentData copyLogSegmentData(PrefetchEnabledHDFSRemoteStorageManager rsm,
                                              File localLogDir,
                                              RemoteLogSegmentMetadata metadata,
                                              int startOffset,
                                              int segmentSize) throws Exception {
        LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(localLogDir, startOffset, segmentSize, false);
        rsm.copyLogSegmentData(metadata, segmentData);
        return segmentData;
    }

    private MiniDFSCluster setupHdfsCluster(File remoteDir) throws IOException {
        int nameNodePort = new Random().nextInt(5000);
        Configuration hadoopConf = new Configuration();
        hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, remoteDir.getAbsolutePath());
        hadoopConf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:" + nameNodePort);

        return new MiniDFSCluster.Builder(hadoopConf)
            .clusterId("test_mini_dfs_cluster")
            .build();
    }

    private RemoteLogSegmentMetadata createSegmentMetadata(
        TopicIdPartition tp, long startOffset, long endOffset, int size) {
        return new RemoteLogSegmentMetadata(
            new RemoteLogSegmentId(tp, Uuid.randomUuid()),
            startOffset, endOffset, 0, 0, 1L, size,
            Collections.singletonMap(0, 0L));
    }

    private void verifyMeter(String name, long expectedCount) {
        RSMTestUtils.verifyMeter(PrefetchEnabledHDFSRemoteStorageManager.class, name, expectedCount);
    }

    private void verifyMeterWithTimeout(Duration timeout, String name, long expectedCount) {
        RSMTestUtils.verifyMeterWithTimeout(timeout, PrefetchEnabledHDFSRemoteStorageManager.class, name, expectedCount);
    }

    private void assertDataEquals(ByteBuffer expectedBuffer, InputStream actual) throws Exception {
        ByteBuffer actualBuffer = ByteBuffer.wrap(new byte[actual.available()]);
        int read = actual.read(actualBuffer.array());
        assertEquals(expectedBuffer.limit(), read);
        assertEquals(expectedBuffer, actualBuffer);
    }
}
