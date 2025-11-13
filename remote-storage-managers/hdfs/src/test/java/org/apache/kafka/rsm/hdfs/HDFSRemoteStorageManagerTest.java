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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.rsm.hdfs.pool.ByteBufferWrapper;
import org.apache.kafka.rsm.hdfs.prefetch.RSMTestUtils;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteReadContext;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageProvider;
import org.apache.kafka.server.log.remote.storage.RetriableRemoteStorageException;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_READ_ERROR_BACKOFF_WAIT_MS;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_COPY_CIRCUIT_BREAKER_STATE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DEFAULT_FS_URI_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DELETE_CIRCUIT_BREAKER_STATE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_KEYTAB_PATH_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_OCI_BUCKETS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_USER_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.FS_OPEN_INPUT_STREAM;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.FS_OPEN_OUTPUT_STREAM;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.FS_STATUS_RATE_AND_TIME_MS;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.HEDGED_READ_OPS;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.HEDGED_READ_OPS_WIN;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.READ_THREADPOOL_EXECUTOR_AVG_IDLE_PERCENT;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.READ_THREADPOOL_EXECUTOR_CORE_POOL_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.READ_THREADPOOL_EXECUTOR_MAX_POOL_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.READ_THREADPOOL_EXECUTOR_POOL_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.READ_THREADPOOL_EXECUTOR_REJECTION_COUNT;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.READ_THREADPOOL_EXECUTOR_TASK_QUEUE_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.SEGMENT_HEADER_READ_RATE_AND_TIME_MS;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.SEGMENT_READ_RATE_AND_TIME_MS;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.SEGMENT_WRITE_BYTES_PER_SEC;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerMetrics.SEGMENT_WRITE_RATE_AND_TIME_MS;
import static org.apache.kafka.rsm.hdfs.prefetch.RSMTestUtils.clearKafkaMetrics;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HDFSRemoteStorageManagerTest {
    private static final int ONE_MB = 1024 * 1024;
    private String baseDir;
    private final TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test", 1));

    private File logDir;
    private File remoteDir;
    private Configuration hadoopConf;
    private MiniDFSCluster hdfsCluster;
    private FileSystem hdfs;
    private Map<String, String> configs;

    private HDFSRemoteStorageManager rsm;
    private String defaultFsUri;
    private final Time time = new MockTime();

    @BeforeEach
    public void setup() throws Exception {
        logDir = TestUtils.tempDirectory();
        remoteDir = TestUtils.tempDirectory();

        int nameNodePort = new Random().nextInt(5000);
        hadoopConf = new Configuration();
        hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, remoteDir.getAbsolutePath());
        hadoopConf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://localhost:" + nameNodePort);

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hadoopConf);
        builder.clusterId("test_mini_dfs_cluster");
        hdfsCluster = builder.build();
        // Note that the URI can have different port allocated than the supplied nameNodePort
        defaultFsUri = hdfsCluster.getFileSystem().getDefaultUri().toString();

        configs = new HashMap<>();
        configs.put(HDFS_BASE_DIR_PROP, "kafka-remote-logs");
        configs.put(HDFS_DEFAULT_FS_URI_PROP, defaultFsUri);

        rsm = new HDFSRemoteStorageManager();
        rsm.setDefaultHadoopConfiguration(hadoopConf);
        rsm.configure(configs);
        rsm.setTime(time);
        hdfs = rsm.getFS(defaultFsUri);
        baseDir = rsm.baseDir();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (rsm != null) {
            rsm.close();
        }
        hdfsCluster.shutdown();
        Utils.delete(logDir);
        Utils.delete(remoteDir);
        clearKafkaMetrics();
    }

    @Test
    public void testConfigAndClose() throws Exception {
        RemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.configure(configs);
        rsm.close();
    }

    @Test
    public void testGetFSBeforeConfigure() {
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            assertThrows(RuntimeException.class, () -> rsm.getFS(defaultFsUri),
                "File system is not initialized");
        }
    }

    @Test
    @Disabled
    public void testCopySegmentUptoMaxSegmentLimitOfTwoGB() throws Exception {
        verifyUpload(rsm, tp, Uuid.randomUuid(), 0, Integer.MAX_VALUE, true);
    }

    @Test
    public void testSecureLogin() {
        System.setProperty("java.security.krb5.realm", "ATHENA.MIT.EDU");
        System.setProperty("java.security.krb5.kdc", "kerberos.mit.edu:88");
        String user = "test@ATHENA.MIT.EDU";

        Map<String, String> secureConfigs = new HashMap<>(configs);
        secureConfigs.put(HDFS_USER_PROP, user);
        secureConfigs.put(HDFS_KEYTAB_PATH_PROP, "test.keytab");
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            Configuration configuration = new Configuration();
            configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
            rsm.setDefaultHadoopConfiguration(configuration);
            assertThrows(RuntimeException.class, () -> rsm.configure(secureConfigs),
                    "Unable to login as user: " + user);
        } finally {
            UserGroupInformation.setConfiguration(new Configuration());
        }
    }

    @Test
    public void testCopyReadAndDelete() throws Exception {
        Uuid uuid = Uuid.randomUuid();
        RemoteLogSegmentMetadata metadata = verifyUpload(rsm, tp, uuid, 0, 1000, true);
        verifyDeleteRemoteLogSegment(rsm, metadata, tp, uuid);
    }

    @Test
    public void testCopyReadAndDeleteWithMultipleSegments() throws Exception {
        Uuid uuid0 = Uuid.randomUuid();
        RemoteLogSegmentMetadata metadata0 = verifyUpload(rsm, tp, uuid0, 0, 1000, true);
        Uuid uuid1 = Uuid.randomUuid();
        RemoteLogSegmentMetadata metadata1 = verifyUpload(rsm, tp, uuid1, 1000, 2000, true);
        verifyDeleteRemoteLogSegment(rsm, metadata0, tp, uuid0);
        verifyDeleteRemoteLogSegment(rsm, metadata1, tp, uuid1);
    }

    @Test
    public void testCopyReadAndDeleteWithoutOptionalFiles() throws Exception {
        Uuid uuid = Uuid.randomUuid();
        RemoteLogSegmentMetadata metadata = verifyUpload(rsm, tp, uuid, 0, 1000, false);
        verifyDeleteRemoteLogSegment(rsm, metadata, tp, uuid);
    }

    @Test
    public void testFetchOnOptionalFile() throws Exception {
        Uuid uuid = Uuid.randomUuid();
        RemoteLogSegmentMetadata metadata = verifyUpload(rsm, tp, uuid, 0, 1000, false);
        verifyGauge(FS_OPEN_OUTPUT_STREAM, 0);
        verifyGauge(FS_OPEN_INPUT_STREAM, 0);
        try (InputStream stream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.OFFSET)) {
            verifyGauge(FS_OPEN_INPUT_STREAM, 1);
            assertNotEquals(0, stream.available());
        }
        verifyGauge(FS_OPEN_INPUT_STREAM, 0);

        try (InputStream stream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.TRANSACTION)) {
            verifyGauge(FS_OPEN_INPUT_STREAM, 1);
            assertEquals(0, stream.available());
        }
        verifyGauge(FS_OPEN_INPUT_STREAM, 0);
    }

    @Test
    public void testFetchLogSegment() throws Exception {
        Uuid uuid = Uuid.randomUuid();
        int segSize = 1000;
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id, 0L, 100L, 0L, 0, 1L, segSize, Collections.singletonMap(0, 0L));
        LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
        rsm.copyLogSegmentData(segmentMetadata, segmentData);

        // start and end position are both inclusive in RSM
        // full fetch segment
        verifyFetchLogSegmentWithPrefetchVariants(rsm, segmentMetadata, segmentData, 0, 999, 1000);
        // fetch intermediate segment
        verifyFetchLogSegmentWithPrefetchVariants(rsm, segmentMetadata, segmentData, 100, 199, 100);
        // fetch till the end of the segment
        verifyFetchLogSegmentWithPrefetchVariants(rsm, segmentMetadata, segmentData, 990, 999, 10);
        // fetch exceeds the segment size
        verifyFetchLogSegmentWithPrefetchVariants(rsm, segmentMetadata, segmentData, 990, 1050, 10);
    }

    @Test
    public void testRepeatedFetchReadsFromCacheOnFullSegmentFetch() throws Exception {
        LRUCacheWithContext cache = new LRUCacheWithContext(10 * 1048576L);
        // two cache hits due to the additional 25 bytes read for the header exceeds the defined cache line size of 2MB.
        testRepeatedCacheReads(cache, String.valueOf(2 * 1048576), 2097152, 1, 1);
    }

    @Test
    public void testRepeatedFetchReadsFromCacheOnSegmentSizeLessThanCacheLine() throws Exception {
        LRUCacheWithContext cache = new LRUCacheWithContext(10 * 1048576L);
        testRepeatedCacheReads(cache, String.valueOf(1048576), 1024, 1, 1);
    }

    @Test
    public void testRepeatedFetchReadsFromCacheWhenSegmentSizeMoreThanCacheLine() throws Exception {
        LRUCacheWithContext cache = new LRUCacheWithContext(10 * 1048576L);
        testRepeatedCacheReads(cache, String.valueOf(1048576), 2097152, 2, 1);
    }

    @Test
    public void testRepeatedFetchReadsFromCacheWhenSegmentSizeMoreThanCacheSize() throws Exception {
        LRUCacheWithContext cache = new LRUCacheWithContext(2097152);
        testRepeatedCacheReads(cache, String.valueOf(1048576), 2 * 2097152, 0, 2);
    }

    @Test
    public void testDeleteOnNonExistentFiles() throws Exception {
        Uuid uuid = Uuid.randomUuid();
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(id,
                0, 100, 0, 0, 1L, 1000,
                Collections.singletonMap(0, 0L));
        String path = baseDir + Path.SEPARATOR + tp.topicId() + Path.SEPARATOR + tp.topicPartition() + Path.SEPARATOR + uuid;
        assertFalse(hdfs.exists(new Path(path)));
        rsm.deleteLogSegmentData(metadata);
        assertFalse(hdfs.exists(new Path(path)));
    }

    @Test
    public void testDeletePartition() throws IOException, RemoteStorageException {
        int segmentSize = 1024;
        int segmentCount = 10;
        int recordsPerSegment = 100;
        List<RemoteLogSegmentMetadata> metadataList = listRemoteLogSegmentMetadata(segmentCount, recordsPerSegment, segmentSize);
        for (RemoteLogSegmentMetadata metadata: metadataList) {
            rsm.copyLogSegmentData(metadata, TestLogSegmentUtils.createLogSegmentData(logDir, metadata.startOffset(), segmentSize, false));
        }
        Path path = new Path(RSMUtils.getPartitionRemoteDir(baseDir, tp));
        assertTrue(hdfs.exists(path));
        assertEquals(segmentCount, hdfs.listStatus(path).length);
        rsm.deletePartition(tp, metadataList);
        assertFalse(hdfs.exists(path));
    }

    @Test
    public void testCacheThrashWhenReadSizeDoesNotMatchWithCacheLineSize() throws Exception {
        // Cache size is 2 MB, cache-line size is 1 MB
        // Segment size is 10 MB
        // Header is 525 bytes
        // Read 1000 KB at once from the stream
        int segSize = 10485760;
        LRUCacheWithContext cache = new LRUCacheWithContext(2097152);
        configs.put(HDFS_REMOTE_READ_BYTES_PROP, "1048576");
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);
            rsm.setLRUCache(cache);
            // Clear previously registered metrics during object creation
            clearKafkaMetrics();
            rsm.registerMetrics(cache);
            rsm.registerHDFSReadMetrics();

            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tp, Uuid.randomUuid()),
                    0, 100, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
            rsm.copyLogSegmentData(segmentMetadata, segmentData);

            RemoteLogSegmentMetadata segmentMetadata1 = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tp, Uuid.randomUuid()),
                    101, 200, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData1 = TestLogSegmentUtils.createLogSegmentData(logDir, 101, segSize, false);
            rsm.copyLogSegmentData(segmentMetadata1, segmentData1);

            assertEquals(0, rsm.segmentFileReadOpenCounter());

            int startPosition = 0;
            int endPosition = 1023999; // read 1000 KB from stream at a time
            while (startPosition < endPosition) {
                // read segment-0
                try (InputStream stream = rsm.fetchLogSegment(segmentMetadata, startPosition, endPosition)) {
                    ByteBuffer buffer = ByteBuffer.wrap(new byte[endPosition - startPosition + 1]);
                    SeekableByteChannel byteChannel = Files.newByteChannel(segmentData.logSegment());
                    byteChannel.position(startPosition);
                    byteChannel.read(buffer);
                    buffer.rewind();
                    assertDataEquals(buffer, stream);
                    byteChannel.close();
                }
                // read segment-1
                try (InputStream stream = rsm.fetchLogSegment(segmentMetadata1, startPosition, endPosition)) {
                    ByteBuffer buffer = ByteBuffer.wrap(new byte[endPosition - startPosition + 1]);
                    SeekableByteChannel byteChannel = Files.newByteChannel(segmentData1.logSegment());
                    byteChannel.position(startPosition);
                    byteChannel.read(buffer);
                    buffer.rewind();
                    assertDataEquals(buffer, stream);
                    byteChannel.close();
                }

                startPosition = endPosition + 1;
                endPosition = Math.min(segSize - 1, endPosition + 1024000);
            }

            assertEquals(21, rsm.segmentFileReadOpenCounter());

            // We fetch 1 MB of data from HDFS and serve 1000 KB back to the consumer. If trashing happens in cache,
            // then, we lose the remaining 24 KB of cached data and re-fetch it again from HDFS.
            Optional<Metric> thrashRequestsPerSec = findKafkaMetric("HDFSCacheThrashRequestPerSec");
            assertTrue(thrashRequestsPerSec.isPresent());
            assertEquals(18, ((Meter) thrashRequestsPerSec.get()).count());
        }
    }

    @Test
    public void testNoCacheThrashWhenReadSizeMatchesWithCacheLineSize() throws Exception {
        // Cache size is 2 MB, cache-line size is 1 MB
        // Segment size is 10 MB
        // Header is 525 bytes
        // Read 1 MB at once from the stream
        int segSize = 10485760;
        LRUCacheWithContext cache = new LRUCacheWithContext(2097152);
        configs.put(HDFS_REMOTE_READ_BYTES_PROP, "1048576");
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);
            rsm.setLRUCache(cache);
            // Clear previously registered metrics during object creation
            clearKafkaMetrics();
            rsm.registerMetrics(cache);
            rsm.registerHDFSReadMetrics();

            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tp, Uuid.randomUuid()),
                    0, 100, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
            rsm.copyLogSegmentData(segmentMetadata, segmentData);

            RemoteLogSegmentMetadata segmentMetadata1 = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tp, Uuid.randomUuid()),
                    101, 200, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData1 = TestLogSegmentUtils.createLogSegmentData(logDir, 101, segSize, false);
            rsm.copyLogSegmentData(segmentMetadata1, segmentData1);

            assertEquals(0, rsm.segmentFileReadOpenCounter());

            int startPosition = 0;
            int endPosition = 1048575; // read 1 MB from stream at a time
            while (startPosition < endPosition) {
                // read segment-0
                try (InputStream stream = rsm.fetchLogSegment(segmentMetadata, startPosition, endPosition)) {
                    ByteBuffer buffer = ByteBuffer.wrap(new byte[endPosition - startPosition + 1]);
                    SeekableByteChannel byteChannel = Files.newByteChannel(segmentData.logSegment());
                    byteChannel.position(startPosition);
                    byteChannel.read(buffer);
                    buffer.rewind();
                    assertDataEquals(buffer, stream);
                    byteChannel.close();
                }
                // read segment-1
                try (InputStream stream = rsm.fetchLogSegment(segmentMetadata1, startPosition, endPosition)) {
                    ByteBuffer buffer = ByteBuffer.wrap(new byte[endPosition - startPosition + 1]);
                    SeekableByteChannel byteChannel = Files.newByteChannel(segmentData1.logSegment());
                    byteChannel.position(startPosition);
                    byteChannel.read(buffer);
                    buffer.rewind();
                    assertDataEquals(buffer, stream);
                    byteChannel.close();
                }

                startPosition = endPosition + 1;
                endPosition = Math.min(segSize - 1, endPosition + 1048576);
            }

            assertEquals(20, rsm.segmentFileReadOpenCounter());

            // Verify yammer metric
            Optional<Metric> thrashRequestsPerSec = findKafkaMetric("HDFSCacheThrashRequestPerSec");
            assertTrue(thrashRequestsPerSec.isPresent());
            assertEquals(0, ((Meter) thrashRequestsPerSec.get()).count());
        }
    }

    @Test
    public void testHedgedReadsMetrics() throws Exception {
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);
            clearKafkaMetrics();
            rsm.registerHedgedReadMetrics();

            // Verify initial values
            verifyGauge(HEDGED_READ_OPS, 0L);
            verifyGauge(HEDGED_READ_OPS_WIN, 0L);
            verifyGauge(READ_THREADPOOL_EXECUTOR_TASK_QUEUE_SIZE, 0);
            verifyGauge(READ_THREADPOOL_EXECUTOR_REJECTION_COUNT, 0L);
            verifyGauge(READ_THREADPOOL_EXECUTOR_AVG_IDLE_PERCENT, 0.0);
            verifyGauge(READ_THREADPOOL_EXECUTOR_CORE_POOL_SIZE, 1);
            verifyGauge(READ_THREADPOOL_EXECUTOR_MAX_POOL_SIZE, 100);
            verifyGauge(READ_THREADPOOL_EXECUTOR_POOL_SIZE, 0);

            // Copy one segment and fetch it via Remote Storage Manager
            Uuid uuid = Uuid.randomUuid();
            RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                    0, 100, 0, 0, 1L, ONE_MB, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, ONE_MB, false);
            rsm.copyLogSegmentData(segmentMetadata, segmentData);
            verifyFetchLogSegmentDefaultPrefetchAndHedgedReads(rsm, segmentMetadata, segmentData, 0, Integer.MAX_VALUE, ONE_MB);

            // Verify the metrics
            verifyGauge(HEDGED_READ_OPS, 0L);
            verifyGauge(HEDGED_READ_OPS_WIN, 0L);
            verifyGauge(READ_THREADPOOL_EXECUTOR_TASK_QUEUE_SIZE, 0);
            verifyGauge(READ_THREADPOOL_EXECUTOR_REJECTION_COUNT, 0L);
            verifyGauge(READ_THREADPOOL_EXECUTOR_AVG_IDLE_PERCENT, 100.0);
            verifyGauge(READ_THREADPOOL_EXECUTOR_CORE_POOL_SIZE, 1);
            verifyGauge(READ_THREADPOOL_EXECUTOR_MAX_POOL_SIZE, 100);
            verifyGauge(READ_THREADPOOL_EXECUTOR_POOL_SIZE, 1);
        }
    }

    @Test
    public void testHDFSCallMetrics() throws Exception {
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);
            clearKafkaMetrics();
            rsm.registerHDFSReadMetrics();
            rsm.registerStreamMetrics();

            // Call once to initialize the default filesystem
            rsm.getFS(defaultFsUri);

            Map<String, String> hdfsTags = Collections.singletonMap(
                    HDFSRemoteStorageManagerMetrics.PROVIDER, RemoteStorageProvider.HDFS.toString());
            Map<String, String> ociTags = Collections.singletonMap(
                    HDFSRemoteStorageManagerMetrics.PROVIDER, RemoteStorageProvider.OCI.toString());

            // Verify initial values
            verifyTimerCount(FS_STATUS_RATE_AND_TIME_MS, 0L);
            verifyTimerCount(SEGMENT_READ_RATE_AND_TIME_MS, hdfsTags, 0L);
            verifyTimerCount(SEGMENT_HEADER_READ_RATE_AND_TIME_MS, hdfsTags, 0L);
            verifyTimerQuantile(FS_STATUS_RATE_AND_TIME_MS, 0.5, value -> value == 0);
            verifyTimerQuantile(SEGMENT_READ_RATE_AND_TIME_MS, hdfsTags, 0.5, value -> value == 0);
            verifyTimerQuantile(SEGMENT_HEADER_READ_RATE_AND_TIME_MS, hdfsTags, 0.5, value -> value == 0);
            verifyGauge(FS_OPEN_OUTPUT_STREAM, 0);
            verifyGauge(FS_OPEN_INPUT_STREAM, 0);
            // oci metrics should be zero
            verifyTimerCount(SEGMENT_READ_RATE_AND_TIME_MS, ociTags, 0L);
            verifyTimerCount(SEGMENT_HEADER_READ_RATE_AND_TIME_MS, ociTags, 0L);
            verifyTimerQuantile(SEGMENT_READ_RATE_AND_TIME_MS, ociTags, 0.5, value -> value == 0);
            verifyTimerQuantile(SEGMENT_HEADER_READ_RATE_AND_TIME_MS, ociTags, 0.5, value -> value == 0);

            // Copy one segment and fetch it via Remote Storage Manager
            Uuid uuid = Uuid.randomUuid();
            RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                    0, 100, 0, 0, 1L, ONE_MB, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, ONE_MB, false);
            rsm.copyLogSegmentData(segmentMetadata, segmentData);
            verifyFetchLogSegmentWithPrefetchVariants(rsm, segmentMetadata, segmentData, 0, Integer.MAX_VALUE, ONE_MB);

            // Verify the metrics
            verifyTimerCount(FS_STATUS_RATE_AND_TIME_MS, 1L);
            verifyTimerCount(SEGMENT_READ_RATE_AND_TIME_MS, hdfsTags, 2L);
            verifyTimerCount(SEGMENT_HEADER_READ_RATE_AND_TIME_MS, hdfsTags, 1L);
            verifyTimerQuantile(FS_STATUS_RATE_AND_TIME_MS, 0.5, value -> value > 0);
            verifyTimerQuantile(SEGMENT_READ_RATE_AND_TIME_MS, hdfsTags, 0.5, value -> value > 0);
            verifyTimerQuantile(SEGMENT_HEADER_READ_RATE_AND_TIME_MS, hdfsTags, 0.5, value -> value > 0);
            verifyGauge(FS_OPEN_OUTPUT_STREAM, 0);
            verifyGauge(FS_OPEN_INPUT_STREAM, 0);
            // oci metrics should be zero
            verifyTimerCount(SEGMENT_READ_RATE_AND_TIME_MS, ociTags, 0L);
            verifyTimerCount(SEGMENT_HEADER_READ_RATE_AND_TIME_MS, ociTags, 0L);
            verifyTimerQuantile(SEGMENT_READ_RATE_AND_TIME_MS, ociTags, 0.5, value -> value == 0);
            verifyTimerQuantile(SEGMENT_HEADER_READ_RATE_AND_TIME_MS, ociTags, 0.5, value -> value == 0);
        }
    }

    @Test
    public void testReconfigurables() {
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            Set<String> reconfigurableConfigs = rsm.reconfigurableConfigs();
            assertEquals(11, reconfigurableConfigs.size());
            assertTrue(reconfigurableConfigs.contains(HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP));
            assertTrue(reconfigurableConfigs.contains(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP));
            assertTrue(reconfigurableConfigs.contains(HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP));
            assertTrue(reconfigurableConfigs.contains(HDFS_OCI_BUCKETS_PROP));
            assertTrue(reconfigurableConfigs.contains(HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP));
            assertTrue(reconfigurableConfigs.contains(HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP));
            assertTrue(reconfigurableConfigs.contains(HDFS_COPY_CIRCUIT_BREAKER_STATE_PROP));
            assertTrue(reconfigurableConfigs.contains(HDFS_DELETE_CIRCUIT_BREAKER_STATE_PROP));
            assertTrue(reconfigurableConfigs.contains(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP));
            assertTrue(reconfigurableConfigs.contains(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP));
            assertTrue(reconfigurableConfigs.contains(OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP));
        }
    }

    @Test
    public void testVerifyConfigUpdateHedgedReadThreshold() throws Exception {
        String hedgedReadThresholdMillis = HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP;
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);

            // Verify the initial configuration
            assertEquals("true", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
            assertEquals("200", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
            // Verify that the property is reconfigurable
            assertTrue(rsm.reconfigurableConfigs().contains(hedgedReadThresholdMillis));

            Map<String, String> newConfigs = new HashMap<>();
            newConfigs.put(hedgedReadThresholdMillis, "50");
            assertThrows(ConfigException.class, () -> rsm.validateReconfiguration(newConfigs));

            newConfigs.put(hedgedReadThresholdMillis, "500");
            assertThrows(ConfigException.class, () -> rsm.validateReconfiguration(newConfigs));

            newConfigs.put(hedgedReadThresholdMillis, "100");
            assertDoesNotThrow(() -> rsm.validateReconfiguration(newConfigs));

            newConfigs.put(hedgedReadThresholdMillis, "400");
            assertDoesNotThrow(() -> rsm.validateReconfiguration(newConfigs));
        }
    }

    @Test
    public void testVerifyConfigUpdateReadThreadPoolCoreSize() throws Exception {
        String readThreadpoolCoreSize = HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP;
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            // Set initial core size as 5
            configs.put(readThreadpoolCoreSize, "5");
            rsm.configure(configs);

            // Verify the initial configuration
            assertEquals("true", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
            assertEquals("5", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY));
            // Verify that the property is reconfigurable
            assertTrue(rsm.reconfigurableConfigs().contains(readThreadpoolCoreSize));

            Map<String, String> newConfigs = new HashMap<>();
            newConfigs.put(readThreadpoolCoreSize, "20");
            assertThrows(ConfigException.class, () -> rsm.validateReconfiguration(newConfigs));

            newConfigs.put(readThreadpoolCoreSize, "1");
            assertThrows(ConfigException.class, () -> rsm.validateReconfiguration(newConfigs));

            newConfigs.put(readThreadpoolCoreSize, "10");
            assertDoesNotThrow(() -> rsm.validateReconfiguration(newConfigs));

            newConfigs.put(readThreadpoolCoreSize, "3");
            assertDoesNotThrow(() -> rsm.validateReconfiguration(newConfigs));
        }
    }

    @Test
    public void testVerifyConfigUpdateReadThreadPoolMaxSize() throws Exception {
        String readThreadpoolMaxSize = HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP;
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            // Set initial max size as 50
            configs.put(readThreadpoolMaxSize, "50");
            rsm.configure(configs);

            // Verify the initial configuration
            assertEquals("true", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
            assertEquals("50", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY));
            // Verify that the property is reconfigurable
            assertTrue(rsm.reconfigurableConfigs().contains(readThreadpoolMaxSize));

            Map<String, String> newConfigs = new HashMap<>();
            newConfigs.put(readThreadpoolMaxSize, "20");
            assertThrows(ConfigException.class, () -> rsm.validateReconfiguration(newConfigs));

            newConfigs.put(readThreadpoolMaxSize, "150");
            assertThrows(ConfigException.class, () -> rsm.validateReconfiguration(newConfigs));

            newConfigs.put(readThreadpoolMaxSize, "25");
            assertDoesNotThrow(() -> rsm.validateReconfiguration(newConfigs));

            newConfigs.put(readThreadpoolMaxSize, "100");
            assertDoesNotThrow(() -> rsm.validateReconfiguration(newConfigs));
        }
    }

    @Test
    public void testReconfigure() {
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);

            // Verify the initial configuration
            assertEquals("true", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
            assertEquals("200", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
            assertEquals("1", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY));
            assertEquals("100", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY));
            assertEquals(DEFAULT_HDFS_READ_ERROR_BACKOFF_WAIT_MS, rsm.fetchErrorBackoffWaitMs());
            assertEquals(DEFAULT_HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS, rsm.errorMaxBackoffWaitMs());

            // Reconfigure with new threshold
            Map<String, String> configs = new HashMap<>();
            configs.put(HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP, "100");
            configs.put(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP, "5");
            configs.put(HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP, "200");
            configs.put(HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP, "10");
            configs.put(HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP, "100");

            // Verify that all the configs are reconfigurable
            configs.keySet().forEach(key -> assertTrue(rsm.reconfigurableConfigs().contains(key)));

            // Reconfigure and verify the new configurations
            rsm.reconfigure(configs);
            rsm.fileSystemManager().handleDynamicHedgedReadsConfigUpdates();
            assertEquals("100", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
            assertEquals("5", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY));
            assertEquals("200", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY));
            assertEquals(10L, rsm.fetchErrorBackoffWaitMs());
            assertEquals(100L, rsm.errorMaxBackoffWaitMs());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"abcd", "invalid-uri", "oci://uber-staging@abcd,hdfs://abcd", ""})
    public void testOCIBucketsReconfigureValidation(String updatedOciBuckets) {
        Map<String, String> configs = new HashMap<>();
        configs.put(HDFS_OCI_BUCKETS_PROP, updatedOciBuckets);
        try {
            rsm.validateReconfiguration(configs);
            fail("should have failed for invalid OCI buckets: " + updatedOciBuckets);
        } catch (IllegalArgumentException | ConfigException e) {
            // expected exception for invalid OCI buckets
        }
    }

    @Test
    public void testBackOffConfigValidation() {
        assertThrows(ConfigException.class, () -> rsm.validateReconfiguration(Collections.singletonMap(HDFS_READ_ERROR_BACKOFF_WAIT_MS_PROP, "-1")));
        assertThrows(ConfigException.class, () -> rsm.validateReconfiguration(Collections.singletonMap(HDFS_READ_ERROR_MAX_BACKOFF_WAIT_MS_PROP, "-1")));
    }

    @Test
    public void testGetPartitionRemoteDir() {
        RemoteLogSegmentId segmentId = generateRemoteLogSegmentId();
        String partitionRemoteDir = RSMUtils.getPartitionRemoteDir(baseDir, segmentId.topicIdPartition());
        assertEquals("/user/kloak/kafka-remote-logs/test-0-hHJfD_slRkGCrDPSvJsMtA", partitionRemoteDir);
    }

    @Test
    public void testGetSegmentRemoteDir() {
        RemoteLogSegmentId segmentId = generateRemoteLogSegmentId();
        String segmentRemoteDir = RSMUtils.getSegmentRemoteDir(baseDir, segmentId);
        assertEquals("/user/kloak/kafka-remote-logs/test-0-hHJfD_slRkGCrDPSvJsMtA/pQpAc9OvTGaxywm8JnN9IQ", segmentRemoteDir);
    }


    /**
     * The test asserts that the SimpleInputStream impl is able to read the data from the stream beyond the
     * cacheLineSize or hdfs.remote.read.bytes.
     */
    @Test
    public void testFetchLogSegmentWithSegmentSizeExceedCacheLineSize() throws Exception {
        rsm.close();
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            configs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP, String.valueOf(ONE_MB));
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);
            clearKafkaMetrics();

            Uuid uuid = Uuid.randomUuid();
            int segSize = 2 * ONE_MB;
            RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id, 0L, 100L, 0L, 0, 1L, segSize, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
            rsm.copyLogSegmentData(segmentMetadata, segmentData);

            // start and end position are both inclusive in RSM
            // full fetch segment
            verifyFetchLogSegmentWithPrefetchVariants(rsm, segmentMetadata, segmentData, 0, 999, 1000);
            // fetch intermediate segment
            verifyFetchLogSegmentWithPrefetchVariants(rsm, segmentMetadata, segmentData, 100, 199, 100);
            // fetch till the end of the segment
            verifyFetchLogSegmentWithPrefetchVariants(rsm, segmentMetadata, segmentData, 990, segSize, 2096162);
            // fetch exceeds the segment size
            verifyFetchLogSegmentWithPrefetchVariants(rsm, segmentMetadata, segmentData, 990, segSize + 10, 2096162);
        }
    }

    @Test
    public void testOpenRemoteOutputStreamCountOnException() throws IOException {
        clearKafkaMetrics();
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
             MockedStatic<FileSystem> mockedFileSystem = Mockito.mockStatic(FileSystem.class)) {
            FileSystem spyFileSystem = spy(hdfs);
            mockedFileSystem.when(() -> FileSystem.get(any(URI.class), any(Configuration.class)))
                    .thenReturn(spyFileSystem);
            doThrow(new IOException("Test exception")).when(spyFileSystem).create(any());
            rsm.configure(configs);
            rsm.setTime(time);
            rsm.registerStreamMetrics();

            RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tp, Uuid.randomUuid());
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId,
                    0, 100, 0, 0, 1L, 1024, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData = TestLogSegmentUtils
                    .createLogSegmentData(logDir, 0, 1024, false);
            assertThrows(RemoteStorageException.class, () -> rsm.copyLogSegmentData(segmentMetadata, segmentData));
            verifyGauge(FS_OPEN_OUTPUT_STREAM, 0);
        }
    }

    @Test
    public void testPathContainSchemeAndAuthority() throws Exception {
        clearKafkaMetrics();
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
             MockedStatic<FileSystem> mockedFileSystem = Mockito.mockStatic(FileSystem.class)) {
            FileSystem spyFileSystem = spy(hdfs);
            mockedFileSystem.when(() -> FileSystem.get(any(URI.class), any(Configuration.class)))
                    .thenReturn(spyFileSystem);

            rsm.configure(configs);
            rsm.setTime(time);
            rsm.registerStreamMetrics();
            ArgumentCaptor<Path> pathArgCaptor = ArgumentCaptor.forClass(Path.class);
            Mockito.doCallRealMethod().when(spyFileSystem).exists(pathArgCaptor.capture());
            Mockito.doCallRealMethod().when(spyFileSystem).create(pathArgCaptor.capture());
            Mockito.doCallRealMethod().when(spyFileSystem).open(pathArgCaptor.capture());
            Mockito.doCallRealMethod().when(spyFileSystem).delete(pathArgCaptor.capture(), anyBoolean());

            Uuid uuid = Uuid.randomUuid();
            RemoteLogSegmentMetadata metadata = verifyUpload(rsm, tp, uuid, 0, 1000, true);
            verifyDeleteRemoteLogSegment(rsm, metadata, tp, uuid);

            List<Path> capturedPaths = pathArgCaptor.getAllValues();
            assertFalse(capturedPaths.isEmpty());
            String remotePartitionDir = defaultFsUri + "/user/kloak/kafka-remote-logs/" + tp.topicPartition() + "-" + tp.topicId();
            String segmentPath = remotePartitionDir + "/" + uuid;
            for (Path path : capturedPaths) {
                assertEquals(segmentPath, path.toString());
            }
            rsm.deletePartition(tp, Collections.singletonList(metadata));
            assertEquals(remotePartitionDir, pathArgCaptor.getValue().toString());

            verify(spyFileSystem, atLeastOnce()).exists(any());
            verify(spyFileSystem, atLeastOnce()).create(any());
            verify(spyFileSystem, atLeastOnce()).open(any());
            verify(spyFileSystem, atLeastOnce()).delete(any(), anyBoolean());
        }
    }

    @Test
    public void testCopyLogSegmentDataMetrics() throws IOException, RemoteStorageException {
        clearKafkaMetrics();
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);

            // Clear previously registered metrics during object creation
            clearKafkaMetrics();
            rsm.registerHDFSReadMetrics();

            Map<String, String> hdfsTags = Collections.singletonMap(
                    HDFSRemoteStorageManagerMetrics.PROVIDER, RemoteStorageProvider.HDFS.toString());
            Map<String, String> ociTags = Collections.singletonMap(
                    HDFSRemoteStorageManagerMetrics.PROVIDER, RemoteStorageProvider.OCI.toString());

            // Verify initial metrics are zero
            verifyTimerCount(SEGMENT_WRITE_RATE_AND_TIME_MS, hdfsTags, 0L);
            verifyTimerQuantile(SEGMENT_WRITE_RATE_AND_TIME_MS, hdfsTags, 0.5, value -> value == 0);
            verifyMeter(SEGMENT_WRITE_BYTES_PER_SEC, hdfsTags, 0L);
            verifyTimerCount(SEGMENT_WRITE_RATE_AND_TIME_MS, ociTags, 0L);
            verifyTimerQuantile(SEGMENT_WRITE_RATE_AND_TIME_MS, ociTags, 0.5, value -> value == 0);
            verifyMeter(SEGMENT_WRITE_BYTES_PER_SEC, ociTags, 0L);

            Uuid uuid = Uuid.randomUuid();
            RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                    0, 100, 0, 0, 1L, 1024, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, 1024, false);
            rsm.copyLogSegmentData(segmentMetadata, segmentData);

            // Verify the hdfs metrics
            verifyTimerCount(SEGMENT_WRITE_RATE_AND_TIME_MS, hdfsTags, 1L);
            verifyTimerQuantile(SEGMENT_WRITE_RATE_AND_TIME_MS, hdfsTags, 0.5, value -> value > 0);
            verifyMeter(SEGMENT_WRITE_BYTES_PER_SEC, hdfsTags, 1024L);

            // verify oci metrics are zero
            verifyTimerCount(SEGMENT_WRITE_RATE_AND_TIME_MS, ociTags, 0L);
            verifyTimerQuantile(SEGMENT_WRITE_RATE_AND_TIME_MS, ociTags, 0.5, value -> value == 0);
            verifyMeter(SEGMENT_WRITE_BYTES_PER_SEC, ociTags, 0L);
        }
    }

    @Test
    public void shouldThrowRetriableExceptionWhenCopyCircuitIsOpen() throws Exception {
        clearKafkaMetrics();
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
             MockedStatic<FileSystem> mockedFileSystem = Mockito.mockStatic(FileSystem.class)) {
            FileSystem spyFileSystem = spy(hdfs);
            mockedFileSystem.when(() -> FileSystem.get(any(URI.class), any(Configuration.class)))
                    .thenReturn(spyFileSystem);
            doThrow(new IOException("Test exception")).when(spyFileSystem).create(any());
            rsm.configure(configs);
            rsm.setTime(time);
            rsm.registerStreamMetrics();

            RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tp, Uuid.randomUuid());
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId,
                    0, 100, 0, 0, 1L, 1024, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData = TestLogSegmentUtils
                    .createLogSegmentData(logDir, 0, 1024, false);
            for (int i = 0; i < 150; i++) {
                // circuit breaker should be open after 100 error calls, further calls should not hit the OCI.
                assertThrows(RemoteStorageException.class, () -> rsm.copyLogSegmentData(segmentMetadata, segmentData));
            }
            try {
                rsm.copyLogSegmentData(segmentMetadata, segmentData);
                fail("Should have thrown RetriableRemoteStorageException");
            } catch (RemoteStorageException ex) {
                assertInstanceOf(RetriableRemoteStorageException.class, ex);
            }
            verify(spyFileSystem, times(100)).create(any());
            rsm.copyErrorBreaker().reset();
            assertThrows(RemoteStorageException.class, () -> rsm.copyLogSegmentData(segmentMetadata, segmentData));
            verify(spyFileSystem, times(101)).create(any());
            verifyGauge(FS_OPEN_OUTPUT_STREAM, 0);
        }
    }

    @Test
    public void shouldThrowRetriableExceptionWhenDeletionCircuitIsOpen() throws Exception {
        clearKafkaMetrics();
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
             MockedStatic<FileSystem> mockedFileSystem = Mockito.mockStatic(FileSystem.class)) {
            FileSystem spyFileSystem = spy(hdfs);
            mockedFileSystem.when(() -> FileSystem.get(any(URI.class), any(Configuration.class)))
                    .thenReturn(spyFileSystem);
            doReturn(true).when(spyFileSystem).exists(any());
            doThrow(new IOException("Test exception")).when(spyFileSystem).delete(any(), anyBoolean());
            rsm.configure(configs);
            rsm.setTime(time);

            RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tp, Uuid.randomUuid());
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId,
                    0, 100, 0, 0, 1L, 1024, Collections.singletonMap(0, 0L));
            for (int i = 0; i < 150; i++) {
                // circuit breaker should be open after 100 error calls, further calls should not hit the OCI.
                assertThrows(RemoteStorageException.class, () -> rsm.deleteLogSegmentData(segmentMetadata));
            }
            try {
                rsm.deleteLogSegmentData(segmentMetadata);
                fail("Should have thrown RetriableRemoteStorageException");
            } catch (RemoteStorageException ex) {
                assertInstanceOf(RetriableRemoteStorageException.class, ex);
            }
            verify(spyFileSystem, times(100)).delete(any(), anyBoolean());
            rsm.deleteErrorBreaker().reset();
            assertThrows(RemoteStorageException.class, () -> rsm.deleteLogSegmentData(segmentMetadata));
            verify(spyFileSystem, times(101)).delete(any(), anyBoolean());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            HDFS_COPY_CIRCUIT_BREAKER_STATE_PROP,
            HDFS_DELETE_CIRCUIT_BREAKER_STATE_PROP,
    })
    public void testInvalidCircuitBreakerState(String circuitBreakerProp) {
        Map<String, String> updatedConfigs = new HashMap<>(configs);
        updatedConfigs.put(circuitBreakerProp, "xyz");
        assertThrows(ConfigException.class, () -> new HDFSRemoteStorageManagerConfig(updatedConfigs, false));
        ConfigException ex = assertThrows(ConfigException.class, () -> rsm.validateReconfiguration(updatedConfigs));
        assertEquals("Invalid value xyz for configuration " + circuitBreakerProp +
                ": Valid values are: [FORCED_OPEN, CLOSED, DISABLED]", ex.getMessage());
    }

    @Test
    public void testCircuitBreakerStateChange() {
        assertEquals(CircuitBreaker.State.CLOSED, rsm.copyErrorBreaker().getState());
        assertEquals(CircuitBreaker.State.CLOSED, rsm.deleteErrorBreaker().getState());
        for (String state1 : HDFSRemoteStorageManagerConfig.ALLOWED_CIRCUIT_BREAKER_VALUES) {
            verifyCircuitBreakerState(state1);
            for (String state2 : HDFSRemoteStorageManagerConfig.ALLOWED_CIRCUIT_BREAKER_VALUES) {
                verifyCircuitBreakerState(state2);
            }
        }
    }

    private void verifyCircuitBreakerState(String state) {
        Map<String, String> updatedConfigs = new HashMap<>();
        updatedConfigs.put(HDFS_COPY_CIRCUIT_BREAKER_STATE_PROP, state);
        updatedConfigs.put(HDFS_DELETE_CIRCUIT_BREAKER_STATE_PROP, state);
        rsm.reconfigureCircuitBreakerState(updatedConfigs);
        CircuitBreaker.State expectedBreakerState = CircuitBreaker.State.valueOf(state);
        assertEquals(expectedBreakerState, rsm.copyErrorBreaker().getState());
        assertEquals(expectedBreakerState, rsm.deleteErrorBreaker().getState());
    }

    private RemoteLogSegmentId generateRemoteLogSegmentId() {
        Uuid segmentId = Uuid.fromString("pQpAc9OvTGaxywm8JnN9IQ");
        Uuid topicId = Uuid.fromString("hHJfD_slRkGCrDPSvJsMtA");
        TopicPartition tp = new TopicPartition("test", 0);
        TopicIdPartition tpId = new TopicIdPartition(topicId, tp);
        return new RemoteLogSegmentId(tpId, segmentId);
    }

    private void verifyTimerCount(String name, long expectedValue) {
        RSMTestUtils.verifyTimerCount(HDFSRemoteStorageManager.class, name, expectedValue);
    }

    private void verifyTimerCount(String name, Map<String, String> tags, long expectedValue) {
        RSMTestUtils.verifyTimerCount(HDFSRemoteStorageManager.class, name, tags, expectedValue);
    }

    private void verifyTimerQuantile(String name, double quantile, Predicate<Double> assertion) {
        RSMTestUtils.verifyTimerQuantile(HDFSRemoteStorageManager.class, name, Collections.emptyMap(), quantile, assertion);
    }

    private void verifyTimerQuantile(String name, Map<String, String> tags, double quantile, Predicate<Double> assertion) {
        RSMTestUtils.verifyTimerQuantile(HDFSRemoteStorageManager.class, name, tags, quantile, assertion);
    }

    private void verifyMeter(String name, Map<String, String> tags, long expectedCount) {
        RSMTestUtils.verifyMeter(HDFSRemoteStorageManager.class, name, tags, expectedCount);
    }

    private <T> void verifyGauge(String name, T expectedValue) {
        RSMTestUtils.verifyGauge(HDFSRemoteStorageManager.class, name, expectedValue);
    }

    private List<RemoteLogSegmentMetadata> listRemoteLogSegmentMetadata(int segmentCount,
                                                                        int recordsPerSegment,
                                                                        int segmentSize) {
        final List<RemoteLogSegmentMetadata> metadataList = new ArrayList<>();
        for (int idx = 0; idx < segmentCount; idx++) {
            long timestamp = time.milliseconds();
            long startOffset = (long) idx * recordsPerSegment;
            long endOffset = startOffset + recordsPerSegment - 1;
            Map<Integer, Long> segmentLeaderEpochs = Collections.singletonMap(0, 0L);
            RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tp, Uuid.randomUuid());
            RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(segmentId, startOffset, endOffset,
                    timestamp, 0, timestamp, segmentSize, Optional.empty(),
                    RemoteLogSegmentState.DELETE_SEGMENT_STARTED, segmentLeaderEpochs);
            metadataList.add(metadata);
        }
        return metadataList;
    }

    private void testRepeatedCacheReads(LRUCacheWithContext cache,
                                        String cacheLineSizeInBytes,
                                        int segSize,
                                        int expectedCacheHit,
                                        int expectedSegmentReadFileOpenCalls) throws Exception {
        configs.put(HDFS_REMOTE_READ_BYTES_PROP, cacheLineSizeInBytes);
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);
            rsm.setLRUCache(cache);
            // Clear previously registered metrics during object creation
            clearKafkaMetrics();
            rsm.registerMetrics(cache);
            rsm.registerHDFSReadMetrics();

            Uuid uuid = Uuid.randomUuid();
            RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                    0, 100, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
            rsm.copyLogSegmentData(segmentMetadata, segmentData);

            assertEquals(0, cache.getCacheHit());
            assertEquals(0, rsm.segmentFileReadOpenCounter());
            verifyFetchLogSegmentDefaultPrefetch(rsm, segmentMetadata, segmentData, 0, Integer.MAX_VALUE, segSize);
            assertEquals(0, cache.getCacheHit());

            // read from cache
            verifyFetchLogSegmentDefaultPrefetch(rsm, segmentMetadata, segmentData, 0, Integer.MAX_VALUE, segSize);
            assertEquals(expectedCacheHit, cache.getCacheHit());
            assertEquals(expectedSegmentReadFileOpenCalls, rsm.segmentFileReadOpenCounter());

            // Verify yammer metric
            Optional<Metric> hitCount = findKafkaMetric("hitCount");
            assertTrue(hitCount.isPresent());
            assertEquals(expectedCacheHit, ((Long) ((Gauge<?>) hitCount.get()).value()).intValue());
        }
    }

    private Optional<Metric> findKafkaMetric(String name) {
        return RSMTestUtils.findKafkaMetric(HDFSRemoteStorageManager.class, name);
    }

    private void verifyFetchLogSegmentDefaultPrefetch(RemoteStorageManager rsm,
                                                      RemoteLogSegmentMetadata metadata,
                                                      LogSegmentData segmentData,
                                                      int startPosition,
                                                      int endPosition,
                                                      int size) throws Exception {
        RemoteReadContext readContext = RemoteReadContext.builder()
                .withBlockPrefetchEnabled(true)
                .build();
        verifyFetchLogSegmentInternal(rsm, metadata, segmentData, readContext, startPosition, endPosition, size);
    }

    private void verifyFetchLogSegmentDefaultPrefetchAndHedgedReads(RemoteStorageManager rsm,
                                                                    RemoteLogSegmentMetadata metadata,
                                                                    LogSegmentData segmentData,
                                                                    int startPosition,
                                                                    int endPosition,
                                                                    int size) throws Exception {
        RemoteReadContext readContext = RemoteReadContext.builder()
                .withBlockPrefetchEnabled(true)
                .withHedgedReadsEnabled(true)
                .withNextSegmentOffsetAndEpoch(null)
                .withSegmentPrefetchEnabled(false)
                .build();
        verifyFetchLogSegmentInternal(rsm, metadata, segmentData, readContext, startPosition, endPosition, size);
    }

    private void verifyFetchLogSegmentWithPrefetchVariants(RemoteStorageManager rsm,
                                                           RemoteLogSegmentMetadata metadata,
                                                           LogSegmentData segmentData,
                                                           int startPosition,
                                                           int endPosition,
                                                           int size) throws Exception {
        for (boolean enablePrefetch : Arrays.asList(true, false)) {
            RemoteReadContext readContext = RemoteReadContext.builder()
                    .withBlockPrefetchEnabled(enablePrefetch)
                    .withHedgedReadsEnabled(false)
                    .withNextSegmentOffsetAndEpoch(null)
                    .withSegmentPrefetchEnabled(false)
                    .build();
            verifyFetchLogSegmentInternal(rsm, metadata, segmentData, readContext, startPosition, endPosition, size);
        }
    }

    /**
     * Verifies the log segment fetch.
     * @param rsm           remote storage manager
     * @param metadata      metadata about the remote log segment.
     * @param segmentData   segment data.
     * @param readContext   read context.
     * @param startPosition start position to fetch from the segment, inclusive
     * @param endPosition   Fetch data till the end position, inclusive
     * @throws Exception I/O Error, file not found exception.
     */
    private void verifyFetchLogSegmentInternal(RemoteStorageManager rsm,
                                               RemoteLogSegmentMetadata metadata,
                                               LogSegmentData segmentData,
                                               RemoteReadContext readContext,
                                               int startPosition,
                                               int endPosition,
                                               int size) throws Exception {
        try (InputStream stream = rsm.fetchLogSegment(metadata, readContext, startPosition, endPosition)) {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[size]);
            SeekableByteChannel byteChannel = Files.newByteChannel(segmentData.logSegment());
            byteChannel.position(startPosition);
            byteChannel.read(buffer);
            buffer.rewind();
            assertDataEquals(buffer, stream);
            byteChannel.close();
            if (readContext.isHedgedReadsEnabled()) {
                SafeInputStream safeInputStream = (SafeInputStream) stream;
                assertTrue(safeInputStream.delegate() instanceof HDFSRemoteStorageManager.CachedInputStream);
            }
        }
    }

    private RemoteLogSegmentMetadata verifyUpload(RemoteStorageManager rsm,
                                                  TopicIdPartition tp,
                                                  Uuid uuid,
                                                  int startOffset,
                                                  int segSize,
                                                  boolean withOptionalFiles) throws Exception {
        long bytesReadFromRemoteSoFar = ((HDFSRemoteStorageManager) rsm).bytesReadFromRemote();
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId,
                0, 100, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
        LogSegmentData segmentData = TestLogSegmentUtils
                .createLogSegmentData(logDir, startOffset, segSize, withOptionalFiles);
        Optional<RemoteLogSegmentMetadata.CustomMetadata> customMetadataOpt = rsm.copyLogSegmentData(segmentMetadata, segmentData);
        checkFileExistence(uuid);
        checkAssociatedFileContents(rsm, segmentMetadata, segmentData);
        assertTrue(hdfs.exists(new Path(RSMUtils.getPartitionRemoteDir(baseDir, tp) + Path.SEPARATOR + uuid)));
        long expectedBytesRead = TestLogSegmentUtils.OFFSET_INDEX_FILE_SIZE + TestLogSegmentUtils.TIME_INDEX_FILE_SIZE +
                TestLogSegmentUtils.LEADER_EPOCH_INDEX_FILE_SIZE + TestLogSegmentUtils.PRODUCER_SNAPSHOT_FILE_SIZE;
        long expectedBytesReadWithTxnIndex = expectedBytesRead + TestLogSegmentUtils.TXN_INDEX_FILE_SIZE;
        if (withOptionalFiles) {
            assertEquals(expectedBytesReadWithTxnIndex, ((HDFSRemoteStorageManager) rsm).bytesReadFromRemote() - bytesReadFromRemoteSoFar);
        } else {
            assertEquals(expectedBytesRead, ((HDFSRemoteStorageManager) rsm).bytesReadFromRemote() - bytesReadFromRemoteSoFar);
        }
        assertTrue(customMetadataOpt.isPresent());
        assertEquals(defaultFsUri, FileSystemManager.getBucket(customMetadataOpt.get()));
        return segmentMetadata;
    }

    private void verifyDeleteRemoteLogSegment(RemoteStorageManager rsm,
                                              RemoteLogSegmentMetadata metadata,
                                              TopicIdPartition tp,
                                              Uuid uuid) throws RemoteStorageException, IOException {
        rsm.deleteLogSegmentData(metadata);
        assertFalse(hdfs.exists(new Path(baseDir + Path.SEPARATOR + tp.topicId() + Path.SEPARATOR +
                tp.topicPartition() + Path.SEPARATOR + uuid)));

        long beforeMs = time.milliseconds();
        RemoteStorageException ex = assertThrows(RemoteStorageException.class, () ->
                rsm.fetchLogSegment(metadata, 0));
        assertTrue(ex.getMessage().contains("Failed to fetch SEGMENT file from remote storage. Metadata: " + metadata));
        assertTrue(time.milliseconds() > beforeMs);

        beforeMs = time.milliseconds();
        ex = assertThrows(RemoteStorageException.class, () ->
                rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.OFFSET));
        assertTrue(ex.getMessage().contains("Failed to fetch OFFSET_INDEX file from remote storage. Metadata: " + metadata));
        assertTrue(time.milliseconds() > beforeMs);
    }

    private void checkFileExistence(Uuid uuid) throws IOException {
        Path path = new Path(RSMUtils.getPartitionRemoteDir(baseDir, tp));
        assertTrue(hdfs.exists(path));

        Path filePath = new Path(path, uuid.toString());
        assertTrue(hdfs.exists(filePath));

        int count = 0;
        RemoteIterator<LocatedFileStatus> iter = hdfs.listFiles(filePath, true);
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        assertEquals(1, count);
    }

    private void checkAssociatedFileContents(RemoteStorageManager rsm,
                                             RemoteLogSegmentMetadata metadata,
                                             LogSegmentData segmentData) throws Exception {
        // Fetch the files in random order
        try (InputStream actualStream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.TIMESTAMP)) {
            assertFileEquals(segmentData.timeIndex().toFile(), actualStream);
        }
        try (InputStream actualStream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.OFFSET)) {
            assertFileEquals(segmentData.offsetIndex().toFile(), actualStream);
        }
        if (segmentData.transactionIndex().isPresent()) {
            try (InputStream actualStream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.TRANSACTION)) {
                assertFileEquals(segmentData.transactionIndex().get().toFile(), actualStream);
            }
        }
        try (InputStream actualStream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.LEADER_EPOCH)) {
            ByteBuffer leaderEpochIndex = segmentData.leaderEpochIndex();
            leaderEpochIndex.rewind();
            assertDataEquals(leaderEpochIndex, actualStream);
        }
        if (segmentData.producerSnapshotIndex() != null) {
            try (InputStream actualStream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT)) {
                assertFileEquals(segmentData.producerSnapshotIndex().toFile(), actualStream);
            }
        }
        // Fetch the segment with and without LRU cache.
        for (boolean enablePrefetch : Arrays.asList(true, false)) {
            RemoteReadContext readContext = RemoteReadContext.builder()
                    .withBlockPrefetchEnabled(enablePrefetch)
                    .build();
            try (InputStream actualStream = rsm.fetchLogSegment(metadata, readContext, 0)) {
                assertFileEquals(segmentData.logSegment().toFile(), actualStream);
            }
        }
    }

    private void assertFileEquals(File expected, InputStream actual) throws Exception {
        ByteBuffer expectedByteBuffer;
        try (InputStream inputStream = Files.newInputStream(expected.toPath())) {
            expectedByteBuffer = read(inputStream);
            assertEquals(expected.length(), expectedByteBuffer.limit());
        }
        ByteBuffer actualByteBuffer = read(actual);
        assertEquals(expected.length(), actualByteBuffer.limit());
        assertEquals(expectedByteBuffer, actualByteBuffer);
        assertArrayEquals(expectedByteBuffer.array(), actualByteBuffer.array());
    }

    private ByteBuffer read(InputStream inputStream) throws IOException {
        // ByteBuffer expands internally
        try (ByteBufferOutputStream out = new ByteBufferOutputStream(8)) {
            byte[] bytes = new byte[8];
            int nRead;
            while ((nRead = inputStream.read(bytes, 0, bytes.length)) != -1) {
                out.write(bytes, 0, nRead);
            }
            out.buffer().flip();
            return out.buffer();
        }
    }

    private void assertDataEquals(ByteBuffer expectedBuffer, InputStream actual) throws Exception {
        ByteBuffer actualBuffer = ByteBuffer.wrap(new byte[actual.available()]);
        int read = actual.read(actualBuffer.array());
        assertEquals(expectedBuffer.limit(), read);
        assertEquals(expectedBuffer, actualBuffer);
    }

    private static class LRUCacheWithContext extends LRUCache {
        private int cacheHit = 0;

        private LRUCacheWithContext(long maxBytes) {
            super(maxBytes);
        }

        synchronized ByteBufferWrapper get(String path, long offset) {
            ByteBufferWrapper result = super.get(path, offset);
            if (result != null) {
                cacheHit++;
            }
            return result;
        }

        public int getCacheHit() {
            return cacheHit;
        }
    }
}
