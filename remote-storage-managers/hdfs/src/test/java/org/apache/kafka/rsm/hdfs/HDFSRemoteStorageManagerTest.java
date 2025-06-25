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
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteReadContext;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteStorageProvider;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DEFAULT_FS_URI_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_KEYTAB_PATH_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_OCI_BUCKETS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_USER_PROP;
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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
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
        Path path = new Path(HDFSRemoteStorageManager.getPartitionRemoteDir(baseDir, tp));
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
    public void testSetHedgedReadsConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, "/tmp");
        HDFSRemoteStorageManagerConfig remoteStorageManagerConfig = new HDFSRemoteStorageManagerConfig(props, false);

        Configuration config = new Configuration();
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setHedgedReadsConfiguration(config, remoteStorageManagerConfig);

            assertEquals("true", config.get(HdfsClientConfigKeys.HedgedRead.ENABLED));
            assertEquals("200", config.get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
            assertEquals("1", config.get(HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY));
            assertEquals("100", config.get(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY));
            assertEquals("60", config.get(HdfsClientConfigKeys.ReadThreadPool.KEEP_ALIVE_TIME_KEY));
            assertEquals("true", config.get(HdfsClientConfigKeys.ReadThreadPool.ALLOW_CORE_THREAD_TIMEOUT_KEY));
        }
    }

    @Test
    public void testGetFileSystemWithHedgedReads() throws IOException {
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);

            // Verify hedged reads is disabled by default
            assertNull(rsm.getFS(defaultFsUri).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));

            // Verify the configuration of the returned FileSystem when hedged reads is enabled
            assertEquals("true", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));

            // Verify the configuration of returned FileSystem when hedged reads is disabled
            assertNull(rsm.getFS(defaultFsUri, false).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
        }
    }

    @Test
    public void testUpdateHedgedReadsThreshold() throws IOException {
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);

            // Verify hedged reads is enabled in the FileSystem configuration
            FileSystem originalFS = rsm.getFS(defaultFsUri, true);
            assertEquals("true", originalFS.getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
            assertEquals("200", originalFS.getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));

            // Without updates, the returned FileSystem should be the same as the original one
            FileSystem fsPreUpdate = rsm.getFS(defaultFsUri, true);
            assertEquals("true", fsPreUpdate.getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
            assertEquals("200", fsPreUpdate.getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
            assertSame(originalFS, fsPreUpdate);

            // Update the hedged reads threshold and verify the returned FileSystem has the updated configuration
            rsm.setHedgedReadThresholdMillis(100);
            rsm.handleDynamicHedgedReadsConfigUpdates();
            FileSystem fsPostUpdate = rsm.getFS(defaultFsUri, true);
            assertEquals("true", fsPostUpdate.getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
            assertEquals("100", fsPostUpdate.getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
            assertNotSame(originalFS, fsPostUpdate);

            // Verify updates to the configuration does not affect the returned FileSystem when hedged reads is disabled
            rsm.setHedgedReadThresholdMillis(500);
            rsm.handleDynamicHedgedReadsConfigUpdates();
            assertNull(rsm.getFS(defaultFsUri, false).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));

            // Verify the previous update takes affect for the filesystem with hedged reads enabled
            assertEquals("true", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
            assertEquals("500", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
        }
    }

    @Test
    public void testUpdateReadThreadPoolCoreSize() throws IOException {
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);

            // Verify the read thread pool core size before the update
            assertEquals(DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE,
                    ((DistributedFileSystem) rsm.getFS(defaultFsUri, true)).getDFSClientReaderThreadPoolSize());

            // Update the read thread pool core size and verify the returned FileSystem has the updated configuration
            int newCoreSize = DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE + 1;
            rsm.setReadThreadPoolCoreSize(newCoreSize);
            rsm.handleDynamicHedgedReadsConfigUpdates();
            assertEquals(newCoreSize, ((DistributedFileSystem) rsm.getFS(defaultFsUri, true)).getDFSClientReaderThreadPoolSize());
        }
    }

    @Test
    public void testUpdateReadThreadPoolMaxSize() throws IOException {
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);

            // Verify the read thread pool max size before the update
            assertEquals(DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE,
                    ((DistributedFileSystem) rsm.getFS(defaultFsUri, true)).getDFSClientReaderThreadPoolMaxSize());

            // Update the read thread pool max size and verify the returned FileSystem has the updated configuration
            int newMaxSize = DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE + 1;
            rsm.setReadThreadPoolMaxSize(newMaxSize);
            rsm.handleDynamicHedgedReadsConfigUpdates();
            assertEquals(newMaxSize, ((DistributedFileSystem) rsm.getFS(defaultFsUri, true)).getDFSClientReaderThreadPoolMaxSize());
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

            assertEquals(4, reconfigurableConfigs.size());
            assertTrue(reconfigurableConfigs.contains(HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP));
            assertTrue(reconfigurableConfigs.contains(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP));
            assertTrue(reconfigurableConfigs.contains(HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP));
            assertTrue(reconfigurableConfigs.contains(HDFS_OCI_BUCKETS_PROP));
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
    public void testReconfigure() throws Exception {
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);

            // Verify the initial configuration
            assertEquals("true", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
            assertEquals("200", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
            assertEquals("1", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY));
            assertEquals("100", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY));

            // Reconfigure with new threshold
            Map<String, String> configs = new HashMap<>();
            configs.put(HDFS_DFS_CLIENT_HEDGED_READ_THRESHOLD_MILLIS_PROP, "100");
            configs.put(HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE_PROP, "5");
            configs.put(HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE_PROP, "200");

            // Verify that all the configs are reconfigurable
            configs.keySet().forEach(key -> assertTrue(rsm.reconfigurableConfigs().contains(key)));

            // Reconfigure and verify the new configurations
            rsm.reconfigure(configs);
            rsm.handleDynamicHedgedReadsConfigUpdates();
            assertEquals("100", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
            assertEquals("5", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY));
            assertEquals("200", rsm.getFS(defaultFsUri, true).getConf().get(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY));
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
    public void testGetPartitionRemoteDir() {
        RemoteLogSegmentId segmentId = generateRemoteLogSegmentId();
        String partitionRemoteDir = HDFSRemoteStorageManager.getPartitionRemoteDir(baseDir, segmentId.topicIdPartition());
        assertEquals("/user/kloak/kafka-remote-logs/test-0-hHJfD_slRkGCrDPSvJsMtA", partitionRemoteDir);
    }

    @Test
    public void testGetSegmentRemoteDir() {
        RemoteLogSegmentId segmentId = generateRemoteLogSegmentId();
        String segmentRemoteDir = HDFSRemoteStorageManager.getSegmentRemoteDir(baseDir, segmentId);
        assertEquals("/user/kloak/kafka-remote-logs/test-0-hHJfD_slRkGCrDPSvJsMtA/pQpAc9OvTGaxywm8JnN9IQ", segmentRemoteDir);
    }

    @Test
    public void testRelogin() throws Exception {
        try (MockedStatic<UserGroupInformation> mockedUserGroupInfo = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation mockUser = mock(UserGroupInformation.class);
            mockedUserGroupInfo.when(UserGroupInformation::getCurrentUser).thenReturn(mockUser);
            rsm.relogin();
            verify(mockUser, atLeastOnce()).checkTGTAndReloginFromKeytab();
        }
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
    public void testReConfigureWithOciBuckets() {
        String ociBucket1 = "oci://uber@abc/lwrka";
        String ociBucket2 = "oci://uber@def/lwrka";
        String ociBucket3 = "oci://uber@xyz/lwrka";
        List<String> allBuckets = Arrays.asList(ociBucket1, ociBucket2, ociBucket3);

        configs.put(HDFS_OCI_BUCKETS_PROP, String.join(",", allBuckets));
        AtomicInteger instanceCount = new AtomicInteger();
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
             MockedStatic<FileSystem> mockedFileSystem = Mockito.mockStatic(FileSystem.class)) {
            // the MiniDFSCluster only supports hdfs filesystem, but we want to test with different valid schemes
            // so we mock the FileSystem creation
            mockedFileSystem.when(() -> FileSystem.get(any(URI.class), any(Configuration.class)))
                    .thenAnswer(ans -> {
                        instanceCount.incrementAndGet();
                        return hdfs;
                    });
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            rsm.configure(configs);

            assertEquals(allBuckets, rsm.ociBuckets());
            // verify that the FileSystem instance is called 5 times
            // once for the default filesystem, once for the hedged reads enabled filesystem and 3 times for the OCI buckets
            assertEquals(5, instanceCount.get());

            Uuid topicId = Uuid.fromString("p9egHc6hSBGpCXzSk59d7g");
            String topic = "topicA";
            TopicIdPartition p0tpId = new TopicIdPartition(topicId, new TopicPartition(topic, 0));
            RemoteLogSegmentId p0SegId0 = new RemoteLogSegmentId(p0tpId, Uuid.fromString("k5X5v70mQcWQ34-gnNDJhA"));
            RemoteLogSegmentId p0SegId1 = new RemoteLogSegmentId(p0tpId, Uuid.fromString("cxXowFkJSWysCDlVs8WFfQ"));
            TopicIdPartition p1tpId = new TopicIdPartition(topicId, new TopicPartition(topic, 1));
            RemoteLogSegmentId p1SegId0 = new RemoteLogSegmentId(p1tpId, Uuid.fromString("zq3EhJvvRfamDtjXm1UGmw"));
            RemoteLogSegmentId p1SegId1 = new RemoteLogSegmentId(p1tpId, Uuid.fromString("R0KHXc26RFSZYLMczT0VFA"));

            // verify that the same bucket is returned for the same partition
            assertEquals(ociBucket1, rsm.findBucket(RemoteStorageProvider.OCI, p0SegId0));
            assertEquals(ociBucket1, rsm.findBucket(RemoteStorageProvider.OCI, p0SegId1));

            assertEquals(ociBucket3, rsm.findBucket(RemoteStorageProvider.OCI, p1SegId0));
            assertEquals(ociBucket3, rsm.findBucket(RemoteStorageProvider.OCI, p1SegId1));

            // Reconfigure the OCI Buckets -- add new buckets
            String ociBucket4 = "oci://uber@ghi/lwrka";
            List<String> expectedBuckets = Arrays.asList(ociBucket1, ociBucket2, ociBucket3, ociBucket4);
            String updatedOciBuckets = String.join(",", expectedBuckets);
            configs.put(HDFS_OCI_BUCKETS_PROP, updatedOciBuckets);
            rsm.reconfigure(configs);
            assertNotNull(rsm.getFS(ociBucket4));
            assertEquals(6, instanceCount.get());
            assertEquals(expectedBuckets, rsm.ociBuckets());

            // Reconfigure the OCI Buckets -- remove some buckets
            expectedBuckets = Arrays.asList(ociBucket1, ociBucket3, ociBucket4);
            updatedOciBuckets = String.join(",", expectedBuckets);
            configs.put(HDFS_OCI_BUCKETS_PROP, updatedOciBuckets);
            rsm.reconfigure(configs);
            // removed bucket should still be accessible for reads.
            assertNotNull(rsm.getFS(ociBucket2));
            assertEquals(6, instanceCount.get());
            assertEquals(expectedBuckets, rsm.ociBuckets());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"abcd", "invalid-uri"})
    public void testConfigureInvalidOciBuckets(String ociBuckets) {
        configs.put(HDFS_OCI_BUCKETS_PROP, ociBuckets);
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            assertThrows(IllegalArgumentException.class, () -> rsm.configure(configs));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"abcd", "invalid-uri"})
    public void testConfigureInvalidHdfsBuckets(String hdfsBuckets) {
        configs.put(HDFS_DEFAULT_FS_URI_PROP, hdfsBuckets);
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setDefaultHadoopConfiguration(hadoopConf);
            assertThrows(IllegalArgumentException.class, () -> rsm.configure(configs));
        }
    }

    @Test
    public void testGetBuckets() {
        RemoteLogSegmentId segmentId = generateRemoteLogSegmentId();
        long timestamp = time.milliseconds();
        int segmentSize = 1024;
        Map<Integer, Long> segmentLeaderEpochs = Collections.singletonMap(0, 0L);
        RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(segmentId, 0, 100,
                timestamp, 0, timestamp, segmentSize, Optional.empty(),
                RemoteLogSegmentState.DELETE_SEGMENT_STARTED, segmentLeaderEpochs);

        String bucket = "bucket";
        RemoteLogSegmentMetadata.CustomMetadata customMetadata1 = HDFSRemoteStorageManager.createCustomMetadata(bucket);
        RemoteLogSegmentMetadata metadata1 = new RemoteLogSegmentMetadata(segmentId, 101, 200,
                timestamp, 0, timestamp, segmentSize, Optional.of(customMetadata1),
                RemoteLogSegmentState.DELETE_SEGMENT_STARTED, segmentLeaderEpochs);

        Set<String> buckets = rsm.getBuckets(Arrays.asList(metadata, metadata1));
        assertEquals(2, buckets.size());
        assertTrue(buckets.contains(bucket));
        assertTrue(buckets.contains(defaultFsUri));
    }

    @ParameterizedTest
    @CsvSource(value = {
            "oci://uber-staging-vwxyz@ab9cdef6ghij/lwrka, oci://uber-staging-vwxyz@ab9cdef6ghij/lwrka",
            "oci://uber-prod-ea6bj@ax9estk6tuja/jwj42, oci://uber-prod-ea6bj@ax9estk6tuja",
            "oci://uber-prod-abcde@ax9estk6tuja/jwj42, oci://uber-prod-abcde@ax9estk6tuja/jwj42"
    })
    public void testCustomMetadataSizeWithinAllowedMaxBytes(String bucket, String expectedBucket) {
        RemoteLogSegmentMetadata.CustomMetadata customMetadata = HDFSRemoteStorageManager.createCustomMetadata(bucket);
        assertNotNull(customMetadata);
        assertEquals(expectedBucket, HDFSRemoteStorageManager.getBucket(customMetadata));
        assertTrue(customMetadata.value().length < RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES);

        // Backward compatibility
        // `kafka-dev1-dca` is already deployed with the old build. This can be removed once the stress test is completed.
        assertEquals(expectedBucket, HDFSRemoteStorageManager.getBucket(
                new RemoteLogSegmentMetadata.CustomMetadata(bucket.getBytes(StandardCharsets.UTF_8))));
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

    private RemoteLogSegmentId generateRemoteLogSegmentId() {
        Uuid segmentId = Uuid.fromString("pQpAc9OvTGaxywm8JnN9IQ");
        Uuid topicId = Uuid.fromString("hHJfD_slRkGCrDPSvJsMtA");
        TopicPartition tp = new TopicPartition("test", 0);
        TopicIdPartition tpId = new TopicIdPartition(topicId, tp);
        return new RemoteLogSegmentId(tpId, segmentId);
    }

    private void verifyTimerCount(String name, long expectedValue) {
        verifyTimerCount(name, Collections.emptyMap(), expectedValue);
    }

    private void verifyTimerCount(String name, Map<String, String> tags, long expectedValue) {
        Timer timer = findKafkaMetric(name, tags)
                .map(metric -> (Timer) metric)
                .orElseThrow(() -> new AssertionError("Metric " + name + " with tags " + tags + " not found"));
        assertEquals(expectedValue, timer.count(), "Timer count check failed for " + name + " with tags " + tags);
    }

    private void verifyTimerQuantile(String name, double quantile, Predicate<Double> assertion) {
        verifyTimerQuantile(name, Collections.emptyMap(), quantile, assertion);
    }

    private void verifyTimerQuantile(String name, Map<String, String> tags, double quantile, Predicate<Double> assertion) {
        Timer timer = findKafkaMetric(name, tags)
                .map(metric -> (Timer) metric)
                .orElseThrow(() -> new AssertionError("Metric " + name + " not found"));

        double value = timer.getSnapshot().getValue(quantile);
        assertTrue(assertion.test(value), "Timer quantile check failed for " + name);
    }

    private void verifyMeter(String name, Map<String, String> tags, long expectedCount) {
        Meter meter = findKafkaMetric(name, tags)
                .map(metric -> (Meter) metric)
                .orElseThrow(() -> new AssertionError("Meter " + name + " with tags " + tags + " not found"));
        assertEquals(expectedCount, meter.count(), "Meter count check failed for " + name + " with tags " + tags);
    }

    private <T> void verifyGauge(String name, T expectedValue) {
        Optional<Metric> metric = findKafkaMetric(name)
                .filter(m -> m instanceof Gauge<?>);

        assertTrue(metric.isPresent(), "Metric " + name + " not found or not a Gauge");
        Object actualValue = ((Gauge<?>) metric.get()).value();
        assertEquals(expectedValue, actualValue, "Gauge value mismatch for " + name);
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

    private void clearKafkaMetrics() {
        KafkaYammerMetrics.defaultRegistry().allMetrics().forEach(
            (metricName, metric) -> KafkaYammerMetrics.defaultRegistry().removeMetric(metricName));
    }

    private Optional<Metric> findKafkaMetric(String name) {
        return findKafkaMetric(name, Collections.emptyMap());
    }

    private Optional<Metric> findKafkaMetric(String name, Map<String, String> tags) {
        String scope = tags.entrySet().stream().map(e -> e.getKey() + "." + e.getValue()).collect(Collectors.joining(","));
        return KafkaYammerMetrics.defaultRegistry().allMetrics()
                .entrySet()
                .stream()
                .filter(entry -> {
                    MetricName metricName = entry.getKey();
                    return metricName.getGroup().equals(HDFSRemoteStorageManager.class.getPackage().getName()) &&
                            metricName.getType().equals(HDFSRemoteStorageManager.class.getSimpleName()) &&
                            metricName.getName().equals(name) && (!metricName.hasScope() || metricName.getScope().equals(scope));
                })
                .findFirst()
                .map(Map.Entry::getValue);
    }

    private void verifyFetchLogSegmentDefaultPrefetch(RemoteStorageManager rsm,
                                                      RemoteLogSegmentMetadata metadata,
                                                      LogSegmentData segmentData,
                                                      int startPosition,
                                                      int endPosition,
                                                      int size) throws Exception {
        RemoteReadContext readContext = new RemoteReadContext(true, false);
        verifyFetchLogSegmentInternal(rsm, metadata, segmentData, readContext, startPosition, endPosition, size);
    }

    private void verifyFetchLogSegmentDefaultPrefetchAndHedgedReads(RemoteStorageManager rsm,
                                                                    RemoteLogSegmentMetadata metadata,
                                                                    LogSegmentData segmentData,
                                                                    int startPosition,
                                                                    int endPosition,
                                                                    int size) throws Exception {
        RemoteReadContext readContext = new RemoteReadContext(true, true);
        verifyFetchLogSegmentInternal(rsm, metadata, segmentData, readContext, startPosition, endPosition, size);
    }

    private void verifyFetchLogSegmentWithPrefetchVariants(RemoteStorageManager rsm,
                                                           RemoteLogSegmentMetadata metadata,
                                                           LogSegmentData segmentData,
                                                           int startPosition,
                                                           int endPosition,
                                                           int size) throws Exception {
        for (boolean enablePrefetch : Arrays.asList(true, false)) {
            RemoteReadContext readContext = new RemoteReadContext(enablePrefetch, false);
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
                assertTrue(stream instanceof HDFSRemoteStorageManager.CachedInputStream);
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
        assertTrue(hdfs.exists(new Path(HDFSRemoteStorageManager.getPartitionRemoteDir(baseDir, tp) + Path.SEPARATOR + uuid)));
        long expectedBytesRead = TestLogSegmentUtils.OFFSET_INDEX_FILE_SIZE + TestLogSegmentUtils.TIME_INDEX_FILE_SIZE +
                TestLogSegmentUtils.LEADER_EPOCH_INDEX_FILE_SIZE + TestLogSegmentUtils.PRODUCER_SNAPSHOT_FILE_SIZE;
        long expectedBytesReadWithTxnIndex = expectedBytesRead + TestLogSegmentUtils.TXN_INDEX_FILE_SIZE;
        if (withOptionalFiles) {
            assertEquals(expectedBytesReadWithTxnIndex, ((HDFSRemoteStorageManager) rsm).bytesReadFromRemote() - bytesReadFromRemoteSoFar);
        } else {
            assertEquals(expectedBytesRead, ((HDFSRemoteStorageManager) rsm).bytesReadFromRemote() - bytesReadFromRemoteSoFar);
        }
        assertTrue(customMetadataOpt.isPresent());
        assertEquals(defaultFsUri, HDFSRemoteStorageManager.getBucket(customMetadataOpt.get()));
        return segmentMetadata;
    }

    private void verifyDeleteRemoteLogSegment(RemoteStorageManager rsm,
                                              RemoteLogSegmentMetadata metadata,
                                              TopicIdPartition tp,
                                              Uuid uuid) throws RemoteStorageException, IOException {
        rsm.deleteLogSegmentData(metadata);
        assertFalse(hdfs.exists(new Path(baseDir + Path.SEPARATOR + tp.topicId() + Path.SEPARATOR +
                tp.topicPartition() + Path.SEPARATOR + uuid)));
        RemoteStorageException ex = assertThrows(RemoteStorageException.class, () ->
                rsm.fetchLogSegment(metadata, 0));
        assertEquals("Failed to fetch SEGMENT file from remote storage. Metadata: " + metadata,
                ex.getMessage());
        ex = assertThrows(RemoteStorageException.class, () ->
                rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.OFFSET));
        assertEquals("Failed to fetch OFFSET_INDEX file from remote storage. Metadata: " + metadata,
                ex.getMessage());
    }

    private void checkFileExistence(Uuid uuid) throws IOException {
        Path path = new Path(HDFSRemoteStorageManager.getPartitionRemoteDir(baseDir, tp));
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
            RemoteReadContext readContext = new RemoteReadContext(enablePrefetch, false);
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
