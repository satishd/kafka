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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageProvider;

import com.oracle.bmc.hdfs.BmcConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_DEFAULT_FS_URI_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.HDFS_OCI_BUCKETS_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link FileSystemManager}.
 */
public class FileSystemManagerTest {
    private static final String OCI_BUCKET = "oci://uber@abc/lwrka";
    private MiniDFSCluster cluster;
    private FileSystem hdfs;
    private Configuration hadoopConf;
    private Map<String, Object> configs;
    private String defaultFsUri;
    private String baseDir;
    private Time time;
    private TopicIdPartition tp;
    private HDFSRemoteStorageManager rsm;

    @BeforeEach
    public void setup() throws Exception {
        File baseDir = new File(System.getProperty("java.io.tmpdir"), "miniHDFS-" + System.currentTimeMillis());
        hadoopConf = new Configuration();
        hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        cluster = new MiniDFSCluster.Builder(hadoopConf).build();
        hdfs = cluster.getFileSystem();
        defaultFsUri = hdfs.getUri().toString();
        this.baseDir = "/user/kloak/kafka-remote-logs";
        time = new MockTime();
        tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test", 0));

        configs = new HashMap<>();
        configs.put(HDFS_DEFAULT_FS_URI_PROP, defaultFsUri);
        configs.put(HDFS_BASE_DIR_PROP, this.baseDir);

        rsm = new HDFSRemoteStorageManager();
        rsm.setDefaultHadoopConfiguration(hadoopConf);
        rsm.configure(configs);
    }

    @AfterEach
    public void tearDown() {
        if (rsm != null) {
            rsm.close();
        }
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test
    public void testSetHedgedReadsConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, "/tmp");
        HDFSRemoteStorageManagerConfig remoteStorageManagerConfig = new HDFSRemoteStorageManagerConfig(props, false);

        Configuration config = new Configuration();
        FileSystemManager fileSystemManager = new FileSystemManager();
        fileSystemManager.setHedgedReadsConfiguration(config, remoteStorageManagerConfig);

        assertEquals("true", config.get(HdfsClientConfigKeys.HedgedRead.ENABLED));
        assertEquals("200", config.get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
        assertEquals("1", config.get(HdfsClientConfigKeys.ReadThreadPool.CORE_SIZE_KEY));
        assertEquals("100", config.get(HdfsClientConfigKeys.ReadThreadPool.MAX_SIZE_KEY));
        assertEquals("60", config.get(HdfsClientConfigKeys.ReadThreadPool.KEEP_ALIVE_TIME_KEY));
        assertEquals("true", config.get(HdfsClientConfigKeys.ReadThreadPool.ALLOW_CORE_THREAD_TIMEOUT_KEY));
    }


    @Test
    public void testGetFSDefaultConfiguration() throws IOException {
        FileSystemManager fileSystemManager = rsm.fileSystemManager();
        FileSystem fs = fileSystemManager.getFS(new FileSystemOptions(defaultFsUri));
        assertNotNull(fs);
        assertNull(fs.getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
    }

    @Test
    public void testGetFSHedgedReads() throws IOException {
        FileSystemManager fileSystemManager = rsm.fileSystemManager();
        FileSystem fs = fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true));
        assertNotNull(fs);
        assertEquals("true", fs.getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
    }

    @Test
    public void testGetFSUsesCache() throws IOException {
        FileSystemManager fileSystemManager = rsm.fileSystemManager();
        FileSystem fs1 = fileSystemManager.getFS(new FileSystemOptions(defaultFsUri));
        FileSystem fs2 = fileSystemManager.getFS(new FileSystemOptions(defaultFsUri));
        assertSame(fs1, fs2);

        FileSystem fs3 = fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true));
        assertNotSame(fs1, fs3);
    }

    @Test
    public void testGetFileSystemWithHedgedReads() throws IOException {
        FileSystemManager fileSystemManager = rsm.fileSystemManager();

        // Verify hedged reads is disabled by default
        assertNull(fileSystemManager.getFS(new FileSystemOptions(defaultFsUri)).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));

        // Verify the configuration of the returned FileSystem when hedged reads is enabled
        assertEquals("true", fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true)).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));

        // Verify the configuration of returned FileSystem when hedged reads is disabled
        assertNull(fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, false)).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
    }

    @Test
    public void testUpdateHedgedReadsThreshold() throws IOException {
        FileSystemManager fileSystemManager = rsm.fileSystemManager();

        // Verify hedged reads is enabled in the FileSystem configuration
        FileSystem originalFS = fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true));
        assertEquals("true", originalFS.getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
        assertEquals("200", originalFS.getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));

        // Without updates, the returned FileSystem should be the same as the original one
        FileSystem fsPreUpdate = fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true));
        assertEquals("true", fsPreUpdate.getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
        assertEquals("200", fsPreUpdate.getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
        assertSame(originalFS, fsPreUpdate);

        // Update the hedged reads threshold and verify the returned FileSystem has the updated configuration
        fileSystemManager.setHedgedReadThresholdMillis(100);
        fileSystemManager.handleDynamicHedgedReadsConfigUpdates();
        FileSystem fsPostUpdate = fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true));
        assertEquals("true", fsPostUpdate.getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
        assertEquals("100", fsPostUpdate.getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
        assertNotSame(originalFS, fsPostUpdate);

        // Verify updates to the configuration does not affect the returned FileSystem when hedged reads is disabled
        fileSystemManager.setHedgedReadThresholdMillis(500);
        fileSystemManager.handleDynamicHedgedReadsConfigUpdates();
        assertNull(fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, false)).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));

        // Verify the previous update takes affect for the filesystem with hedged reads enabled
        assertEquals("true", fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true)).getConf().get(HdfsClientConfigKeys.HedgedRead.ENABLED));
        assertEquals("500", fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true)).getConf().get(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY));
    }

    @Test
    public void testUpdateReadThreadPoolCoreSize() throws IOException {
        FileSystemManager fileSystemManager = rsm.fileSystemManager();

        // Verify the read thread pool core size before the update
        assertEquals(DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE,
                ((DistributedFileSystem) fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true))).getDFSClientReaderThreadPoolSize());

        // Update the read thread pool core size and verify the returned FileSystem has the updated configuration
        int newCoreSize = DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_CORE_SIZE + 1;
        fileSystemManager.setReadThreadPoolCoreSize(newCoreSize);
        fileSystemManager.handleDynamicHedgedReadsConfigUpdates();
        assertEquals(newCoreSize, ((DistributedFileSystem) fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true))).getDFSClientReaderThreadPoolSize());
    }

    @Test
    public void testUpdateReadThreadPoolMaxSize() throws IOException {
        FileSystemManager fileSystemManager = rsm.fileSystemManager();

        // Verify the read thread pool max size before the update
        assertEquals(DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE,
                ((DistributedFileSystem) fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true))).getDFSClientReaderThreadPoolMaxSize());

        // Update the read thread pool max size and verify the returned FileSystem has the updated configuration
        int newMaxSize = DEFAULT_HDFS_DFS_CLIENT_READ_THREADPOOL_MAX_SIZE + 1;
        fileSystemManager.setReadThreadPoolMaxSize(newMaxSize);
        fileSystemManager.handleDynamicHedgedReadsConfigUpdates();
        assertEquals(newMaxSize, ((DistributedFileSystem) fileSystemManager.getFS(new FileSystemOptions(defaultFsUri, true))).getDFSClientReaderThreadPoolMaxSize());
    }

    @Test
    public void testRelogin() throws Exception {
        try (MockedStatic<UserGroupInformation> mockedUserGroupInfo = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation mockUser = mock(UserGroupInformation.class);
            mockedUserGroupInfo.when(UserGroupInformation::getCurrentUser).thenReturn(mockUser);
            rsm.fileSystemManager().relogin();
            verify(mockUser, atLeastOnce()).checkTGTAndReloginFromKeytab();
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
            // verify that the FileSystem instance is called 8 times
            // once for the default filesystem, once for the hedged reads enabled filesystem and 3 times for the OCI
            // buckets with and without read ahead
            assertEquals(8, instanceCount.get());

            Uuid topicId = Uuid.fromString("p9egHc6hSBGpCXzSk59d7g");
            String topic = "topicA";
            TopicIdPartition p0tpId = new TopicIdPartition(topicId, new TopicPartition(topic, 0));
            RemoteLogSegmentId p0SegId0 = new RemoteLogSegmentId(p0tpId, Uuid.fromString("k5X5v70mQcWQ34-gnNDJhA"));
            RemoteLogSegmentId p0SegId1 = new RemoteLogSegmentId(p0tpId, Uuid.fromString("cxXowFkJSWysCDlVs8WFfQ"));
            TopicIdPartition p1tpId = new TopicIdPartition(topicId, new TopicPartition(topic, 1));
            RemoteLogSegmentId p1SegId0 = new RemoteLogSegmentId(p1tpId, Uuid.fromString("zq3EhJvvRfamDtjXm1UGmw"));
            RemoteLogSegmentId p1SegId1 = new RemoteLogSegmentId(p1tpId, Uuid.fromString("R0KHXc26RFSZYLMczT0VFA"));

            FileSystemManager fileSystemManager = rsm.fileSystemManager();
            // verify that the same bucket is returned for the same partition
            assertEquals(ociBucket1, fileSystemManager.findBucket(RemoteStorageProvider.OCI, p0SegId0));
            assertEquals(ociBucket1, fileSystemManager.findBucket(RemoteStorageProvider.OCI, p0SegId1));

            assertEquals(ociBucket3, fileSystemManager.findBucket(RemoteStorageProvider.OCI, p1SegId0));
            assertEquals(ociBucket3, fileSystemManager.findBucket(RemoteStorageProvider.OCI, p1SegId1));

            // Reconfigure the OCI Buckets -- add new buckets
            String ociBucket4 = "oci://uber@ghi/lwrka";
            List<String> expectedBuckets = Arrays.asList(ociBucket1, ociBucket2, ociBucket3, ociBucket4);
            String updatedOciBuckets = String.join(",", expectedBuckets);
            configs.put(HDFS_OCI_BUCKETS_PROP, updatedOciBuckets);
            rsm.reconfigure(configs);
            assertNotNull(fileSystemManager.getFS(new FileSystemOptions(ociBucket4)));
            assertEquals(9, instanceCount.get());
            assertEquals(expectedBuckets, rsm.ociBuckets());

            // Reconfigure the OCI Buckets -- remove some buckets
            expectedBuckets = Arrays.asList(ociBucket1, ociBucket3, ociBucket4);
            updatedOciBuckets = String.join(",", expectedBuckets);
            configs.put(HDFS_OCI_BUCKETS_PROP, updatedOciBuckets);
            rsm.reconfigure(configs);
            // removed bucket should still be accessible for reads.
            assertNotNull(fileSystemManager.getFS(new FileSystemOptions(ociBucket2)));
            assertEquals(9, instanceCount.get());
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
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(tp, Uuid.randomUuid());
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
            "oci://uber-prod-abcde@ax9estk6tuja/jwj42, oci://uber-prod-abcde@ax9estk6tuja/jwj42"
    })
    public void testCustomMetadataSizeWithinAllowedMaxBytes(String bucket, String expectedBucket) {
        RemoteLogSegmentMetadata.CustomMetadata customMetadata = HDFSRemoteStorageManager.createCustomMetadata(bucket);
        assertNotNull(customMetadata);
        assertEquals(expectedBucket, FileSystemManager.getBucket(customMetadata));
        assertTrue(customMetadata.value().length < RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_CUSTOM_METADATA_MAX_BYTES);
    }

    @Test
    public void testSetOCIReadAheadConfigurationWithDefaultValues() {
        Map<String, Object> props = new HashMap<>();
        props.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, "/tmp");
        HDFSRemoteStorageManagerConfig remoteStorageManagerConfig = new HDFSRemoteStorageManagerConfig(props, false);

        Configuration config = new Configuration();
        FileSystemManager fileSystemManager = new FileSystemManager();
        fileSystemManager.setOCIReadAheadConfiguration(config, remoteStorageManagerConfig);

        assertEquals("2", config.get(BmcConstants.READ_AHEAD_BLOCK_COUNT_KEY));
        assertEquals("4194304", config.get(BmcConstants.READ_AHEAD_BLOCK_SIZE_KEY));
        assertEquals("10", config.get(BmcConstants.NUM_READ_AHEAD_THREADS_KEY));
    }

    @Test
    public void testSetOCIReadAheadConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, "/tmp");
        props.put(HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP, 1);
        props.put(HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP, 1048576);
        props.put(HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP, 2);
        HDFSRemoteStorageManagerConfig remoteStorageManagerConfig = new HDFSRemoteStorageManagerConfig(props, false);

        Configuration config = new Configuration();
        FileSystemManager fileSystemManager = new FileSystemManager();
        fileSystemManager.setOCIReadAheadConfiguration(config, remoteStorageManagerConfig);

        assertEquals("1", config.get(BmcConstants.READ_AHEAD_BLOCK_COUNT_KEY));
        assertEquals("1048576", config.get(BmcConstants.READ_AHEAD_BLOCK_SIZE_KEY));
        assertEquals("2", config.get(BmcConstants.NUM_READ_AHEAD_THREADS_KEY));
    }

    @ParameterizedTest
    @CsvSource({
        "0, 1048576, 5",
        "4, 512, 5",
        "4, 1048576, 0"
    })
    public void testSetOCIReadAheadConfigurationWithInvalidValues(int blockCount, int blockSize, int numThreads) {
        Map<String, Object> props = new HashMap<>();
        props.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, "/tmp");
        props.put(HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP, blockCount);
        props.put(HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP, blockSize);
        props.put(HDFSRemoteStorageManagerConfig.OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP, numThreads);

        assertThrows(ConfigException.class, () -> new HDFSRemoteStorageManagerConfig(props, false));
    }

    /**
     * Test that the readAheadHadoopConf is correctly configured with the values from HDFSRemoteStorageManagerConfig.
     */
    @Test
    public void testReadAheadConfiguration() throws Exception {
        // Initialize mock FileSystem
        FileSystem mockFileSystem = mock(FileSystem.class);
        
        // Create a new configs map for this test
        Map<String, Object> testConfigs = new HashMap<>();
        testConfigs.put(HDFS_DEFAULT_FS_URI_PROP, defaultFsUri);
        testConfigs.put(HDFS_OCI_BUCKETS_PROP, OCI_BUCKET);
        testConfigs.put(HDFS_BASE_DIR_PROP, "kafka-remote-logs");
        
        // Capture the Configuration object passed to FileSystem.get
        ArgumentCaptor<Configuration> configCaptor = ArgumentCaptor.forClass(Configuration.class);
        
        try (MockedStatic<FileSystem> mockedFileSystem = Mockito.mockStatic(FileSystem.class)) {
            // Mock FileSystem.get to return our mock FileSystem and capture the Configuration
            mockedFileSystem.when(() -> FileSystem.get(any(URI.class), configCaptor.capture()))
                    .thenReturn(mockFileSystem);
            
            // Create and configure the FileSystemManager
            FileSystemManager fileSystemManager = new FileSystemManager();
            fileSystemManager.configure(testConfigs);
            
            // Get a FileSystem with read-ahead enabled
            FileSystemOptions readAheadOptions = new FileSystemOptions(OCI_BUCKET, false, true);
            fileSystemManager.getFS(readAheadOptions);
            
            // Get the captured Configuration
            Configuration readAheadConf = configCaptor.getValue();
            
            // Verify that the read-ahead configuration values are set correctly
            assertEquals(DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT, 
                    readAheadConf.getInt(BmcConstants.READ_AHEAD_BLOCK_COUNT_KEY, -1));
            assertEquals(DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE, 
                    readAheadConf.getInt(BmcConstants.READ_AHEAD_BLOCK_SIZE_KEY, -1));
            assertEquals(DEFAULT_OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS,
                    readAheadConf.getInt(BmcConstants.NUM_READ_AHEAD_THREADS_KEY, -1));
            assertTrue(readAheadConf.getBoolean("fs.oci.impl.disable.cache", false));
        }
    }

    /**
     * Test that the getFS method correctly uses readAheadHadoopConf when readAheadEnabled is true.
     */
    @Test
    public void testGetFSWithReadAheadEnabled() throws Exception {
        // Initialize mock FileSystem
        FileSystem mockFileSystem = mock(FileSystem.class);
        
        // Create a new configs map for this test
        Map<String, Object> testConfigs = new HashMap<>();
        testConfigs.put(HDFS_DEFAULT_FS_URI_PROP, defaultFsUri);
        testConfigs.put(HDFS_OCI_BUCKETS_PROP, OCI_BUCKET);
        testConfigs.put(HDFS_BASE_DIR_PROP, "kafka-remote-logs");
        
        // Set custom values for read-ahead configuration
        int customBlockCount = 5;
        int customBlockSize = 8388608; // 8MB
        int customNumThreads = 20;
        
        testConfigs.put(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_COUNT_PROP, customBlockCount);
        testConfigs.put(OCI_PREFETCH_CLIENT_READ_AHEAD_BLOCK_SIZE_PROP, customBlockSize);
        testConfigs.put(OCI_PREFETCH_CLIENT_READ_AHEAD_NUM_THREADS_PROP, customNumThreads);
        
        // Capture the Configuration object passed to FileSystem.get
        ArgumentCaptor<Configuration> configCaptor = ArgumentCaptor.forClass(Configuration.class);
        ArgumentCaptor<URI> uriCaptor = ArgumentCaptor.forClass(URI.class);
        
        try (MockedStatic<FileSystem> mockedFileSystem = Mockito.mockStatic(FileSystem.class)) {
            // Mock FileSystem.get to return our mock FileSystem and capture the Configuration
            mockedFileSystem.when(() -> FileSystem.get(uriCaptor.capture(), configCaptor.capture()))
                    .thenReturn(mockFileSystem);
            
            // Create and configure the FileSystemManager
            FileSystemManager fileSystemManager = new FileSystemManager();
            fileSystemManager.configure(testConfigs);
            
            // Get a FileSystem with read-ahead enabled
            FileSystemOptions readAheadOptions = new FileSystemOptions(OCI_BUCKET, false, true);
            FileSystem fs = fileSystemManager.getFS(readAheadOptions);
            
            // Verify that the correct FileSystem was returned
            assertNotNull(fs);
            assertEquals(mockFileSystem, fs);
            
            // Verify that the correct URI was used
            assertEquals(new URI(OCI_BUCKET), uriCaptor.getValue());
            
            // Get the captured Configuration
            Configuration readAheadConf = configCaptor.getValue();
            
            // Verify that the read-ahead configuration values are set correctly
            assertEquals(customBlockCount, 
                    readAheadConf.getInt(BmcConstants.READ_AHEAD_BLOCK_COUNT_KEY, -1));
            assertEquals(customBlockSize, 
                    readAheadConf.getInt(BmcConstants.READ_AHEAD_BLOCK_SIZE_KEY, -1));
            assertEquals(customNumThreads, 
                    readAheadConf.getInt(BmcConstants.NUM_READ_AHEAD_THREADS_KEY, -1));
        }
    }
}
