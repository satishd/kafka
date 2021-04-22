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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.log.remote.storage.LogSegmentData;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.log.remote.storage.RemoteStorageException;
import org.apache.kafka.common.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HDFSRemoteStorageManagerTest {
    private File logDir;
    private File remoteDir;
    private MiniDFSCluster hdfsCluster;
    private FileSystem hdfs;
    private final String baseDir = "/kafka-remote-logs";
    private Map<String, String> configs;

    private RemoteStorageManager rsm;

    @BeforeEach
    public void setup() throws Exception {
        logDir = TestUtils.tempDirectory();
        remoteDir = TestUtils.tempDirectory();

        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, remoteDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        builder.clusterId("test_mini_dfs_cluster");
        hdfsCluster = builder.build();
        String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
        hdfs = FileSystem.newInstance(new URI(hdfsURI), conf);

        configs = new HashMap<>();
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_URI_PROP, hdfsURI);
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, baseDir);

        rsm = new HDFSRemoteStorageManager();
        rsm.configure(configs);
    }

    @AfterEach
    public void tearDown() throws Exception {
        hdfsCluster.shutdown();
        Utils.delete(logDir);
        Utils.delete(remoteDir);
    }

    @Test
    public void testConfigAndClose() throws Exception {
        RemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.configure(configs);
        rsm.close();
    }

    @Test
    public void testSecureLogin() {
        System.setProperty("java.security.krb5.realm", "ATHENA.MIT.EDU");
        System.setProperty("java.security.krb5.kdc", "kerberos.mit.edu:88");
        String user = "test@ATHENA.MIT.EDU";
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_USER_PROP, user);
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_KEYTAB_PATH_PROP, "test.keytab");

        Configuration configuration = new Configuration();
        configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.setHadoopConfiguration(configuration);
        Throwable exception = assertThrows(RuntimeException.class, () -> rsm.configure(configs),
                "Unable to login as user: " + user);
        assertTrue(exception.getCause() instanceof KerberosAuthException);
    }

    @Test
    public void testCopyReadAndDelete() throws Exception {
        TopicPartition tp = new TopicPartition("test", 1);
        UUID uuid = UUID.randomUUID();
        RemoteLogSegmentMetadata metadata = verifyUpload(rsm, tp, uuid, 0, 1000, true);
        verifyDeleteRemoteLogSegment(rsm, metadata, tp, uuid);
    }

    @Test
    public void testCopyReadAndDeleteWithMultipleSegments() throws Exception {
        TopicPartition tp = new TopicPartition("test", 1);
        UUID uuid0 = UUID.randomUUID();
        RemoteLogSegmentMetadata metadata0 = verifyUpload(rsm, tp, uuid0, 0, 1000, true);
        UUID uuid1 = UUID.randomUUID();
        RemoteLogSegmentMetadata metadata1 = verifyUpload(rsm, tp, uuid1, 1000, 2000, true);
        verifyDeleteRemoteLogSegment(rsm, metadata0, tp, uuid0);
        verifyDeleteRemoteLogSegment(rsm, metadata1, tp, uuid1);
    }

    @Test
    public void testCopyReadAndDeleteWithoutOptionalFiles() throws Exception {
        TopicPartition tp = new TopicPartition("test", 1);
        UUID uuid = UUID.randomUUID();
        RemoteLogSegmentMetadata metadata = verifyUpload(rsm, tp, uuid, 0, 1000, false);
        verifyDeleteRemoteLogSegment(rsm, metadata, tp, uuid);
    }

    @Test
    public void testFetchOnOptionalFile() throws Exception {
        TopicPartition tp = new TopicPartition("test", 1);
        UUID uuid = UUID.randomUUID();
        RemoteLogSegmentMetadata metadata = verifyUpload(rsm, tp, uuid, 0, 1000, false);
        try (InputStream stream = rsm.fetchOffsetIndex(metadata)) {
            assertNotEquals(0, stream.available());
        }
        try (InputStream stream = rsm.fetchTransactionIndex(metadata)) {
            assertEquals(0, stream.available());
        }
    }

    @Test
    public void testFetchLogSegment() throws Exception {
        TopicPartition tp = new TopicPartition("test", 1);
        UUID uuid = UUID.randomUUID();
        int segSize = 1000;
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                0, 100, 0, 0, segSize, Collections.emptyMap());
        LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
        rsm.copyLogSegment(segmentMetadata, segmentData);

        // start and end position are both inclusive in RSM
        // full fetch segment
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 0L, 999L, 1000);
        // fetch intermediate segment
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 100L, 199L, 100);
        // fetch till the end of the segment
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 990L, 999L, 10);
        // fetch exceeds the segment size
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 990L, 1050L, 10);
    }
    
    @Test
    public void testRepeatedFetchReadsFromCacheOnFullSegmentFetch() throws Exception {
        LRUCacheWithContext cache = new LRUCacheWithContext(10 * 1048576L);
        // two cache hits due to the additional 25 bytes read for the header exceeds the defined cache line size of 2MB.
        testRepeatedCacheReads(cache, "2", 2097152, 2);
    }

    @Test
    public void testRepeatedFetchReadsFromCacheOnSegmentSizeLessThanCacheLine() throws Exception {
        LRUCacheWithContext cache = new LRUCacheWithContext(10 * 1048576L);
        testRepeatedCacheReads(cache, "1", 1024, 1);
    }

    private void testRepeatedCacheReads(LRUCacheWithContext cache,
                                        String cacheLineSizeInMb,
                                        int segSize,
                                        int expectedCacheHit) throws Exception {
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_MB_PROP, cacheLineSizeInMb);
        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.configure(configs);
        rsm.setLRUCache(cache);

        TopicPartition tp = new TopicPartition("test", 1);
        UUID uuid = UUID.randomUUID();
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                0, 100, 0, 0, segSize, Collections.emptyMap());
        LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
        rsm.copyLogSegment(segmentMetadata, segmentData);

        assertEquals(0, cache.getCacheHit());
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 0L, Long.MAX_VALUE, segSize);
        assertEquals(0, cache.getCacheHit());

        // read from cache
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 0L, Long.MAX_VALUE, segSize);
        // two cache hits due to the additional 25 bytes read for the header.
        assertEquals(expectedCacheHit, cache.getCacheHit());
    }

    /**
     * Verifies the log segment fetch.
     * @param rsm           remote storage manager
     * @param metadata      metadata about the remote log segment.
     * @param segmentData   segment data.
     * @param startPosition start position to fetch from the segment, inclusive
     * @param endPosition   Fetch data till the end position, inclusive
     * @throws Exception I/O Error, file not found exception.
     */
    private void verifyFetchLogSegment(RemoteStorageManager rsm,
                                       RemoteLogSegmentMetadata metadata,
                                       LogSegmentData segmentData,
                                       long startPosition,
                                       long endPosition,
                                       int size) throws Exception {
        try (InputStream stream = rsm.fetchLogSegmentData(metadata, startPosition, endPosition)) {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[size]);
            SeekableByteChannel byteChannel = Files.newByteChannel(segmentData.logSegment().toPath());
            byteChannel.position(startPosition);
            byteChannel.read(buffer);
            buffer.rewind();
            assertDataEquals(buffer, stream);
        }
    }

    private RemoteLogSegmentMetadata verifyUpload(RemoteStorageManager rsm,
                                                  TopicPartition tp,
                                                  UUID uuid,
                                                  int startOffset,
                                                  int segSize,
                                                  boolean withOptionalFiles) throws Exception {
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                0, 100, 0, 0, segSize, Collections.emptyMap());
        LogSegmentData segmentData = TestLogSegmentUtils
                .createLogSegmentData(logDir, startOffset, segSize, withOptionalFiles);
        rsm.copyLogSegment(segmentMetadata, segmentData);
        checkFileExistence(uuid);
        checkAssociatedFileContents(rsm, segmentMetadata, segmentData);
        assertTrue(hdfs.exists(new Path(baseDir + File.separator + tp.toString() + File.separator + uuid)));
        return segmentMetadata;
    }

    private void verifyDeleteRemoteLogSegment(RemoteStorageManager rsm,
                                              RemoteLogSegmentMetadata metadata,
                                              TopicPartition tp,
                                              UUID uuid) throws RemoteStorageException, IOException {
        rsm.deleteLogSegment(metadata);
        assertFalse(hdfs.exists(new Path(baseDir + File.separator + tp.toString() + File.separator + uuid)));
        RemoteStorageException ex = assertThrows(RemoteStorageException.class, () ->
                rsm.fetchLogSegmentData(metadata, 0L, Long.MAX_VALUE));
        assertEquals("Failed to fetch SEGMENT file from remote storage", ex.getMessage());
        ex = assertThrows(RemoteStorageException.class, () ->
                rsm.fetchOffsetIndex(metadata));
        assertEquals("Failed to fetch OFFSET_INDEX file from remote storage", ex.getMessage());
    }

    private void checkFileExistence(UUID uuid) throws IOException {
        Path path = new Path(baseDir, "test-1");
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
        try (InputStream actualStream = rsm.fetchLogSegmentData(metadata, 0L, Long.MAX_VALUE)) {
            assertFileEquals(segmentData.logSegment(), actualStream);
        }
        try (InputStream actualStream = rsm.fetchOffsetIndex(metadata)) {
            assertFileEquals(segmentData.offsetIndex(), actualStream);
        }
        try (InputStream actualStream = rsm.fetchTimestampIndex(metadata)) {
            assertFileEquals(segmentData.timeIndex(), actualStream);
        }
        try (InputStream actualStream = rsm.fetchLeaderEpochIndex(metadata)) {
            assertFileEquals(segmentData.leaderEpochCheckpoint(), actualStream);
        }
        if (segmentData.txnIndex() != null) {
            try (InputStream actualStream = rsm.fetchTransactionIndex(metadata)) {
                assertFileEquals(segmentData.txnIndex(), actualStream);
            }
        }
        if (segmentData.producerIdSnapshotIndex() != null) {
            try (InputStream actualStream = rsm.fetchProducerSnapshotIndex(metadata)) {
                assertFileEquals(segmentData.producerIdSnapshotIndex(), actualStream);
            }
        }
    }

    private void assertFileEquals(File expected, InputStream actual) throws Exception {
        byte[] expectedBuffer = new byte[1_000_000];
        byte[] actualBuffer = new byte[1_000_000];
        try (InputStream is1 = new FileInputStream(expected)) {
            long bytesRead = 0;
            while (bytesRead < expected.length()) {
                long expectedBytesRead = is1.read(expectedBuffer);
                long actualBytesRead = actual.read(actualBuffer);
                assertEquals(expectedBytesRead, actualBytesRead);
                assertArrayEquals(expectedBuffer, actualBuffer);
                bytesRead += expectedBytesRead;
            }
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

        synchronized byte[] get(String path, long offset) {
            byte[] result = super.get(path, offset);
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
