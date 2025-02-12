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
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import io.netty.buffer.ByteBuf;

import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager.ALLOCATOR_CHUNK_SIZE;
import static org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager.ALLOCATOR_USED_HEAP_MEMORY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HDFSRemoteStorageManagerTest {

    private static final int ONE_MB = 1024 * 1024;
    private final String baseDir = "/kafka-remote-logs";
    private final TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test", 1));

    private File logDir;
    private File remoteDir;
    private Configuration hadoopConf;
    private MiniDFSCluster hdfsCluster;
    private FileSystem hdfs;
    private Map<String, String> configs;

    private RemoteStorageManager rsm;
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

        configs = new HashMap<>();
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_BASE_DIR_PROP, baseDir);

        rsm = new HDFSRemoteStorageManager();
        ((HDFSRemoteStorageManager) rsm).setHadoopConfiguration(hadoopConf);
        rsm.configure(configs);
        hdfs = ((HDFSRemoteStorageManager) rsm).getFS();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (rsm != null) {
            rsm.close();
        }
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
        secureConfigs.put(HDFSRemoteStorageManagerConfig.HDFS_USER_PROP, user);
        secureConfigs.put(HDFSRemoteStorageManagerConfig.HDFS_KEYTAB_PATH_PROP, "test.keytab");
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            Configuration configuration = new Configuration();
            configuration.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
            rsm.setHadoopConfiguration(configuration);
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
        try (InputStream stream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.OFFSET)) {
            assertNotEquals(0, stream.available());
        }
        try (InputStream stream = rsm.fetchIndex(metadata, RemoteStorageManager.IndexType.TRANSACTION)) {
            assertEquals(0, stream.available());
        }
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
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 0, 999, 1000);
        // fetch intermediate segment
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 100, 199, 100);
        // fetch till the end of the segment
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 990, 999, 10);
        // fetch exceeds the segment size
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 990, 1050, 10);
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
        // remove the type-cast once the delete-partition is implemented
        ((HDFSRemoteStorageManager) rsm).deletePartition(tp);
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
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP, "1048576");
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setHadoopConfiguration(hadoopConf);
            rsm.configure(configs);
            rsm.setLRUCache(cache);
            // Clear previously registered metrics during object creation
            clearKafkaMetrics();
            rsm.registerMetrics(cache);

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
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP, "1048576");
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setHadoopConfiguration(hadoopConf);
            rsm.configure(configs);
            rsm.setLRUCache(cache);
            // Clear previously registered metrics during object creation
            clearKafkaMetrics();
            rsm.registerMetrics(cache);

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
    public void testNoByteBufResourceLeaksCacheInlineSizeMatchesChunkSize() throws Exception {
        int cacheSize = 2 * ONE_MB;
        // Cache inline size is 1 MB, number of cache entries = 2
        int cacheInlineSize = ONE_MB;
        int segSize = 10 * ONE_MB;
        // Chunk size = pageSize << maxOrder (1 MB here)
        int pageSize = 8192;
        int maxOrder = 7;
        // Expected used memory = 2 MB, each of the 2 cache entries will reserve 1 MB
        int expectedUsedMemory = 2097152;
        testNoByteBufResourceLeak(cacheSize, cacheInlineSize, segSize, pageSize, maxOrder, expectedUsedMemory);
    }

    @Test
    public void testNoByteBufResourceLeaksCacheInlineSizeExceedsChunkSize() throws Exception {
        int cacheSize = 6 * ONE_MB;
        // Cache inline size is 1.5 MB, number of cache entries = 4
        int cacheInlineSize = (int) (1.5 * ONE_MB);
        int segSize = 10 * ONE_MB;
        // Chunk size = pageSize << maxOrder (1 MB here)
        int pageSize = 8192;
        int maxOrder = 7;
        // Expected used memory = 4 * 1.5 MB = 6 MB (same as cache size here)
        int expectedUsedMemory = 6 * ONE_MB;
        testNoByteBufResourceLeak(cacheSize, cacheInlineSize, segSize, pageSize, maxOrder, expectedUsedMemory);
    }

    @Test
    public void testNoByteBufResourceLeaksChunkSizeExceedsCacheInlineSize() throws Exception {
        int cacheSize = 6 * ONE_MB;
        // Cache inline size is 1.5 MB, number of cache entries = 4
        int cacheInlineSize = (int) (1.5 * ONE_MB);
        int segSize = 10 * ONE_MB;
        // Chunk size = pageSize << maxOrder (2 MB here)
        int pageSize = 8192;
        int maxOrder = 8;
        // Expected used memory = 8 MB, each of the 4 cache entries will reserve 2 MB, causing memory wastage
        int expectedUsedMemory = 8 * ONE_MB;
        testNoByteBufResourceLeak(cacheSize, cacheInlineSize, segSize, pageSize, maxOrder, expectedUsedMemory);
    }

    private void testNoByteBufResourceLeak(int cacheSize, int cacheInlineSize, int segSize, int pageSize, int maxOrder, long expectedUsedMemory) throws Exception {
        LRUCacheWithContext cache = new LRUCacheWithContext(cacheSize);
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP, String.valueOf(cacheInlineSize));
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_POOLED_BYTE_BUF_ALLOCATOR_PAGE_SIZE_PROP, String.valueOf(pageSize));
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_CACHE_POOLED_BYTE_BUF_ALLOCATOR_MAX_ORDER_PROP, String.valueOf(maxOrder));
        try (HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager()) {
            rsm.setHadoopConfiguration(hadoopConf);
            rsm.configure(configs);
            rsm.setLRUCache(cache);
            // Clear previously registered metrics during object creation
            clearKafkaMetrics();
            rsm.registerMetrics(cache);
            rsm.registerPooledByteBufAllocatorMetrics();

            Optional<Metric> chunkSize = findKafkaMetric(ALLOCATOR_CHUNK_SIZE);
            assertTrue(chunkSize.isPresent());
            // Verify the chunk size
            assertEquals(pageSize << maxOrder, ((Gauge<?>) chunkSize.get()).value());

            RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tp, Uuid.randomUuid()),
                    0, 100, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
            LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
            rsm.copyLogSegmentData(segmentMetadata, segmentData);

            verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 0, Integer.MAX_VALUE, segSize);

            Optional<Metric> usedHeapMemory = findKafkaMetric(ALLOCATOR_USED_HEAP_MEMORY);
            assertTrue(usedHeapMemory.isPresent());
            // Verify the size of used heap memory by the PooledByteBufAllocator
            // Only those ByteBufs that are currently in the cache should be alive. Everything else should be reclaimed.
            assertEquals(expectedUsedMemory, ((Gauge<?>) usedHeapMemory.get()).value());
        }
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
            metadataList.add(new RemoteLogSegmentMetadata(new RemoteLogSegmentId(tp, Uuid.randomUuid()),
                    startOffset, endOffset, timestamp, 0, timestamp, segmentSize, segmentLeaderEpochs));
        }
        return metadataList;
    }

    private void testRepeatedCacheReads(LRUCacheWithContext cache,
                                        String cacheLineSizeInBytes,
                                        int segSize,
                                        int expectedCacheHit,
                                        int expectedSegmentReadFileOpenCalls) throws Exception {
        configs.put(HDFSRemoteStorageManagerConfig.HDFS_REMOTE_READ_BYTES_PROP, cacheLineSizeInBytes);
        HDFSRemoteStorageManager rsm = new HDFSRemoteStorageManager();
        rsm.setHadoopConfiguration(hadoopConf);
        rsm.configure(configs);
        rsm.setLRUCache(cache);
        // Clear previously registered metrics during object creation
        clearKafkaMetrics();
        rsm.registerMetrics(cache);

        Uuid uuid = Uuid.randomUuid();
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                0, 100, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
        LogSegmentData segmentData = TestLogSegmentUtils.createLogSegmentData(logDir, 0, segSize, false);
        rsm.copyLogSegmentData(segmentMetadata, segmentData);

        assertEquals(0, cache.getCacheHit());
        assertEquals(0, rsm.segmentFileReadOpenCounter());
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 0, Integer.MAX_VALUE, segSize);
        assertEquals(0, cache.getCacheHit());

        // read from cache
        verifyFetchLogSegment(rsm, segmentMetadata, segmentData, 0, Integer.MAX_VALUE, segSize);
        assertEquals(expectedCacheHit, cache.getCacheHit());
        assertEquals(expectedSegmentReadFileOpenCalls, rsm.segmentFileReadOpenCounter());

        // Verify yammer metric
        Optional<Metric> hitCount = findKafkaMetric("hitCount");
        assertTrue(hitCount.isPresent());
        assertEquals(expectedCacheHit, ((Long) ((Gauge<?>) hitCount.get()).value()).intValue());
    }

    private void clearKafkaMetrics() {
        KafkaYammerMetrics.defaultRegistry().allMetrics().forEach(
            (metricName, metric) -> KafkaYammerMetrics.defaultRegistry().removeMetric(metricName));
    }

    private Optional<Metric> findKafkaMetric(String name) {
        return KafkaYammerMetrics.defaultRegistry().allMetrics()
            .entrySet()
            .stream()
            .filter(entry -> {
                MetricName metricName = entry.getKey();
                return metricName.getGroup().equals(HDFSRemoteStorageManager.class.getPackage().getName()) &&
                    metricName.getType().equals(HDFSRemoteStorageManager.class.getSimpleName()) &&
                    metricName.getName().equals(name);

            })
            .findFirst()
            .map(Map.Entry::getValue);
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
                                       int startPosition,
                                       int endPosition,
                                       int size) throws Exception {
        try (InputStream stream = rsm.fetchLogSegment(metadata, startPosition, endPosition)) {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[size]);
            SeekableByteChannel byteChannel = Files.newByteChannel(segmentData.logSegment());
            byteChannel.position(startPosition);
            byteChannel.read(buffer);
            buffer.rewind();
            assertDataEquals(buffer, stream);
            byteChannel.close();
        }
    }

    private RemoteLogSegmentMetadata verifyUpload(RemoteStorageManager rsm,
                                                  TopicIdPartition tp,
                                                  Uuid uuid,
                                                  int startOffset,
                                                  int segSize,
                                                  boolean withOptionalFiles) throws Exception {
        long bytesReadFromRemoteSoFar = ((HDFSRemoteStorageManager) rsm).bytesReadFromRemote();
        RemoteLogSegmentId id = new RemoteLogSegmentId(tp, uuid);
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(id,
                0, 100, 0, 0, 1L, segSize, Collections.singletonMap(0, 0L));
        LogSegmentData segmentData = TestLogSegmentUtils
                .createLogSegmentData(logDir, startOffset, segSize, withOptionalFiles);
        rsm.copyLogSegmentData(segmentMetadata, segmentData);
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
        try (InputStream actualStream = rsm.fetchLogSegment(metadata, 0)) {
            assertFileEquals(segmentData.logSegment().toFile(), actualStream);
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

        synchronized ByteBuf get(String path, long offset) {
            ByteBuf result = super.get(path, offset);
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
