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
package kafka.log.remote;

import kafka.cluster.Partition;
import kafka.log.UnifiedLog;
import kafka.log.remote.RemoteLogManager.RLMExpirationTask;
import kafka.server.BrokerTopicStats;
import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;

import scala.Option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for RemoteLogManager partition size metrics functionality.
 * This class focuses specifically on testing the SizeInPercent and LocalSizeInPercent metrics.
 */
public class RemoteLogManagerMetricsTest {
    
    private final Time time = new MockTime();
    private final int brokerId = 0;
    private String logDir;
    private final String clusterId = "dummyId";
    
    private BrokerTopicStats brokerTopicStats;
    private RemoteLogManager remoteLogManager;
    private TopicIdPartition leaderTopicIdPartition;
    private RemoteStorageManager remoteStorageManager;
    private RemoteLogMetadataManager remoteLogMetadataManager;
    private Metrics metrics;
    private KafkaConfig config;
    private EpochEntry epochEntry0;
    private Map<String, Uuid> topicIds;
    private UnifiedLog mockLog;

    @BeforeEach
    public void setUp() throws Exception {
        File tempDir = File.createTempFile("kafka-test", "");
        tempDir.delete();
        tempDir.mkdirs();
        logDir = tempDir.getAbsolutePath();
        
        brokerTopicStats = mock(BrokerTopicStats.class);
        remoteStorageManager = mock(RemoteStorageManager.class);
        remoteLogMetadataManager = mock(RemoteLogMetadataManager.class);
        metrics = new Metrics(time);
        leaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("Leader", 0));
        epochEntry0 = new EpochEntry(0, 0L);
        topicIds = Collections.singletonMap(leaderTopicIdPartition.topic(), leaderTopicIdPartition.topicId());
        Properties props = TestUtils.createDummyBrokerConfig();
        props.setProperty(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true");
        appendRLMConfig(props);
        config = KafkaConfig.fromProps(props);
        mockLog = mock(UnifiedLog.class);
        remoteLogManager = new RemoteLogManager(
            config.remoteLogManagerConfig(),
            brokerId,
            logDir,
            clusterId,
            time,
            tp -> Optional.of(mockLog),
            (topicPartition, offset) -> { },
            brokerTopicStats,
            metrics
        ) {
            public RemoteStorageManager createRemoteStorageManager() {
                return remoteStorageManager;
            }
            public RemoteLogMetadataManager createRemoteLogMetadataManager() {
                return remoteLogMetadataManager;
            }
        };
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (remoteLogManager != null) {
            remoteLogManager.close();
        }
        if (metrics != null) {
            metrics.close();
        }
    }

    private RLMExpirationTask setupExpirationTaskForPartitionSizeMetricTest() throws RemoteStorageException {
        RLMExpirationTask task = remoteLogManager.new RLMExpirationTask(leaderTopicIdPartition);
        
        // Mock remote log segments for size calculation
        List<RemoteLogSegmentMetadata> metadataList = listOfRemoteLogSegmentMetadata(
            leaderTopicIdPartition, 10, 100, 1024, 
            Collections.singletonList(epochEntry0), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
            .thenReturn(metadataList.iterator());
        return task;
    }

    private List<RemoteLogSegmentMetadata> listOfRemoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                          int segmentCount,
                                                                          long baseOffset,
                                                                          int segmentSize,
                                                                          List<EpochEntry> epochEntries,
                                                                          RemoteLogSegmentState state) {
        List<RemoteLogSegmentMetadata> metadataList = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            long startOffset = baseOffset + (i * 10);
            long endOffset = startOffset + 9;
            Map<Integer, Long> segmentLeaderEpochs = new HashMap<>();
            for (EpochEntry entry : epochEntries) {
                segmentLeaderEpochs.put(entry.epoch, entry.startOffset);
            }
            RemoteLogSegmentId segmentId = RemoteLogSegmentId.generateNew(topicIdPartition);
            RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(
                segmentId,
                startOffset,
                endOffset,
                time.milliseconds(),
                brokerId,
                time.milliseconds(),
                segmentSize,
                Optional.empty(),
                state,
                segmentLeaderEpochs
            );
            metadataList.add(metadata);
        }
        return metadataList;
    }

    @Test
    public void testLocalAndRemotePartitionSizeInPercentMetrics() throws RemoteStorageException {
        RLMExpirationTask task = setupExpirationTaskForPartitionSizeMetricTest();
        
        TreeMap<Integer, Long> epochEntries = new TreeMap<>();
        epochEntries.put(epochEntry0.epoch, epochEntry0.startOffset);
        
        // Test case 1: Testing SizeInPercent metric (standard retention scenario)
        task.buildRetentionSizeData(12288, 100, 1000, epochEntries, 6144);

        // Each remote log segment size is 1024. There are 10 remote-log-segments. Total remote size = 10 * 1024 = 10240
        // ((100 + 10240) * 100) / 12288 = 84%
        // Verify the task's internal metric value is set correctly
        assertEquals(84, task.sizeInPercent());
        
        // Test case 2: Testing LocalSizeInPercent metric (local retention scenario)
        // localRetentionBytes = 200, onlyLocalLogSegmentsSize = 100, so percentage = (100 * 100) / 200 = 50%
        task.buildRetentionSizeData(12288, 100, 1000, epochEntries, 200);
        assertEquals(50, task.localSizeInPercent());
    }

    @Test
    public void testSizeInPercentMetricsUpdated() throws RemoteStorageException {
        RLMExpirationTask task = setupExpirationTaskForPartitionSizeMetricTest();

        // Set up metrics with non-zero values
        TreeMap<Integer, Long> epochEntries = new TreeMap<>();
        epochEntries.put(epochEntry0.epoch, epochEntry0.startOffset);
        task.buildRetentionSizeData(12288, 100, 1000, epochEntries, 6144);
        
        // Verify task internal metric values are set correctly
        assertEquals(84, task.sizeInPercent());
        // localRetentionBytes = 6144, onlyLocalLogSegmentsSize = 100, so percentage = (100 * 100) / 6144 = 1%
        assertEquals(1, task.localSizeInPercent());
        
        // Test with different retention values - create a new task to avoid state issues
        RLMExpirationTask task2 = setupExpirationTaskForPartitionSizeMetricTest();
        task2.buildRetentionSizeData(20000, 200, 2000, epochEntries, 400);
        
        // Verify updated values
        // Total size = 200 + 10240 = 10440, percentage = (10440 * 100) / 20000 = 52%
        assertEquals(52, task2.sizeInPercent());
        // Local percentage = (200 * 100) / 400 = 50%
        assertEquals(50, task2.localSizeInPercent());
    }

    @Test
    public void testSizeInPercentMetricsWithZeroRetention() throws RemoteStorageException {
        RLMExpirationTask task = setupExpirationTaskForPartitionSizeMetricTest();
        TreeMap<Integer, Long> epochEntries = new TreeMap<>();
        epochEntries.put(epochEntry0.epoch, epochEntry0.startOffset);
        
        task.buildRetentionSizeData(0, 100, 1000, epochEntries, 0);
        
        // Should be 0% when retention sizes are 0
        assertEquals(0, task.sizeInPercent());
        assertEquals(0, task.localSizeInPercent());
    }

    @Test
    public void testSizeInPercentMetricsWithNegativeRetention() throws RemoteStorageException {
        RLMExpirationTask task = setupExpirationTaskForPartitionSizeMetricTest();
        TreeMap<Integer, Long> epochEntries = new TreeMap<>();
        epochEntries.put(epochEntry0.epoch, epochEntry0.startOffset);
        
        // Test with negative retention (disabled)
            // Should return empty Optional when retention is disabled (-1)
        Optional<RemoteLogManager.RetentionSizeData> result = task.buildRetentionSizeData(-1, 100, 1000, epochEntries, -1);
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testSizeInPercentMetricsTaskCancellation() throws RemoteStorageException {
        RLMExpirationTask task = setupExpirationTaskForPartitionSizeMetricTest();
        TreeMap<Integer, Long> epochEntries = new TreeMap<>();
        epochEntries.put(epochEntry0.epoch, epochEntry0.startOffset);
        task.buildRetentionSizeData(12288, 100, 1000, epochEntries, 6144);
        
        // Verify initial metrics are set
        assertEquals(84, task.sizeInPercent());
        
        // Cancel the task
        task.cancel();
        
        // Verify metrics are reset to 0 on cancellation
        assertEquals(0, task.sizeInPercent());
        assertEquals(0, task.localSizeInPercent());
    }

    private void appendRLMConfig(Properties props) {
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true);
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, NoOpRemoteStorageManager.class.getName());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, NoOpRemoteLogMetadataManager.class.getName());
    }

    private Partition mockPartition(TopicIdPartition topicIdPartition) {
        TopicPartition tp = topicIdPartition.topicPartition();
        Partition partition = mock(Partition.class);
        UnifiedLog log = mock(UnifiedLog.class);
        when(partition.topicPartition()).thenReturn(tp);
        when(partition.topic()).thenReturn(tp.topic());
        when(log.remoteLogEnabled()).thenReturn(true);
        when(partition.log()).thenReturn(Option.apply(log));
        when(log.config()).thenReturn(new LogConfig(new Properties()));
        return partition;
    }

    @Test
    public void testRLMExpirationTaskMetricsLifecycle() throws IOException, RemoteStorageException {
        RLMExpirationTask task = setupExpirationTaskForPartitionSizeMetricTest();
        
        assertEquals(0, task.sizeInPercent());
        assertEquals(0, task.localSizeInPercent());
        
        TreeMap<Integer, Long> epochEntries = new TreeMap<>();
        epochEntries.put(epochEntry0.epoch, epochEntry0.startOffset);
        task.buildRetentionSizeData(12288, 100, 1000, epochEntries, 6144);
        
        assertEquals(84, task.sizeInPercent());
        assertEquals(1, task.localSizeInPercent());
        
        task.cancel();
        assertEquals(0, task.sizeInPercent());
        assertEquals(0, task.localSizeInPercent());
    }

    @Test
    public void testMetricsOnLeaderToFollowerTransition() throws RemoteStorageException {
        remoteLogManager.startup();
        
        // Set up remote log segment metadata for the test
        List<RemoteLogSegmentMetadata> metadataList = listOfRemoteLogSegmentMetadata(
            leaderTopicIdPartition, 10, 100, 1024, 
            Collections.singletonList(epochEntry0), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        
        // Mock the metadata manager to return our test data
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
            .thenReturn(metadataList.iterator());
        
        // Initially start as leader
        Partition mockLeaderPartition = mockPartition(leaderTopicIdPartition);
        remoteLogManager.onLeadershipChange(
            Collections.singleton(mockLeaderPartition), 
            Collections.emptySet(), 
            topicIds
        );
        
        // Verify leader task is created and has access to metrics
        RLMExpirationTask leaderTask = remoteLogManager.rlmExpirationTask(leaderTopicIdPartition);
        assertNotNull(leaderTask);
        
        // Build retention data as leader
        TreeMap<Integer, Long> epochEntries = new TreeMap<>();
        epochEntries.put(epochEntry0.epoch, epochEntry0.startOffset);
        leaderTask.buildRetentionSizeData(12288, 100, 1000, epochEntries, 6144);
        assertEquals(84, leaderTask.sizeInPercent());
        assertEquals(1, leaderTask.localSizeInPercent());
        
        // Simulate leadership change from leader to follower
        remoteLogManager.onLeadershipChange(
            Collections.emptySet(),
            Collections.singleton(mockLeaderPartition),
            topicIds
        );
        
        // Verify leader task is cancelled and metrics are reset
        assertNull(remoteLogManager.rlmExpirationTask(leaderTopicIdPartition));
    }

    @Test
    public void testMetricsOnFollowerToLeaderTransition() throws RemoteStorageException {
        remoteLogManager.startup();
        
        // Set up remote log segment metadata for the test
        List<RemoteLogSegmentMetadata> metadataList = listOfRemoteLogSegmentMetadata(
            leaderTopicIdPartition, 10, 100, 1024, 
            Collections.singletonList(epochEntry0), RemoteLogSegmentState.COPY_SEGMENT_FINISHED);
        
        // Mock the metadata manager to return our test data
        when(remoteLogMetadataManager.listRemoteLogSegments(leaderTopicIdPartition, 0))
            .thenReturn(metadataList.iterator());
        
        // Initially start as follower
        Partition mockFollowerPartition = mockPartition(leaderTopicIdPartition);
        remoteLogManager.onLeadershipChange(
            Collections.emptySet(),
            Collections.singleton(mockFollowerPartition),
            topicIds
        );
        
        // Verify no leader task exists initially
        assertNull(remoteLogManager.rlmExpirationTask(leaderTopicIdPartition));
        
        // Simulate leadership change from follower to leader
        remoteLogManager.onLeadershipChange(
            Collections.singleton(mockFollowerPartition),
            Collections.emptySet(),
            topicIds
        );
        
        // Verify leader task is created
        RLMExpirationTask leaderTask = remoteLogManager.rlmExpirationTask(leaderTopicIdPartition);
        assertNotNull(leaderTask);
        
        // Initially metrics should be 0
        assertEquals(0, leaderTask.sizeInPercent());
        assertEquals(0, leaderTask.localSizeInPercent());
        
        // Build retention data as new leader
        TreeMap<Integer, Long> epochEntries = new TreeMap<>();
        epochEntries.put(epochEntry0.epoch, epochEntry0.startOffset);
        leaderTask.buildRetentionSizeData(12288, 100, 1000, epochEntries, 6144);
        
        // Verify metrics are properly set after becoming leader
        assertEquals(84, leaderTask.sizeInPercent());
        assertEquals(1, leaderTask.localSizeInPercent());
    }
}
