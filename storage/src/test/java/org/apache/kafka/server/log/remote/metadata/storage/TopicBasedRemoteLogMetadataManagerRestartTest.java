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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import scala.jdk.CollectionConverters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.LOG_DIR;

public class TopicBasedRemoteLogMetadataManagerRestartTest {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManagerRestartTest.class);

    private static final int SEG_SIZE = 1024 * 1024;

    private final Time time = new MockTime(1);
    private String logDir = TestUtils.tempDirectory("_rlmm_segs_").getAbsolutePath();

    private TopicBasedRemoteLogMetadataManagerHarness remoteLogMetadataManagerHarness;

    @BeforeEach
    public void setup() {
        // Start the cluster and initialize TopicBasedRemoteLogMetadataManager.
        startTopicBasedRemoteLogMetadataManagerHarness();
    }

    private void startTopicBasedRemoteLogMetadataManagerHarness() {
        remoteLogMetadataManagerHarness = new TopicBasedRemoteLogMetadataManagerHarness() {
            protected Map<String, Object> overrideRemoteLogMetadataManagerProps() {
                Map<String, Object> props = new HashMap<>();
                props.put(LOG_DIR, logDir);
                return props;
            }
        };
        remoteLogMetadataManagerHarness.initialize(Collections.emptySet());
    }

    @AfterEach
    public void teardown() throws IOException {
        stopTopicBasedRemoteLogMetadataManagerHarness();
    }

    private void stopTopicBasedRemoteLogMetadataManagerHarness() throws IOException {
        if (remoteLogMetadataManagerHarness != null) {
            remoteLogMetadataManagerHarness.close();
        }
    }

    public TopicBasedRemoteLogMetadataManager topicBasedRlmm() {
        return remoteLogMetadataManagerHarness.topicBasedRlmm();
    }

    @Test
    public void testRLMMAPIsAfterRestart() throws Exception {
        // Create topics.
        String leaderTopic = "new-leader";
        HashMap<Object, Seq<Object>> assignedLeaderTopicReplicas = new HashMap<>();
        List<Object> leaderTopicReplicas = new ArrayList<>();
        // Set broker id 0 as the first entry which is taken as the leader.
        leaderTopicReplicas.add(0);
        leaderTopicReplicas.add(1);
        leaderTopicReplicas.add(2);
        assignedLeaderTopicReplicas.put(0, CollectionConverters.ListHasAsScala(leaderTopicReplicas).asScala());
        remoteLogMetadataManagerHarness.createTopic(leaderTopic, CollectionConverters.MapHasAsScala(assignedLeaderTopicReplicas).asScala());

        String followerTopic = "new-follower";
        HashMap<Object, Seq<Object>> assignedFollowerTopicReplicas = new HashMap<>();
        List<Object> followerTopicReplicas = new ArrayList<>();
        // Set broker id 1 as the first entry which is taken as the leader.
        followerTopicReplicas.add(1);
        followerTopicReplicas.add(2);
        followerTopicReplicas.add(0);
        assignedFollowerTopicReplicas.put(0, CollectionConverters.ListHasAsScala(followerTopicReplicas).asScala());
        remoteLogMetadataManagerHarness.createTopic(followerTopic, CollectionConverters.MapHasAsScala(assignedFollowerTopicReplicas).asScala());

        final TopicIdPartition leaderTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(leaderTopic, 0));
        final TopicIdPartition followerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(followerTopic, 0));

        // Register these partitions to RLMM.
        topicBasedRlmm().onPartitionLeadershipChanges(Collections.singleton(leaderTopicIdPartition), Collections.singleton(followerTopicIdPartition));

        // Add segments for these partitions but they are not available as they have not yet been subscribed.
        RemoteLogSegmentMetadata leaderSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(leaderTopicIdPartition, Uuid.randomUuid()),
                                                                                      0, 100, -1L, 0,
                                                                                      time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        topicBasedRlmm().addRemoteLogSegmentMetadata(leaderSegmentMetadata);

        RemoteLogSegmentMetadata followerSegmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(followerTopicIdPartition, Uuid.randomUuid()),
                                                                                        0, 100, -1L, 0,
                                                                                        time.milliseconds(), SEG_SIZE, Collections.singletonMap(0, 0L));
        topicBasedRlmm().addRemoteLogSegmentMetadata(followerSegmentMetadata);


        // Stop servers and close TopicBasedRemoteLogMetadataManagerHarness.
        stopTopicBasedRemoteLogMetadataManagerHarness();

        // Start servers and TopicBasedRemoteLogMetadataManagerHarness.
        startTopicBasedRemoteLogMetadataManagerHarness();

        // Register these partitions to RLMM.
        topicBasedRlmm().onPartitionLeadershipChanges(Collections.singleton(leaderTopicIdPartition), Collections.singleton(followerTopicIdPartition));

        Assertions.assertTrue(TestUtils.sameElementsWithoutOrder(Collections.singleton(leaderSegmentMetadata).iterator(),
                                                                 topicBasedRlmm().listRemoteLogSegments(leaderTopicIdPartition)));
        Assertions.assertTrue(TestUtils.sameElementsWithoutOrder(Collections.singleton(followerSegmentMetadata).iterator(),
                                                                 topicBasedRlmm().listRemoteLogSegments(followerTopicIdPartition)));
    }

}
