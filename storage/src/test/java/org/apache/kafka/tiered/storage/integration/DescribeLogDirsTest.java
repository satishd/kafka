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
package org.apache.kafka.tiered.storage.integration;

import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.TieredStorageTestHarness;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DescribeLogDirsTest extends TieredStorageTestHarness {

    private final String topic = "topicB";
    private final int broker0 = 0;
    private final int broker1 = 1;
    private final int partitionCount = 1;
    private final Set<Integer> brokerIds = new HashSet<>(Arrays.asList(broker1, broker0));

    @Override
    public int brokerCount() {
        return 2;
    }

    @Override
    protected void writeTestSpecifications(TieredStorageTestBuilder builder) {
        final int p0 = 0;
        final int replicationFactor = 2;
        final int maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = true;

        builder
                // create topicB with 1 partition and 2 RF
                .createTopic(topic, partitionCount, replicationFactor, maxBatchCountPerSegment,
                        mkMap(mkEntry(p0, Arrays.asList(broker1, broker0))), enableRemoteLogStorage)
                // send records to partition 0
                .expectSegmentToBeOffloaded(broker1, topic, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker1, topic, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topic, p0, 2L)
                .produce(topic, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // Ensure that the follower can become a leader
                .expectLeader(topic, p0, broker0, true)
                // describe log directories including remoteInfo
                .describeLogDirs(brokerIds, true, predicate(true))
                // describe log directories without remoteInfo
                .describeLogDirs(brokerIds, false, predicate(false));
    }

    private Predicate<Map<Integer, Map<String, LogDirDescription>>> predicate(boolean withRemoteInfo) {
        return describeLogDirsResult -> {
            for (Integer brokerId : brokerIds) {
                Map<String, LogDirDescription> logDirDescriptionMap = describeLogDirsResult.get(brokerId);
                for (int partition = 0; partition < partitionCount; partition++) {
                    boolean found = false;
                    TopicPartition topicPartition = new TopicPartition(topic, partition);
                    // there can be multiple log directories configured on the server
                    for (LogDirDescription logDirDescription : logDirDescriptionMap.values()) {
                        assertNotNull(logDirDescription);
                        assertNull(logDirDescription.error());
                        Map<TopicPartition, ReplicaInfo> replicaInfoMap = logDirDescription.replicaInfos();
                        if (!replicaInfoMap.containsKey(topicPartition)) {
                            continue;
                        }
                        found = true;
                        ReplicaInfo replicaInfo = replicaInfoMap.get(topicPartition);
                        assertNotNull(replicaInfo);
                        assertTrue(replicaInfo.size() > 0);
                        if (withRemoteInfo) {
                            assertTrue(replicaInfo.remoteLogSize() > 0);
                            assertNotEquals(-1, replicaInfo.onlyLocalLogSize());
                        } else {
                            assertEquals(-1, replicaInfo.remoteLogSize());
                            assertEquals(-1, replicaInfo.onlyLocalLogSize());
                        }
                    }
                    assertTrue(found);
                }

            }
            return true;
        };
    }
}
