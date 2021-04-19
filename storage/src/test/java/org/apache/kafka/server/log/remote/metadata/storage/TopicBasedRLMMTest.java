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

import kafka.api.IntegrationTestHarness;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRLMMConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRLMMConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRLMMConfig.REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP;

/**
 * This will be mostly mock tests to test out whether the respective delegated methods are invoked.
 */
public class TopicBasedRLMMTest extends IntegrationTestHarness {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRLMMTest.class);
    private static final Random RANDOM = new Random();
    private static final TopicIdPartition TP0 = new TopicIdPartition(Uuid.randomUuid(),
                                                                     new TopicPartition("foo", 0));
    private static final int SEG_SIZE = 1024 * 1024;

    private static final int METADATA_TOPIC_PARTITIONS_COUNT = 3;
    private static final int METADATA_TOPIC_REPLICATION_FACTOR = 2;
    private static final long METADATA_TOPIC_RETENTION_MS = 24 * 60 * 60 * 1000L;

    private final Time time = new MockTime(1);

    @Override
    public Properties clientSecurityProps(String certAlias) {
        return new Properties();
    }

    @BeforeEach
    public void setup() {
        Properties topicConfigs = new Properties();
        topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(METADATA_TOPIC_RETENTION_MS));
        TestUtils.createTopic(zkClient(), TopicBasedRLMMConfig.REMOTE_LOG_METADATA_TOPIC_NAME, METADATA_TOPIC_PARTITIONS_COUNT,
                              METADATA_TOPIC_REPLICATION_FACTOR, servers(), topicConfigs);
    }

    @Test
    public void testRemoteLogSegmentMetadataLifeCycle() throws Exception {
        try (TopicBasedRLMM rlmm = new TopicBasedRLMM()) {
            rlmm.setTime(time);

            Map<String, Object> configs = new HashMap<>();
            configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList());
            configs.put("broker.id", 0);
            configs.put(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP, METADATA_TOPIC_PARTITIONS_COUNT);
            configs.put(REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, METADATA_TOPIC_REPLICATION_FACTOR);
            configs.put(REMOTE_LOG_METADATA_TOPIC_RETENTION_MILLIS_PROP, METADATA_TOPIC_REPLICATION_FACTOR);
            rlmm.configure(configs);
            rlmm.waitUntilInitialized(120_000);
            rlmm.onPartitionLeadershipChanges(Collections.singleton(TP0), Collections.emptySet());

            RemoteLogSegmentLifeCycleTest remoteLogSegmentLifeCycleTest = new RemoteLogSegmentLifeCycleTest(TP0) {
                @Override
                protected void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate segmentMetadataUpdate) throws RemoteStorageException {
                    rlmm.updateRemoteLogSegmentMetadata(segmentMetadataUpdate);
                }

                @Override
                protected Optional<Long> highestOffsetForEpoch(TopicIdPartition topicIdPartition,
                                                               int epoch) throws RemoteStorageException {
                    return rlmm.highestOffsetForEpoch(topicIdPartition, epoch);
                }

                @Override
                protected Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                                      int leaderEpoch,
                                                                                      long offset) throws RemoteStorageException {
                    return rlmm.remoteLogSegmentMetadata(topicIdPartition, leaderEpoch, offset);
                }

                @Override
                protected void addCopyInProgressSegment(RemoteLogSegmentMetadata segmentMetadata) throws RemoteStorageException {
                    rlmm.addRemoteLogSegmentMetadata(segmentMetadata);
                }
            };
            remoteLogSegmentLifeCycleTest.doTestRemoteLogSegmentLifeCycle();
        }
    }

    private int randomBrokerId() {
        return RANDOM.nextInt(brokerCount());
    }

    private RemoteLogSegmentMetadata createSegmentUpdateWithState(TopicBasedRLMM rlmm,
                                                                  Map<Integer, Long> segmentLeaderEpochs,
                                                                  long startOffset,
                                                                  long endOffset,
                                                                  RemoteLogSegmentState state)
            throws RemoteStorageException {
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, Uuid.randomUuid());
        RemoteLogSegmentMetadata segmentMetadata = new RemoteLogSegmentMetadata(segmentId, startOffset, endOffset, -1L,
                                                                                randomBrokerId(), time.milliseconds(),
                                                                                SEG_SIZE, segmentLeaderEpochs);
        rlmm.addRemoteLogSegmentMetadata(segmentMetadata);

        RemoteLogSegmentMetadataUpdate segMetadataUpdate = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(), state, randomBrokerId());
        rlmm.updateRemoteLogSegmentMetadata(segMetadataUpdate);

        return segmentMetadata.createWithUpdates(segMetadataUpdate);
    }

    @Override
    public int brokerCount() {
        return 3;
    }

    private static class EpochOffset {
        final int epoch;
        final long offset;

        private EpochOffset(int epoch,
                            long offset) {
            this.epoch = epoch;
            this.offset = offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EpochOffset that = (EpochOffset) o;
            return epoch == that.epoch && offset == that.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(epoch, offset);
        }

        @Override
        public String toString() {
            return "EpochOffset{" +
                    "epoch=" + epoch +
                    ", offset=" + offset +
                    '}';
        }
    }
}
