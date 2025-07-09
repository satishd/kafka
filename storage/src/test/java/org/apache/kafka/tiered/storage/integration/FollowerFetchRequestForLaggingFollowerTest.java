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

import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.TieredStorageTestHarness;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;
import org.apache.kafka.tiered.storage.specs.RemoteFetchCount;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

/**
 * Test Case:
 *
 *    Given a cluster of brokers {B0, B1} and a topic-partition Ta-p0.
 *    The purpose of this test is to verify that a non-empty broker on becoming leader for a remote storage enabled topic
 *    is able to serve requests for all the valid offsets
 *
 *    - Two segments were uploaded to the remote storage for Ta-p0 and the log-start-offset doesn't start with zero;
 *    - The follower goes down for some time and another broker is reassigned replica during the period
 *    - The follower comes back with stale data and is reassigned the replica back
 *
 *    Acceptance:
 *    -----------
 *    - The follower on gaining leadership should be able to retrieve records for all valid offsets
 */
public class FollowerFetchRequestForLaggingFollowerTest extends TieredStorageTestHarness {
    @Override
    public int brokerCount() {
        return 3;
    }

    @Override
    protected void writeTestSpecifications(TieredStorageTestBuilder builder) {
        final Integer broker0 = 0;
        final Integer broker1 = 1;
        final Integer broker2 = 2;
        final String topicA = "topicA";
        final Integer p0 = 0;
        final Integer partitionCount = 1;
        final Integer replicationFactor = 2;
        final Integer maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = true;
        final Integer batchSize = 1;
        final Map<Integer, List<Integer>> assignment = mkMap(
            mkEntry(p0, Arrays.asList(broker0, broker1))
        );

        builder
            .createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment, assignment,
                enableRemoteLogStorage)
            .expectLeader(topicA, p0, broker0, false)
            // Send 3 records to partition 0, offload the first two segments to remote storage and ensure the uploaded
            // segments are deleted locally
            .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k1", "v1"))
            .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k2", "v2"))
            .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 2L)
            .produce(topicA, p0, new KeyValueSpec("k1", "v1"), new KeyValueSpec("k2", "v2"), new KeyValueSpec("k3", "v3"))
            .withBatchSize(topicA, p0, batchSize)
            // Delete the first record, so start offset becomes non-zero.
            // We now end up with one record in remote tier and one locally
            .deleteRecords(topicA, p0, 1L)
            // Bring down the follower
            .stop(broker1)
            // Reassign partition to another broker
            .reassignReplica(topicA, p0, Arrays.asList(broker0, broker2))
            .expectLeader(topicA, p0, broker0, true)
            // Send 3 more records, offload the earliest 3 records to remote storage and ensure the uploaded segments are
            // deleted locally
            // TODO - to uncomment the below requires a fix in the test framework
            // .expectSegmentToBeOffloaded(broker0, topicA, p0, 2, new KeyValueSpec("k3", "v3"))
            .expectSegmentToBeOffloaded(broker0, topicA, p0, 3, new KeyValueSpec("k4", "v4"))
            .expectSegmentToBeOffloaded(broker0, topicA, p0, 4, new KeyValueSpec("k5", "v5"))
            // Ensure the uploaded segments are deleted locally
            .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 5L)
            .produce(topicA, p0, new KeyValueSpec("k4", "v4"), new KeyValueSpec("k5", "v5"), new KeyValueSpec("k6", "v6"))
            .withBatchSize(topicA, p0, batchSize)
            // Delete all records before offset 4, so that broker1 would encounter Offset Out of Range error
            .deleteRecords(topicA, p0, 4L)
            // Start broker1 whose end offset is 3
            .start(broker1)
            // Reassign the replica back to broker1
            .reassignReplica(topicA, p0, Arrays.asList(broker0, broker1))
            // Ensure broker1 can become a leader
            .expectLeader(topicA, p0, broker1, true)
            // We should be able to consume all records for the topic with the new broker as leader
            // We should be fetching one record from remote storage
            .consume(topicA, p0, 0L, 2, 1)
            // Ensure we are only making one remote fetch request
            .expectFetchFromTieredStorage(broker1, topicA, p0, new RemoteFetchCount(1));
    }
}
