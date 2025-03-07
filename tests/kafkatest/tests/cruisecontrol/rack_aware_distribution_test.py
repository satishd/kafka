# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from collections import Counter

from ducktape.mark import parametrize

from kafkatest.tests.cruisecontrol.cruise_control_test import CruiseControlTest
from kafkatest.tests.cruisecontrol.cruise_control_test import fetch_topic_partition_proposal


def verify_is_rack_aware_distribution(per_node_server_prop_overrides, replica_assignment):
    replicas_by_rack = Counter(per_node_server_prop_overrides[broker][0][1] for broker in replica_assignment)
    max_replicas_in_a_rack = max(replicas_by_rack.values())
    min_replicas_in_a_rack = min(replicas_by_rack.values())

    if max_replicas_in_a_rack > 1 and (
            len(replicas_by_rack) < 3 or min_replicas_in_a_rack < max_replicas_in_a_rack - 1):
        return False
    return True


def verify_rack_aware_distribution_for_proposal(cruise_control, topic_name, partition,
                                                per_node_server_prop_overrides):
    topic_partition_proposal = fetch_topic_partition_proposal(cruise_control, topic_name, partition)
    new_replica_assignment = topic_partition_proposal[partition]["newReplicas"]
    return verify_is_rack_aware_distribution(per_node_server_prop_overrides=per_node_server_prop_overrides,
                                             replica_assignment=new_replica_assignment)


def verify_rack_awareness_is_intact(cruise_control, topic_name, partition, per_node_server_prop_overrides):
    topic_partition_proposal = fetch_topic_partition_proposal(cruise_control, topic_name, partition)
    if len(topic_partition_proposal) == 0:
        return True
    new_replica_assignment = topic_partition_proposal[partition]["newReplicas"]
    return verify_is_rack_aware_distribution(per_node_server_prop_overrides=per_node_server_prop_overrides,
                                             replica_assignment=new_replica_assignment)


class RackAwareDistributionTest(CruiseControlTest):
    # 4 brokers on rack-a, 2 brokers on rack-b, 1 broker on rack-c
    PER_NODE_SERVER_PROP_OVERRIDES = {
        1: [("broker.rack", "rack-a")],
        2: [("broker.rack", "rack-b")],
        3: [("broker.rack", "rack-c")],
        4: [("broker.rack", "rack-a")],
        5: [("broker.rack", "rack-a")],
        6: [("broker.rack", "rack-a")],
        7: [("broker.rack", "rack-b")]
    }

    def __init__(self, test_context):
        super(RackAwareDistributionTest, self).__init__(test_context, num_nodes=7,
                                                        per_node_server_prop_overrides=self.PER_NODE_SERVER_PROP_OVERRIDES)
        self.topic_name = "test-topic-name"
        self.partition = 0

    @parametrize(replication_factor=2, initial_replica_assignment=[1, 4])
    @parametrize(replication_factor=3, initial_replica_assignment=[1, 4, 5])
    @parametrize(replication_factor=3, initial_replica_assignment=[1, 2, 4])
    @parametrize(replication_factor=4, initial_replica_assignment=[1, 4, 5, 6])
    @parametrize(replication_factor=4, initial_replica_assignment=[1, 2, 4, 5])
    @parametrize(replication_factor=4, initial_replica_assignment=[1, 2, 4, 7])
    def test_rack_aware_distribution_unbalanced(self, replication_factor, initial_replica_assignment):
        self.kafka.start()
        self._create_topic(self.topic_name, replication_factor=replication_factor, num_partitions=1,
                           replica_assignment=":".join(map(str, initial_replica_assignment)))
        # Verify the existing replica assignment does not satisfy rack-aware distribution
        assert not verify_is_rack_aware_distribution(
            per_node_server_prop_overrides=self.PER_NODE_SERVER_PROP_OVERRIDES,
            replica_assignment=initial_replica_assignment)
        # Start cruise control and wait until proposals are generated
        self._start_cruise_control_and_wait_until_ready()
        # Verify the new replica assignment satisfies rack-aware distribution
        assert verify_rack_aware_distribution_for_proposal(self.cruise_control, self.topic_name, self.partition,
                                                           self.PER_NODE_SERVER_PROP_OVERRIDES)
        self._delete_topics(self.topic_name)

    @parametrize(replication_factor=2, initial_replica_assignment=[1, 2])
    @parametrize(replication_factor=3, initial_replica_assignment=[1, 2, 3])
    @parametrize(replication_factor=4, initial_replica_assignment=[1, 2, 3, 4])
    def test_rack_aware_distribution_balanced(self, replication_factor, initial_replica_assignment):
        self.kafka.start()
        self._create_topic(self.topic_name, replication_factor=replication_factor, num_partitions=1,
                           replica_assignment=":".join(map(str, initial_replica_assignment)))
        # Verify the existing replica assignment satisfies rack-aware distribution
        assert verify_is_rack_aware_distribution(per_node_server_prop_overrides=self.PER_NODE_SERVER_PROP_OVERRIDES,
                                                 replica_assignment=initial_replica_assignment)
        # Start cruise control and wait until proposals are generated
        self._start_cruise_control_and_wait_until_ready()
        # Verify the new replica assignment continues to satisfy rack-aware distribution
        verify_rack_awareness_is_intact(self.cruise_control, self.topic_name, self.partition,
                                        self.PER_NODE_SERVER_PROP_OVERRIDES)
        self._delete_topics(self.topic_name)
