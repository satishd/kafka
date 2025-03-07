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

from kafkatest.tests.cruisecontrol.cruise_control_test import CruiseControlTest
from kafkatest.tests.cruisecontrol.cruise_control_test import fetch_topic_partition_proposal


class PodAndRackAwareDistributionTest(CruiseControlTest):
    PER_NODE_SERVER_PROP_OVERRIDES = {
        1: [("broker.pod", "pod1"), ("broker.rack", "rack-a")],
        2: [("broker.pod", "pod1"), ("broker.rack", "rack-b")],
        3: [("broker.pod", "pod1"), ("broker.rack", "rack-a")],
        4: [("broker.pod", "pod1"), ("broker.rack", "rack-b")],
        5: [("broker.pod", "pod2"), ("broker.rack", "rack-a")],
        6: [("broker.pod", "pod2"), ("broker.rack", "rack-b")],
        7: [("broker.pod", "pod2"), ("broker.rack", "rack-a")],
        8: [("broker.pod", "pod2"), ("broker.rack", "rack-b")]
    }

    def __init__(self, test_context):
        super(PodAndRackAwareDistributionTest, self).__init__(test_context, num_nodes=8,
                                                              per_node_server_prop_overrides=self.PER_NODE_SERVER_PROP_OVERRIDES)
        self.topic_name = "test-topic-name"
        self.pod1_partition = 0
        self.pod2_partition = 1

    def test_rack_aware_distribution_unbalanced(self):
        self.kafka.start()
        initial_replica_assignment = "1:3,6:8"
        self._create_topic(self.topic_name, replication_factor=2, num_partitions=2,
                           replica_assignment=initial_replica_assignment)
        # Start cruise control and wait until proposals are generated
        self._start_cruise_control_and_wait_until_ready()

        self.verify_partition_proposal(self.pod1_partition, "pod1")
        self.verify_partition_proposal(self.pod2_partition, "pod2")

        self._delete_topics(self.topic_name)

    def test_rack_aware_distribution_balanced(self):
        self.kafka.start()
        initial_replica_assignment = "1:2,5:6"
        self._create_topic(self.topic_name, replication_factor=2, num_partitions=2,
                           replica_assignment=initial_replica_assignment)
        # Start cruise control and wait until proposals are generated
        self._start_cruise_control_and_wait_until_ready()

        if len(fetch_topic_partition_proposal(self.cruise_control, self.topic_name, self.pod1_partition)) > 0:
            self.verify_partition_proposal(self.pod1_partition, "pod1")

        if len(fetch_topic_partition_proposal(self.cruise_control, self.topic_name, self.pod2_partition)) > 0:
            self.verify_partition_proposal(self.pod2_partition, "pod2")

        self._delete_topics(self.topic_name)

    def verify_partition_proposal(self, partition, expected_pod):
        proposal = fetch_topic_partition_proposal(self.cruise_control, self.topic_name, partition)
        self.logger.info("Proposal for partition %s: %s", partition, proposal)
        new_replicas = proposal[0]["newReplicas"]
        pods = {self.PER_NODE_SERVER_PROP_OVERRIDES[broker][0][1] for broker in new_replicas}
        racks = {self.PER_NODE_SERVER_PROP_OVERRIDES[broker][1][1] for broker in new_replicas}
        assert len(pods) == 1 and pods.pop() == expected_pod
        assert len(racks) == 2