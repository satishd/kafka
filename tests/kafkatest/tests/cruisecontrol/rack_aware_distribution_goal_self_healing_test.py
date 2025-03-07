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

from ducktape.utils.util import wait_until
from kafkatest.tests.cruisecontrol.cruise_control_test import CruiseControlTest
from kafkatest.tests.cruisecontrol.rack_aware_distribution_test import verify_is_rack_aware_distribution


class RackAwareDistributionGoalSelfHealingTest(CruiseControlTest):
    PER_NODE_SERVER_PROP_OVERRIDES = {
        1: [("broker.rack", "rack-a")],
        2: [("broker.rack", "rack-b")],
        3: [("broker.rack", "rack-c")],
        4: [("broker.rack", "rack-a")]
    }

    def __init__(self, test_context):
        super(RackAwareDistributionGoalSelfHealingTest, self).__init__(test_context, num_nodes=4,
                                                                       per_node_server_prop_overrides=self.PER_NODE_SERVER_PROP_OVERRIDES)
        self.topic_name = "test-topic-name"
        self.partition = 0

    def test_self_healing(self):
        def is_rack_aware_distribution():
            topic_description = self.kafka.describe_topic(self.topic_name)
            partitions = self.kafka.parse_describe_topic_with_leader(topic_description)
            replicas = partitions['partitions'][0]['replicas']
            return verify_is_rack_aware_distribution(self.PER_NODE_SERVER_PROP_OVERRIDES, replicas)

        self.kafka.start()
        self._create_topic(self.topic_name, replication_factor=3, num_partitions=1, replica_assignment="1:2:3")
        assert is_rack_aware_distribution()
        self._start_cruise_control_and_wait_until_ready(self_healing_enabled='true')

        self._reassign_partition(self.topic_name, self.partition, [1, 2, 4])
        assert not is_rack_aware_distribution()

        wait_until(lambda: is_rack_aware_distribution(), timeout_sec=120, backoff_sec=5, err_msg="RackAwareDistributionGoal could not be self-healed")
