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


class RackAwareDistributionGoalViolationTest(CruiseControlTest):
    PER_NODE_SERVER_PROP_OVERRIDES = {
        1: [("broker.rack", "rack-a")],
        2: [("broker.rack", "rack-b")],
        3: [("broker.rack", "rack-c")],
        4: [("broker.rack", "rack-a")]
    }

    def __init__(self, test_context):
        super(RackAwareDistributionGoalViolationTest, self).__init__(test_context, num_nodes=4,
                                                                     per_node_server_prop_overrides=self.PER_NODE_SERVER_PROP_OVERRIDES)
        self.topic_name = "test-topic-name"
        self.partition = 0

    def test_goal_violation_anomaly(self):
        self.kafka.start()
        self._create_topic(self.topic_name, replication_factor=3, num_partitions=1, replica_assignment="1:2:3")

        self._start_cruise_control_and_wait_until_ready()
        with self.cruise_control.nodes[0].account.monitor_log(self.cruise_control.LOG_FILE) as monitor:
            self._reassign_partition(self.topic_name, self.partition, [1, 2, 4])
            # Verify alert is reported within 1 min
            self.cruise_control.wait_till_goal_violation_alerts(monitor, "RackAwareDistributionGoal", timeout_sec=60)
