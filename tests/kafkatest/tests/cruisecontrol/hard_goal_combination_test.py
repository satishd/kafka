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
import unittest

from ducktape.errors import TimeoutError
from ducktape.services.service import Service

from kafkatest.tests.cruisecontrol.cruise_control_test import CruiseControlTest
from kafkatest.tests.cruisecontrol.new_replica_exclude_goal_test import verify_no_new_replica_assigned_to_broker
from kafkatest.tests.cruisecontrol.rack_aware_distribution_test import verify_rack_aware_distribution_for_proposal
from kafkatest.tests.cruisecontrol.disk_capacity_test import verify_disk_capacity_goal_no_action


class HardGoalCombinationTest(CruiseControlTest, unittest.TestCase):
    PER_NODE_SERVER_PROP_OVERRIDES = {
        1: [("broker.rack", "rack-a")],
        2: [("broker.rack", "rack-a")],
        3: [("broker.rack", "rack-b")],
        4: [("broker.rack", "rack-b")],
        5: [("broker.rack", "rack-c")],
        6: [("broker.rack", "rack-c")]
    }

    TOPIC1 = "topic1"
    TOPIC2 = "topic2"

    def __init__(self, test_context):
        super(HardGoalCombinationTest, self).__init__(test_context, num_nodes=6,
                                                      per_node_server_prop_overrides=self.PER_NODE_SERVER_PROP_OVERRIDES)
        # Each replica will be of size 150 MB (total disk capacity is 500 MB)
        target_data_size = 150 * 1024 * 1024
        self.message_size = 1024
        self.num_records = int(target_data_size / self.message_size)

    def test_hard_goal_combination_unsatisfiable(self):
        self._setup()
        # since replica moves to broker 5 is not allowed, all the hard goals cannot be satisfied with disk capacity
        # threshold of 50%
        with self.assertRaises(expected_exception=TimeoutError,
                               msg="Cruise control proposals not generated in 300 seconds"):
            self._start_cruise_control_and_wait_until_ready(timeout_sec=300, disk_capacity_threshold=0.5)

        proposals = json.loads(self.cruise_control.proposals())
        assert "OptimizationFailureException: [DiskCapacityGoal]" in proposals["errorMessage"]

        self._delete_topics(self.TOPIC1, self.TOPIC2)

    def test_hard_goal_combination_satisfiable(self):
        self._setup()
        # Although replica moves to broker 5 is not allowed, all the hard goals can be satisfied with disk capacity
        # threshold of 65%
        self._start_cruise_control_and_wait_until_ready(timeout_sec=600, disk_capacity_threshold=0.65)

        for topic in [self.TOPIC1, self.TOPIC2]:
            verify_rack_aware_distribution_for_proposal(self.cruise_control, topic, 0,
                                                        self.PER_NODE_SERVER_PROP_OVERRIDES)

        verify_disk_capacity_goal_no_action(self.cruise_control)
        verify_no_new_replica_assigned_to_broker(5, json.loads(self.cruise_control.proposals())["proposals"])

    def _setup(self):
        self.kafka.start()

        # Brokers 1,2,3 have 300 MB of data each (60% of disk capacity), while other brokers are empty
        self._create_topic(self.TOPIC1, replication_factor=3, num_partitions=1, replica_assignment="1:2:3")
        self._create_topic(self.TOPIC2, replication_factor=3, num_partitions=1, replica_assignment="1:2:3")

        producer1 = self._create_producer(self.TOPIC1, self.num_records, self.message_size)
        producer2 = self._create_producer(self.TOPIC2, self.num_records, self.message_size)

        Service.run_parallel(producer1, producer2)

        # Add broker 5 to new replica exclude list
        self.kafka.set_default_dynamic_config(dynamic_config_name="new.replica.exclude.list", dynamic_config_value="5",
                                              node=None)
