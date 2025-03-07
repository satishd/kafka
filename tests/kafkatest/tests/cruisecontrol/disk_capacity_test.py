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

from kafkatest.tests.cruisecontrol.disk_capacity_goal_helper import DiskCapacityGoalHelper


def verify_disk_capacity_goal_status(cruise_control, status):
    proposals = json.loads(cruise_control.proposals())

    disk_capacity_goal_summary = [summary for summary in proposals["goalSummary"] if
                                  summary["goal"] == "DiskCapacityGoal"]
    assert disk_capacity_goal_summary[0]["status"] == status


def verify_disk_capacity_goal_fixed(cruise_control):
    # Assert the goal was violated before and the new proposal fixed it
    verify_disk_capacity_goal_status(cruise_control, "FIXED")


def verify_disk_capacity_goal_no_action(cruise_control):
    # Assert the goal was satisfied before and the new proposal does nothing
    verify_disk_capacity_goal_status(cruise_control, "NO-ACTION")


class DiskCapacityTest(DiskCapacityGoalHelper, unittest.TestCase):

    def __init__(self, test_context):
        super(DiskCapacityTest, self).__init__(test_context)

    def test_disk_capacity_balanced(self):
        self.kafka.start()
        self.setup_balanced_load()

        self._start_cruise_control_and_wait_until_ready()
        verify_disk_capacity_goal_no_action(self.cruise_control)

        self.delete_topics()

    def test_disk_capacity_unbalanced(self):
        self.kafka.start()
        self.setup_unbalanced_load()

        self._start_cruise_control_and_wait_until_ready()
        verify_disk_capacity_goal_fixed(self.cruise_control)

        self.delete_topics()

    def test_disk_capacity_unsatisfiable(self):
        self.kafka.start()
        self.setup_unsatisfiable_load()

        with self.assertRaises(expected_exception=TimeoutError,
                               msg="Cruise control proposals not generated in 300 seconds"):
            self._start_cruise_control_and_wait_until_ready(timeout_sec=300)

        proposals = json.loads(self.cruise_control.proposals())
        assert "OptimizationFailureException: [DiskCapacityGoal]" in proposals["errorMessage"]

        self.delete_topics()
