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

import unittest

from ducktape.utils.util import wait_until

from kafkatest.tests.cruisecontrol.disk_capacity_goal_helper import DiskCapacityGoalHelper


class DiskCapacityGoalSelfHealingTest(DiskCapacityGoalHelper, unittest.TestCase):
    DISK_CAPACITY_THRESHOLD = 0.7
    DISK_SIZE = 500 * 1024 * 1024

    def __init__(self, test_context):
        super(DiskCapacityGoalSelfHealingTest, self).__init__(test_context)

    def test_disk_capacity_goal_self_healing(self):
        def is_disk_capacity_goal_violated():
            for node in self.kafka.nodes:
                cmd = "du -sh %s*  | awk '{print $1}'" % self.kafka.DATA_LOG_DIR_PREFIX
                disk_usage = 0
                for line in node.account.ssh_capture(cmd, allow_fail=False):
                    disk_usage += self.convert_to_bytes(line.strip())

                if disk_usage > self.DISK_CAPACITY_THRESHOLD * self.DISK_SIZE:
                    return True
            return False

        self.kafka.start()
        assert not is_disk_capacity_goal_violated()

        self._start_cruise_control_and_wait_until_ready(disk_capacity_threshold=self.DISK_CAPACITY_THRESHOLD, self_healing_enabled='true')
        self.setup_unbalanced_load()
        assert is_disk_capacity_goal_violated()

        wait_until(lambda: not is_disk_capacity_goal_violated(), timeout_sec=600, backoff_sec=5, err_msg="DiskCapacityGoal could not be self-healed")

    @staticmethod
    def convert_to_bytes(size):
        units = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4,
                 'P': 1024**5, 'E': 1024**6, 'k': 1024, 'm': 1024**2,
                 'g': 1024**3, 't': 1024**4, 'p': 1024**5, 'e': 1024**6}
        if size[-1] in units:
            return int(float(size[:-1]) * units[size[-1]])
        else:
            return int(size)
