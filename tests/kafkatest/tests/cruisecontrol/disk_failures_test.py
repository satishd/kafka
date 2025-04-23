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

from kafkatest.services.kafka import KafkaService
from kafkatest.tests.cruisecontrol.cruise_control_test import CruiseControlTest


class DiskFailuresTest(CruiseControlTest, unittest.TestCase):

    def __init__(self, test_context):
        super(DiskFailuresTest, self).__init__(test_context, num_nodes=3)

    def test_disk_down(self):
        self.kafka.start()
        self._start_cruise_control_and_wait_until_ready()

        with self.cruise_control.nodes[0].account.monitor_log(self.cruise_control.LOG_FILE) as monitor:
            broker_node = self.kafka.nodes[0]
            # Make log dir inaccessible
            cmd = "chmod a-w %s -R" % KafkaService.DATA_LOG_DIR_1
            broker_node.account.ssh(cmd, allow_fail=False)

            # Verify alert is reported within 1 min of disk failure. Since the alert detection interval is 60 seconds, adding 10 seconds buffer for detection logic to complete.
            self.cruise_control.wait_till_disk_failure_alerts(monitor, timeout_sec=70)
