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

from ducktape.errors import TimeoutError
from kafkatest.tests.cruisecontrol.cruise_control_test import CruiseControlTest


class BrokerFailuresTest(CruiseControlTest, unittest.TestCase):

    def __init__(self, test_context):
        super(BrokerFailuresTest, self).__init__(test_context, num_nodes=3)

    def test_broker_restart(self):
        self.kafka.start()
        self._start_cruise_control_and_wait_until_ready()

        with self.cruise_control.nodes[0].account.monitor_log(self.cruise_control.LOG_FILE) as monitor:
            # Restart a broker so that broker is up before 2 mins (the alerting threshold)
            self.kafka.restart_node(self.kafka.nodes[0])

            # Verify no broker failure alert since broker is up before the alerting threshold
            with self.assertRaises(expected_exception=TimeoutError,
                                   msg="Cruise control broker failure alerts not found in 180 seconds"):
                self.cruise_control.wait_till_broker_failure_alerts(monitor, timeout_sec=180)

    def test_broker_down(self):
        self.kafka.start()
        self._start_cruise_control_and_wait_until_ready()

        with self.cruise_control.nodes[0].account.monitor_log(self.cruise_control.LOG_FILE) as monitor:
            # Stop a broker
            self.kafka.stop_node(self.kafka.nodes[0])

            # Verify alert is reported within 1 min of broker failure alerting threshold which is 2 mins
            self.cruise_control.wait_till_broker_failure_alerts(monitor, timeout_sec=180)
