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

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from kafkatest.tests.cruisecontrol.cruise_control_test import CruiseControlTest


class ProposalExecutionTest(CruiseControlTest):

    def __init__(self, test_context):
        super(ProposalExecutionTest, self).__init__(test_context, num_nodes=4)
        # Each replica will be of size 500 KB
        target_data_size = 500 * 1024
        self.message_size = 1024
        self.num_records = int(target_data_size / self.message_size)

    def test_concurrent_execution(self):
        self.kafka.start()

        topic_names = self.create_topics(4, 25, 3)

        producers = []
        for topic_name in topic_names:
            producers.append(self._create_producer(topic_name, 25 * self.num_records, self.message_size))
        Service.run_parallel(*producers)

        self._start_cruise_control_and_wait_until_ready()
        result = json.loads(self.cruise_control.rebalance(
            goals='NewReplicaExcludeGoal,DiskCapacityGoal,RackAwareDistributionGoal,DiskUsageDistributionGoal'))
        assert 'errorMessage' not in result, "Failed to execute proposal: %s" % result

        result = json.loads(self.cruise_control.rebalance(
            goals='NewReplicaExcludeGoal,DiskCapacityGoal,RackAwareDistributionGoal,LeaderReplicaDistributionGoal'))
        assert 'errorMessage' in result, "Expected to fail to execute proposal: %s" % result
        assert 'Cannot start a new execution while there is an ongoing execution' in result['errorMessage']

    def test_broker_failure_during_execution(self):
        self.kafka.start()

        topic_names = self.create_topics(4, 25, 3)

        producers = []
        for topic_name in topic_names:
            producers.append(self._create_producer(topic_name, 25 * self.num_records, self.message_size))
        Service.run_parallel(*producers)

        self._start_cruise_control_and_wait_until_ready()
        result = json.loads(self.cruise_control.rebalance(
            goals='NewReplicaExcludeGoal,DiskCapacityGoal,RackAwareDistributionGoal,DiskUsageDistributionGoal'))
        assert 'errorMessage' not in result, "Failed to execute proposal: %s" % result

        # get rebalance request id from cruise control
        state = json.loads(self.cruise_control.state())
        executor_state = state['ExecutorState']
        assert executor_state['state'] == 'INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS'
        request_id = executor_state['triggeredUserTaskId']

        # verify state of request
        user_task_status = json.loads(self.cruise_control.user_task_status(request_id))
        assert user_task_status['userTasks'][0]['Status'] == 'InExecution'

        # Stop a broker
        self.kafka.stop_node(self.kafka.nodes[0])

        # verify the request is cancelled
        wait_until(lambda: json.loads(self.cruise_control.user_task_status(request_id))['userTasks'][0][
                               'Status'] == 'CompletedWithError',
                   timeout_sec=180,
                   backoff_sec=10,
                   err_msg="Rebalance did not stop after broker failure")

    def create_topics(self, num_topics, num_partitions, replication_factor):
        topic_names = []
        for i in range(num_topics):
            topic_name = "topic-" + str(i)
            self._create_topic(topic_name, replication_factor, num_partitions, replica_assignment="1:2:3")
            topic_names.append(topic_name)

        return topic_names
