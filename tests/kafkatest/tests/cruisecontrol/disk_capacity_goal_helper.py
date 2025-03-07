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

from ducktape.services.service import Service
from kafkatest.tests.cruisecontrol.cruise_control_test import CruiseControlTest


class DiskCapacityGoalHelper(CruiseControlTest):
    TOPIC1 = "topic1"
    TOPIC2 = "topic2"
    TOPIC3 = "topic3"

    def __init__(self, test_context):
        super(DiskCapacityGoalHelper, self).__init__(test_context, num_nodes=3)
        # Each replica will be of size 128 MB
        target_data_size = 128 * 1024 * 1024
        self.message_size = 1024
        self.num_records = int(target_data_size / self.message_size)

    def setup_balanced_load(self):
        # All brokers have equal disk usage (256 MB) that is below the capacity threshold of 0.7 (total disk capacity
        # is 500 MB)
        self._create_topic(self.TOPIC1, replication_factor=2, num_partitions=1, replica_assignment="1:2")
        self._create_topic(self.TOPIC2, replication_factor=2, num_partitions=1, replica_assignment="2:3")
        self._create_topic(self.TOPIC3, replication_factor=2, num_partitions=1, replica_assignment="1:3")

        producer1 = self._create_producer(self.TOPIC1, self.num_records, self.message_size)
        producer2 = self._create_producer(self.TOPIC2, self.num_records, self.message_size)
        producer3 = self._create_producer(self.TOPIC3, self.num_records, self.message_size)

        Service.run_parallel(producer1, producer2, producer3)

    def setup_unbalanced_load(self):
        # Broker 2 has higher disk usage (384 MB) compared to the other brokers and exceeds the capacity threshold of
        # 0.7 (total disk capacity is 500 MB)
        self._create_topic(self.TOPIC1, replication_factor=2, num_partitions=1, replica_assignment="1:2")
        self._create_topic(self.TOPIC2, replication_factor=2, num_partitions=1, replica_assignment="2:3")
        self._create_topic(self.TOPIC3, replication_factor=2, num_partitions=1, replica_assignment="2:3")

        producer1 = self._create_producer(self.TOPIC1, self.num_records, self.message_size)
        producer2 = self._create_producer(self.TOPIC2, self.num_records, self.message_size)
        producer3 = self._create_producer(self.TOPIC3, self.num_records, self.message_size)

        Service.run_parallel(producer1, producer2, producer3)

    def setup_unsatisfiable_load(self):
        # All brokers have equal disk usage and exceed the capacity threshold of 0.7 (total disk capacity is 500 MB)
        self._create_topic(self.TOPIC1, replication_factor=3, num_partitions=1, replica_assignment="1:2:3")
        self._create_topic(self.TOPIC2, replication_factor=3, num_partitions=1, replica_assignment="1:2:3")
        self._create_topic(self.TOPIC3, replication_factor=3, num_partitions=1, replica_assignment="1:2:3")

        producer1 = self._create_producer(self.TOPIC1, self.num_records, self.message_size)
        producer2 = self._create_producer(self.TOPIC2, self.num_records, self.message_size)
        producer3 = self._create_producer(self.TOPIC3, self.num_records, self.message_size)

        Service.run_parallel(producer1, producer2, producer3)

    def delete_topics(self):
        self._delete_topics(self.TOPIC1, self.TOPIC2, self.TOPIC3)
