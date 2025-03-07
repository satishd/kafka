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

from kafkatest.services.cruisecontrol import CruiseControl
from kafkatest.services.kafka import KafkaService
from kafkatest.services.performance import ProducerPerformanceService
from kafkatest.tests.end_to_end import EndToEndTest


def fetch_topic_partition_proposal(cruise_control, topic_name, partition):
    proposals = json.loads(cruise_control.proposals())
    return [proposal for proposal in proposals["proposals"] if
            proposal["topicPartition"]["topic"] == topic_name and
            proposal["topicPartition"]["partition"] == partition]


class CruiseControlTest(EndToEndTest):

    def __init__(self, test_context, num_nodes, per_node_server_prop_overrides=None):
        super(CruiseControlTest, self).__init__(test_context)
        self.create_zookeeper_if_necessary()
        if self.zk:
            self.zk.start()
        self.kafka = KafkaService(test_context, num_nodes=num_nodes, zk=self.zk,
                                  per_node_server_prop_overrides=per_node_server_prop_overrides)

    def _create_topic(self, topic_name, replication_factor, num_partitions, replica_assignment=None):
        topic_cfg = {"topic": topic_name, "replication-factor": replication_factor, "partitions": num_partitions}
        if replica_assignment:
            topic_cfg["replica-assignment"] = replica_assignment
        self.kafka.create_topic(topic_cfg)

    def _reassign_partition(self, topic_name, partition, replicas):
        partition_info = self.kafka.parse_describe_topic(self.kafka.describe_topic(topic_name))
        partition_info["partitions"][partition]["replicas"] = replicas
        self.kafka.execute_reassign_partitions(partition_info)

    def _start_cruise_control_and_wait_until_ready(self, timeout_sec=600, disk_capacity_threshold=0.7,
                                                   self_healing_enabled='false'):
        bootstrap_servers = self.kafka.bootstrap_servers()
        zk_connect = self.zk.connect_setting(chroot=self.kafka.zk_chroot)
        self.cruise_control = CruiseControl(self.test_context, bootstrap_servers, zk_connect,
                                            disk_capacity_threshold=disk_capacity_threshold,
                                            self_healing_enabled=self_healing_enabled)
        self.cruise_control.start()
        self.cruise_control.wait_till_proposals_generated(timeout_sec=timeout_sec)

    def _create_producer(self, topic, num_records, message_size, num_nodes=1, acks=1, batch_size=8196,
                         buffer_memory=67108864):
        return ProducerPerformanceService(self.test_context, num_nodes=num_nodes, kafka=self.kafka, topic=topic,
                                          num_records=num_records, record_size=message_size, throughput=-1,
                                          settings={
                                              'acks': acks,
                                              'compression.type': 'none',
                                              'batch.size': batch_size,
                                              'buffer.memory': buffer_memory
                                          })

    def _delete_topics(self, *topics):
        for topic in topics:
            self.kafka.delete_topic(topic)