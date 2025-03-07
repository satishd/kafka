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
from collections import Counter

from ducktape.services.service import Service

from kafkatest.tests.cruisecontrol.cruise_control_test import CruiseControlTest


def verify_no_new_replica_assigned_to_broker(broker_id, proposals):
    for proposal in proposals:
        new_replicas = set(proposal["newReplicas"])
        old_replicas = set(proposal["oldReplicas"])
        assert broker_id not in new_replicas.difference(old_replicas)


class NewReplicaExcludeTest(CruiseControlTest):
    TOPIC1 = "topic1"
    TOPIC2 = "topic2"
    TOPIC3 = "topic3"

    def __init__(self, test_context):
        super(NewReplicaExcludeTest, self).__init__(test_context, num_nodes=4)
        # Each replica will be of size 64 MB
        target_data_size = 64 * 1024 * 1024
        self.message_size = 1024
        self.num_records = int(target_data_size / self.message_size)

    """
    Test to validate that if there are no excluded brokers, replicas are assigned to all brokers in the cluster.
    """
    def test_no_excluded_brokers(self):
        self.kafka.start()
        # Create replicas such that the broker 4 is empty
        self._set_up_unbalanced_load_on_cluster()
        self._start_cruise_control_and_wait_until_ready()

        replicas_by_broker = self._replicas_by_broker_from_proposal()
        # Some replicas should be assigned to the empty broker
        assert replicas_by_broker[4] != 0

    """
    Test to validate that replicas are not moved to brokers in the new replica exclude list.
    """
    def test_excluded_brokers(self):
        self.kafka.start()

        # Add broker 4 to new replica exclude list
        self.kafka.set_default_dynamic_config(dynamic_config_name="new.replica.exclude.list", dynamic_config_value="4",
                                              node=None)
        # Create replicas such that the broker 4 is empty
        self._set_up_unbalanced_load_on_cluster()
        self._start_cruise_control_and_wait_until_ready()

        replicas_by_broker = self._replicas_by_broker_from_proposal()
        # No replicas should be assigned to broker 4
        assert replicas_by_broker[4] == 0

    """
    Test to validate that existing replicas are not forced out of the excluded broker. Also, no new replicas are moved 
    into the exclude broker
    """
    def test_no_evacuation_for_existing_replicas(self):
        self.kafka.start()

        self._set_up_balanced_load_on_cluster()

        # Add broker 4 to new replica exclude list
        self.kafka.set_default_dynamic_config(dynamic_config_name="new.replica.exclude.list", dynamic_config_value="4",
                                              node=None)
        self._start_cruise_control_and_wait_until_ready()
        proposals = json.loads(self.cruise_control.proposals())["proposals"]
        replicas_by_broker = self._replicas_by_broker_from_proposal(proposals)
        # Some replicas should still be assigned to the excluded broker, since the new replica exclude feature does not
        # evacuate existing replicas
        assert replicas_by_broker[4] > 0
        # No new replicas should be assigned to the excluded broker
        verify_no_new_replica_assigned_to_broker(broker_id=4, proposals=proposals)

    """
    Sets up topics on the cluster so that all replica are contained within the first 3 brokers. Also, sets up data
    for the topics, so that the first 3 brokers have significant disk usage while the 4th broker is completely empty.
    """
    def _set_up_unbalanced_load_on_cluster(self):
        self._create_topic(self.TOPIC1, replication_factor=2, num_partitions=2, replica_assignment="1:2,2:3")
        self._create_topic(self.TOPIC2, replication_factor=2, num_partitions=2, replica_assignment="1:2,1:3")
        self._create_topic(self.TOPIC3, replication_factor=2, num_partitions=2, replica_assignment="1:3,2:3")

        self.producer1 = self._create_producer(self.TOPIC1, self.num_records, self.message_size)
        self.producer2 = self._create_producer(self.TOPIC2, self.num_records, self.message_size)
        self.producer3 = self._create_producer(self.TOPIC3, self.num_records, self.message_size)

        Service.run_parallel(self.producer1, self.producer2, self.producer3)

    def _set_up_balanced_load_on_cluster(self):
        self._create_topic(self.TOPIC1, replication_factor=2, num_partitions=2, replica_assignment="1:2,3:4")
        self._create_topic(self.TOPIC2, replication_factor=2, num_partitions=2, replica_assignment="1:2,3:4")
        self._create_topic(self.TOPIC3, replication_factor=2, num_partitions=2, replica_assignment="1:2,3:4")

        self.producer1 = self._create_producer(self.TOPIC1, self.num_records, self.message_size)
        self.producer2 = self._create_producer(self.TOPIC2, self.num_records, self.message_size)
        self.producer3 = self._create_producer(self.TOPIC3, self.num_records, self.message_size)

        Service.run_parallel(self.producer1, self.producer2, self.producer3)

    def _replicas_by_broker_from_proposal(self, proposals=None):
        if proposals is None:
            proposals = json.loads(self.cruise_control.proposals())["proposals"]
        # Flatten the list of new replicas and count the number of new replicas on each broker
        all_new_replicas = [replica for sublist in [proposal['newReplicas'] for proposal in proposals] for replica in
                            sublist]
        return Counter(all_new_replicas)
