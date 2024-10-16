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

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService

from ducktape.tests.test import Test
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import matrix
from ducktape.mark.resource import cluster


def get_excluded_brokers(new_replica_exclude_list):
    excluded_broker_list = []
    if len(new_replica_exclude_list) > 0:
        excluded_broker_list = [int(x) for x in new_replica_exclude_list.split(":")]
    return excluded_broker_list


def get_valid_brokers(test, excluded_broker_list):
    available_brokers = [test.kafka.idx(node) for node in test.kafka.nodes]
    return [broker for broker in available_brokers if broker not in excluded_broker_list]


def create_topic(test, replication_factor, new_replica_exclude_list):
    """
    Test topic creation with excluded broker list. The replicas should not be assigned to brokers in the excluded list.
    When there is not enough valid brokers, it will expect an error.
    """
    topic_name = "test-topic-name"
    test.kafka.set_default_dynamic_config("new.replica.exclude.list", new_replica_exclude_list, test.kafka.get_node(0))
    excluded_broker_list = get_excluded_brokers(new_replica_exclude_list)
    valid_brokers = get_valid_brokers(test, excluded_broker_list)
    topic_cfg = {"topic": topic_name, "replication-factor": replication_factor, "partitions": 3}

    if len(valid_brokers) < replication_factor:  # no enough valid brokers
        try:
            test.kafka.create_topic(topic_cfg)
            assert False, "No enough available brokers."
        except RemoteCommandError:
            assert True, "Expect Error"
    else:
        test.kafka.create_topic(topic_cfg)
        topic_description = test.kafka.describe_topic(topic_name)
        partitions = test.kafka.parse_describe_topic(topic_description)
        for partition in partitions["partitions"]:
            for replica in partition["replicas"]:
                assert replica not in excluded_broker_list

        test.kafka.delete_topic(topic_name)

    test.kafka.set_default_dynamic_config("new.replica.exclude.list", "", test.kafka.get_node(0))


def add_partitions(test, replication_factor, new_replica_exclude_list):
    """
    Test partition expansion with excluded broker list. The newly added partition replicas should not be assigned to
    brokers in the excluded list.
    When there is not enough valid brokers, it will expect an error.
    """

    # create a new topic
    topic_name = "test-topic-name"
    test.kafka.set_default_dynamic_config("new.replica.exclude.list", "", test.kafka.get_node(0))
    topic_cfg = {"topic": topic_name, "replication-factor": replication_factor, "partitions": 1}
    if len(test.kafka.nodes) < replication_factor:  # no enough valid brokers
        try:
            test.kafka.create_topic(topic_cfg)
            assert False, "No enough available brokers."
        except RemoteCommandError:
            assert True, "Expect Error"

        # end the test
        test.kafka.set_default_dynamic_config("new.replica.exclude.list", "", test.kafka.get_node(0))
        return
    else:
        test.kafka.create_topic(topic_cfg)

    old_topic_description = test.kafka.describe_topic(topic_name)
    old_partitions = test.kafka.parse_describe_topic(old_topic_description)
    old_partition_ids = [partition["partition"] for partition in old_partitions["partitions"]]

    test.kafka.set_default_dynamic_config("new.replica.exclude.list", new_replica_exclude_list, test.kafka.get_node(0))
    excluded_broker_list = get_excluded_brokers(new_replica_exclude_list)
    valid_brokers = get_valid_brokers(test, excluded_broker_list)

    topic_cfg_alter = {"topic": topic_name, "partitions": 3}
    if len(valid_brokers) < replication_factor:  # no enough valid brokers
        try:
            test.kafka.alter_topic(topic_cfg_alter)
            assert False, "No enough available brokers."
        except RemoteCommandError:
            assert True, "Expect Error"
    else:
        test.kafka.alter_topic(topic_cfg_alter)
        topic_description = test.kafka.describe_topic(topic_name)
        partitions = test.kafka.parse_describe_topic(topic_description)

        for partition in partitions["partitions"]:
            if partition["partition"] not in old_partition_ids:  # check new partitions
                for replica in partition["replicas"]:
                    assert replica not in excluded_broker_list

    test.kafka.set_default_dynamic_config("new.replica.exclude.list", "", test.kafka.get_node(0))
    test.kafka.delete_topic(topic_name)


test_functions = {
    "create_topic": create_topic,
    "add_partitions": add_partitions
}


class NewReplicaExcludeListTest(Test):
    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(NewReplicaExcludeListTest, self).__init__(test_context=test_context)

        self.topics = []
        self.num_brokers = 5

        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context,
                                  num_nodes=self.num_brokers,
                                  zk=self.zk)

    def setUp(self):
        self.zk.start()

    @cluster(num_nodes=7)
    @matrix(test_mode=["create_topic", "add_partitions"],
            replication_factor=[1, 2, 4, 5, 7],
            new_replica_exclude_list=["", "1", "1:2", "3:1:2", "1:2:3:4:5"])
    def test_new_replica_exclude_list(self, test_mode, replication_factor, new_replica_exclude_list):
        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol

        self.kafka.start()
        test_functions[test_mode](self, replication_factor, new_replica_exclude_list)
