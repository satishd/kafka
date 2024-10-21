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

from ducktape.utils.util import wait_until

from ducktape.mark import matrix
from ducktape.mark.resource import cluster

from kafkatest.tests.end_to_end import EndToEndTest
from kafkatest.services.kafka import KafkaService
import time

def broker_node(test, broker_type):
    """ Discover node of requested type. For leader type, discovers leader for our topic and partition 0
    """
    if broker_type == "leader":
        node = test.kafka.leader(test.topic, partition=0)
    elif broker_type == "follower":
        # take the 1st node which is not the current leader
        for follower_node in test.kafka.replicas(test.topic, partition=0):
            if test.kafka.idx(follower_node) != test.kafka.idx(test.kafka.leader(test.topic, partition=0)):
                node = follower_node
                break
    elif broker_type == "controller":
        node = test.kafka.controller()
    else:
        raise Exception("Unexpected broker type %s." % (broker_type))

    return node

def restart(test, doRestart, broker_type, clean_shutdown, leader_deprioritized_list):
    """
    Discover broker node of requested type and shut it shutdown cleanly or hard kill,
    clean up the kafka data directory to simulate an empty broker, then restart.
    """
    node = broker_node(test, broker_type)

    broker_id = test.kafka.idx(node)
    test.logger.info("broker_type: {0}, broker_id: {1} on node {2}".format(broker_type, broker_id, str(node.account)))

    test.kafka.set_default_dynamic_config("leader.deprioritized.list", leader_deprioritized_list, node)

    if doRestart:
        test.kafka.stop_node(node, clean_shutdown)

        if not clean_shutdown:
            # Since this is a hard kill, we need to make sure the process is down and that
            # zookeeper has registered the loss by expiring the broker's session timeout.
            wait_until(lambda: len(test.kafka.pids(node)) == 0 and not test.kafka.is_registered(node),
                        timeout_sec=test.kafka.zk_session_timeout + 5,
                        err_msg="Failed to see timely deregistration of hard-killed broker %s" % str(node.account))
            # delete everything to simulate replacing an empty broker.
            #node.account.ssh("sudo rm -rf -- %s" % KafkaService.PERSISTENT_ROOT, allow_fail=False)
        test.kafka.start_node(node)
        # wait till the replication caught up partition assignment replicas length = ISR length
        wait_until(lambda: len(test.kafka.replicas(test.topic, partition=0))==len(test.kafka.isr_idx_list(test.topic, partition=0)), timeout_sec=30, backoff_sec=.25, err_msg="Timeout waiting for restart broker to join ISR.")

    # run preferred leader election
    test.kafka.run_preferred_leader_election(test.topic)
    topic_partitions = test.kafka.parse_describe_topic_with_leader(test.kafka.describe_topic(test.topic))
    test.logger.info("leader_deprioritized_list: {}  topic_partitions: {}".format(leader_deprioritized_list, topic_partitions))
    #print("[leader, preferred_leader]: {}".format([ [partition["leader"], partition["replicas"][0]] for partition in topic_partitions["partitions"]]))
    if leader_deprioritized_list == "":
        # the current leader should be the same as the preferred leader (1st in the replicas) of the partition.
        assert all(partition["leader"]==partition["replicas"][0] for partition in topic_partitions["partitions"])
    else:
        assert all(partition["leader"] not in [ int(x) for x in leader_deprioritized_list.split(":")] or len(leader_deprioritized_list.split(":"))==3 for partition in topic_partitions["partitions"])

    # clear leader.deprioritized.list back to ""
    test.kafka.set_default_dynamic_config("leader.deprioritized.list", "", node)

def clean_restart(test, broker_type, leader_deprioritized_list):
    restart(test, True, broker_type, True, leader_deprioritized_list)

def hard_restart(test, broker_type, leader_deprioritized_list):
    restart(test, True, broker_type, False, leader_deprioritized_list)

def no_restart(test, broker_type, leader_deprioritized_list):
    restart(test, False, broker_type, False, leader_deprioritized_list)

failures = {
    "clean_restart": clean_restart,
    "hard_restart": hard_restart,
    "no_restart": no_restart
}


class LeaderDeprioritizedListTest(EndToEndTest):
    """
    Note that consuming is a bit tricky, at least with console consumer. The goal is to consume all messages
    (foreach partition) in the topic. In this case, waiting for the last message may cause the consumer to stop
    too soon since console consumer is consuming multiple partitions from a single thread and therefore we lose
    ordering guarantees.

    Waiting on a count of consumed messages can be unreliable: if we stop consuming when num_consumed == num_acked,
    we might exit early if some messages are duplicated (though not an issue here since producer retries==0)

    Therefore rely here on the consumer.timeout.ms setting which times out on the interval between successively
    consumed messages. Since we run the producer to completion before running the consumer, this is a reliable
    indicator that nothing is left to consume.
    """

    TOPIC_CONFIG = {
        "partitions": 3,
        "replication-factor": 3,
        "configs": {"min.insync.replicas": 2}
    }

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(LeaderDeprioritizedListTest, self).__init__(test_context=test_context, topic_config=self.TOPIC_CONFIG)

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(LeaderDeprioritizedListTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    # noinspection PyInterpreter
    @cluster(num_nodes=7)
    @matrix(failure_mode=["no_restart", "clean_restart", "hard_restart"],
            broker_type=["leader", "controller"],
            leader_deprioritized_list=["", "1", "1:2", "3:1:2"],
            security_protocol=["PLAINTEXT"],
            enable_idempotence=[True])
    def test_leadership_changes(self, failure_mode, security_protocol, broker_type, leader_deprioritized_list,
                                client_sasl_mechanism="GSSAPI", interbroker_sasl_mechanism="GSSAPI",
                                compression_type=None, enable_idempotence=False, tls_version=None):
        """Replication tests.
        These tests verify that replication provides simple durability guarantees by checking that data acked by
        brokers is still available for consumption in the face of various failure scenarios.

        Setup: 1 zk, 3 kafka nodes, 1 topic with partitions=3, replication-factor=3, and min.insync.replicas=2

            - Produce messages in the background
            - Consume messages in the background
            - Drive broker failures (shutdown, or bounce repeatedly with kill -15 or kill -9)
            - When done driving failures, stop producing, and finish consuming
            - Validate that every acked message was consumed
        """

        self.create_zookeeper_if_necessary()
        self.zk.start()

        self.create_kafka(num_nodes=3,
                         security_protocol=security_protocol,
                         interbroker_security_protocol=security_protocol,
                         client_sasl_mechanism=client_sasl_mechanism,
                         interbroker_sasl_mechanism=interbroker_sasl_mechanism,
                         tls_version=tls_version,
                         server_prop_overrides=[["auto.leader.rebalance.enable", "false"]])
        self.kafka.start()

        compression_types = None if not compression_type else [compression_type]
        self.create_producer(compression_types=compression_types, enable_idempotence=enable_idempotence)
        self.producer.start()

        self.create_consumer(log_level="DEBUG")
        self.consumer.start()

        self.await_startup()
        failures[failure_mode](self, broker_type, leader_deprioritized_list)
        self.run_validation(enable_idempotence=enable_idempotence)
