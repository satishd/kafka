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

import signal
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

def make_partition_offset_start_from_non_zero(test, topic, partition):
    # make the follower node(s) starting offset != 0 first,   use replica.start.offset.strategy=Latest to achieve that
    for follower_node in test.kafka.replicas(topic, partition):
        if test.kafka.idx(follower_node) != test.kafka.idx(test.kafka.leader(topic, partition)):
            test.kafka.set_broker_dynamic_config("replica.start.offset.strategy", "latest", follower_node)
            test.kafka.stop_node(follower_node, clean_shutdown=False)
            wait_until(lambda: len(test.kafka.pids(follower_node)) == 0 and not test.kafka.is_registered(follower_node),
                       timeout_sec=test.kafka.zk_session_timeout + 5,
                       err_msg="Failed to see timely deregistration of hard-killed broker %s" % str(follower_node.account))
            # delete everything to simulate replacing an empty broker.
            follower_node.account.ssh("sudo rm -rf -- %s" % KafkaService.PERSISTENT_ROOT, allow_fail=False)
            test.kafka.start_node(follower_node)
            time.sleep(1)
            wait_until(lambda: len(test.kafka.replicas(topic, partition))==len(test.kafka.isr_idx_list(topic, partition)), timeout_sec=60, backoff_sec=.25, err_msg="Timeout waiting for restart broker to join ISR.")
            test.kafka.delete_broker_dynamic_config("replica.start.offset.strategy", follower_node)
    # now bounce leader and make it follower and starting offset != 0
    leader_node = test.kafka.leader(topic, partition)
    test.kafka.set_broker_dynamic_config("replica.start.offset.strategy", "latest", leader_node)
    test.kafka.stop_node(leader_node, clean_shutdown=False)
    wait_until(lambda: len(test.kafka.pids(leader_node)) == 0 and not test.kafka.is_registered(leader_node),
               timeout_sec=test.kafka.zk_session_timeout + 5,
               err_msg="Failed to see timely deregistration of hard-killed broker %s" % str(leader_node.account))
    leader_node.account.ssh("sudo rm -rf -- %s" % KafkaService.PERSISTENT_ROOT, allow_fail=False)
    test.kafka.start_node(leader_node)
    time.sleep(1)
    wait_until(lambda: len(test.kafka.replicas(topic, partition))==len(test.kafka.isr_idx_list(topic, partition)), timeout_sec=60, backoff_sec=.25, err_msg="Timeout waiting for restart broker to join ISR.")
    test.kafka.delete_broker_dynamic_config("replica.start.offset.strategy", leader_node)

def restart(test, broker_type, clean_shutdown, replica_start_offset_strategy, offset_start_from_zero, compact_topic):
    """
    Discover broker node of requested type and shut it shutdown cleanly or hard kill,
    clean up the kafka data directory to simulate an empty broker, then restart.
    """

    if not offset_start_from_zero:
        make_partition_offset_start_from_non_zero(test, test.topic, 0)

    node = broker_node(test, broker_type)

    if compact_topic:
        test.logger.info("setting topic {0} to compact topic".format(test.topic))
        test.kafka.set_topic_config(test.topic, "cleanup.policy", "compact", node)

    broker_id = test.kafka.idx(node)
    test.logger.info("clean_shutdown: {0}, broker_type: {1}, broker_id: {2} on node {3}".format(clean_shutdown, broker_type, broker_id, str(node.account)))

    test.kafka.set_broker_dynamic_config("replica.start.offset.strategy", replica_start_offset_strategy, node)
    test.logger.info("setting dynamic config replica.start.offset.strategy {0} on node {1}".format(replica_start_offset_strategy, node))

    test.kafka.stop_node(node, clean_shutdown)

    if not clean_shutdown:
        # Since this is a hard kill, we need to make sure the process is down and that
        # zookeeper has registered the loss by expiring the broker's session timeout.
        wait_until(lambda: len(test.kafka.pids(node)) == 0 and not test.kafka.is_registered(node),
                   timeout_sec=test.kafka.zk_session_timeout + 5,
                   err_msg="Failed to see timely deregistration of hard-killed broker %s" % str(node.account))

    # delete everything to simulate replacing an empty broker.
    node.account.ssh("sudo rm -rf -- %s" % KafkaService.PERSISTENT_ROOT, allow_fail=False)

    test.kafka.start_node(node)

    # wait till the replication caught up partition assignment replicas length = ISR length
    wait_until(lambda: len(test.kafka.replicas(test.topic, partition=0))==len(test.kafka.isr_idx_list(test.topic, partition=0)), timeout_sec=60, backoff_sec=.25, err_msg="Timeout waiting for restart broker to join ISR.")

    # Get the test.topic/partition 0's leader's first offset
    leader_node = broker_node(test, "leader")
    leader_starting_offset = test.kafka.get_starting_offset(leader_node, test.topic, 0)
    test.logger.info("leader_starting_offset: {0}".format(leader_starting_offset))
    # Get the test.topic/partition 0's bounce empty broker's first offset
    bounce_empty_broker_starting_offset = test.kafka.get_starting_offset(node, test.topic, 0)
    test.logger.info("bournce_empty_broker_starting_offset: {0}".format(bounce_empty_broker_starting_offset))

    # for compact_topic, no matter what the replica.start.offset.strategy is (earliest/latest), it will use the leader start offset.
    if replica_start_offset_strategy == "earliest" or compact_topic:
        assert leader_starting_offset == bounce_empty_broker_starting_offset
    elif replica_start_offset_strategy == "latest":
        assert bounce_empty_broker_starting_offset > leader_starting_offset
    else:
        raise ValueError("Invalid replica_start_offset_strategy: {0} .  Can only be earliest or latest".format(replica_start_offset_strategy))

    # Test removing the dynamic config
    test.kafka.delete_broker_dynamic_config("replica.start.offset.strategy", node)

def clean_restart(test, broker_type, replica_start_offset_strategy, offset_start_from_zero, compact_topic):
    restart(test, broker_type, True, replica_start_offset_strategy, offset_start_from_zero, compact_topic)

def hard_restart(test, broker_type, replica_start_offset_strategy, offset_start_from_zero, compact_topic):
    restart(test, broker_type, False, replica_start_offset_strategy, offset_start_from_zero, compact_topic)

failures = {
    "clean_restart": clean_restart,
    "hard_restart": hard_restart
}


class ReplicationReplicaStartOffsetStrategyTest(EndToEndTest):
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
        super(ReplicationReplicaStartOffsetStrategyTest, self).__init__(test_context=test_context, topic_config=self.TOPIC_CONFIG)

    def min_cluster_size(self):
        """Override this since we're adding services outside of the constructor"""
        return super(ReplicationReplicaStartOffsetStrategyTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    @cluster(num_nodes=7)
    @matrix(failure_mode=["clean_restart", "hard_restart"],
            broker_type=["follower", "leader"],
            replica_start_offset_strategy=["earliest", "latest"],
            offset_start_from_zero=[True, False],
            compact_topic=[True, False],
            security_protocol=["PLAINTEXT"],
            enable_idempotence=[True])
    def test_replication_with_broker_failure(self, failure_mode, security_protocol, broker_type, replica_start_offset_strategy,
                                             offset_start_from_zero, compact_topic,
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
        self.create_producer(repeating_keys=10, compression_types=compression_types, enable_idempotence=enable_idempotence)
        self.producer.start()

        self.create_consumer(log_level="DEBUG")
        self.consumer.start()

        self.await_startup()
        failures[failure_mode](self, broker_type, replica_start_offset_strategy, offset_start_from_zero, compact_topic)
        self.run_validation(enable_idempotence=enable_idempotence)