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
from ducktape.mark.resource import cluster
import json

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int_with_prefix


class RecreateRecentlyDeletedTopicTest(ProduceConsumeValidateTest):
    """
    These tests validate produce / consume for compressed topics.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(RecreateRecentlyDeletedTopicTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk, topics={self.topic: {
            "partitions": 10,
            "replication-factor": 1}}, server_prop_overrides=[
                ("auto.create.topics.enable", "false"),
                ("recreate.recently.deleted.topics.enable", "true"),
        ])
        self.num_partitions = 10
        self.timeout_sec = 60
        self.producer_throughput = 1000
        self.num_producers = 2
        self.messages_per_producer = 1000
        self.messages_per_producer_2nd_time = 2000
        self.num_consumers = 1

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(RecreateRecentlyDeletedTopicTest, self).min_cluster_size() + self.num_producers + self.num_consumers

    @cluster(num_nodes=7)
    def test_recreate_recently_deleted_topic(self):
        """Test consume => produce1 => delete_topic => produce2 => endConsume => validate
        Setup: 1 zk, 1 kafka node, 1 topic with partitions=10, replication-factor=1

            - Consume messages in the background
            - Produce 1st set of messages in the background
            - Delete the topic
            - Produce 2nd set of messages in the background
            - Validate that every acked message from 1st and 2nd was consumed
        """

        self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                           self.topic, throughput=self.producer_throughput,
                                           message_validator=is_int_with_prefix, max_messages=(self.messages_per_producer))
        self.consumer = ConsoleConsumer(self.test_context, self.num_consumers, self.kafka, self.topic,
                                        True, consumer_timeout_ms=20000,
                                        message_validator=is_int_with_prefix)
        self.kafka.start()

        try:
            self.start_producer_and_consumer()
            wait_until(
                lambda: self.producer.each_produced_at_least(self.messages_per_producer) == True,
                timeout_sec=120, backoff_sec=1,
                err_msg="Producer did not produce all messages in reasonable amount of time")

            acked_1st_time = len(self.producer.acked)
            self.logger.info("num producer acked 1st time:  %d" % acked_1st_time)
            self.logger.info("num consumer consumed 1st time:  %d" % len(self.consumer.messages_consumed[1]))

            # Delete the topic
            self.kafka.delete_topic(self.topic)

            wait_until(lambda: self.zk.query("/admin/recently_deleted_topics/" + self.topic) is not None, timeout_sec=10, backoff_sec=1,
                       err_msg="Not able to delete the topic %s" % self.topic)

            self.producer = VerifiableProducer(self.test_context, self.num_producers, self.kafka,
                                               self.topic, throughput=self.producer_throughput,
                                               message_validator=is_int_with_prefix,
                                               max_messages=(self.messages_per_producer_2nd_time))
            self.producer.start()
            wait_until(
                lambda: self.producer.each_produced_at_least(self.messages_per_producer_2nd_time) == True,
                timeout_sec=120, backoff_sec=1,
                err_msg="Producer did not produce all messages in reasonable amount of time")

            self.logger.info("num producer acked 2nd time:  %d" % len(self.producer.acked))

            self.consumer.wait()
            self.logger.info("num consumer consumed total:  %d" % len(self.consumer.messages_consumed[1]))

            # Assert that the 1st and 2nd installment of produce equals to the total messages consumed
            assert len(self.consumer.messages_consumed[1]) == acked_1st_time + len(self.producer.acked)

            deleted_topic_metadata = json.loads(self.zk.query("/admin/recently_deleted_topics/" + self.topic))

            assert 10 == int(deleted_topic_metadata["numPartitions"]), "Old partition count should be retained"
            assert "PartitionCount: 10" in self.kafka.describe_topic(self.topic), "Old partition count should be retained"

        except BaseException as e:
            for s in self.test_context.services:
                self.mark_for_collect(s)
            raise

