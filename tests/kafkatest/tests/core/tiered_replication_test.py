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

from kafkatest.tests.core.replication_test import *
from kafkatest.services.hadoop import MiniHadoop
from kafkatest.services.kafka import config_property


class TieredReplicationTest(ReplicationTest):

    def __init__(self, test_context):
        super(TieredReplicationTest, self).__init__(test_context)
        self.logger.debug("My test context: %s", str(test_context))

    @cluster(num_nodes=10)
    @parametrize(failure_mode="clean_shutdown", broker_type="leader", security_protocol="PLAINTEXT")
    def test_replication_with_broker_failure(self, failure_mode, security_protocol, broker_type,
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
        self.hadoop = MiniHadoop(self.test_context, num_nodes=3)
        self.hadoop.start()

        self.create_zookeeper()
        self.zk.start()

        server_prop_overides = [
            [config_property.REMOTE_LOG_STORAGE_ENABLE, "true"],
            [config_property.REMOTE_LOG_STORAGE_MANAGER_CLASS_NAME, "org.apache.kafka.rsm.hdfs.HDFSRemoteStorageManager"],
            [config_property.REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH,
             "/mnt/hadoop/config/etc/hadoop:/opt/kafka-dev/remote-storage-managers/hdfs/build/libs/*"
             + ":/opt/kafka-dev/remote-storage-managers/hdfs/build/dependant-libs/*"],
            [config_property.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME, security_protocol],
            [config_property.REMOTE_LOG_METADATA_TOPIC_NUM_PARTITIONS, "5"],
            [config_property.REMOTE_LOG_STORAGE_HDFS_FS_URI, self.hadoop.namenode_uri()],
            [config_property.REMOTE_LOG_STORAGE_HDFS_BASE_DIR, "/test"],
            [config_property.REMOTE_LOG_STORAGE_HDFS_REMOTE_READ_CACHE_MB, "8"],
            [config_property.REMOTE_LOG_STORAGE_HDFS_REMOTE_READ_BYTES_MB, "1"]
        ]
        self.create_kafka(num_nodes=3,
                          security_protocol=security_protocol,
                          interbroker_security_protocol=security_protocol,
                          client_sasl_mechanism=client_sasl_mechanism,
                          interbroker_sasl_mechanism=interbroker_sasl_mechanism,
                          tls_version=tls_version,
                          server_prop_overides=server_prop_overides)
        self.kafka.start()

        compression_types = None if not compression_type else [compression_type]
        self.create_producer(compression_types=compression_types, enable_idempotence=enable_idempotence)
        self.producer.start()

        self.create_consumer(log_level="DEBUG")
        self.consumer.start()

        self.await_startup()
        failures[failure_mode](self, broker_type)
        self.run_validation(enable_idempotence=enable_idempotence)