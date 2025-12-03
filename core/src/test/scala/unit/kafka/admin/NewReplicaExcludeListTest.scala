/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package unit.kafka.admin

import kafka.server.BaseRequestTest
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.server.config.ServerLogConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException
import scala.jdk.CollectionConverters._

class NewReplicaExcludeListTest extends BaseRequestTest {
  override def brokerCount: Int = 3

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testReplicaPlacementWithSuccess(quorum: String): Unit = {
    setNewReplicaExcludeList(Seq(0, 1))

    // Create a topic with 4 partitions and 1 RF, all replicas across partitions are expected on the broker 2
    val topic = "test"
    val numPartitions = 4
    val numReplicationFactor: Short = 1
    createTopics(topic, numPartitions, numReplicationFactor)

    val description = createAdminClient().describeTopics(util.Collections.singleton(topic)).allTopicNames.get.asScala.get(topic)
    assertTrue(description.isDefined)
    // Verify all partitions have the replica on broker 2
    for (i <- 0 until numPartitions) {
      assertEquals(2, description.get.partitions().get(i).replicas().get(0).id())
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testReplicaPlacementWithInsufficientBrokers(quorum: String): Unit = {
    // Exclude all brokers
    setNewReplicaExcludeList(Seq(0, 1, 2))

    // Create a topic with 1 partition and 1 RF, topic creation should fail
    val topic = "test"
    val numPartitions = 1
    val numReplicationFactor: Short = 1
    assertThrows(classOf[ExecutionException], () => createTopics(topic, numPartitions, numReplicationFactor))
  }

  private def setNewReplicaExcludeList(brokerIds: Seq[Int]): Unit = {
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.BROKER, ""),
      util.Collections.singletonList(
        new AlterConfigOp(new ConfigEntry(ServerLogConfigs.NEW_REPLICA_EXCLUDE_LIST_CONFIG, brokerIds.mkString(":")),
          AlterConfigOp.OpType.SET),
      ))
    createAdminClient().incrementalAlterConfigs(configs).all().get()
  }

  private def createTopics(topic: String, numPartitions: Int, numReplicationFactor: Short): Unit = {
    TestUtils.createTopicWithAdmin(createAdminClient(), topic, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = new Properties())
  }
}
