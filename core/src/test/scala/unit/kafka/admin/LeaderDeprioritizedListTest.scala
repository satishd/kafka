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
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry, NewPartitionReassignment}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.{ElectionType, TopicPartition}
import org.apache.kafka.server.config.ServerLogConfigs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import java.util.{Optional, Properties}
import scala.jdk.CollectionConverters._

class LeaderDeprioritizedListTest extends BaseRequestTest {
  override def brokerCount: Int = 3

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testLeaderDeprioritization(quorum: String): Unit = {
    val topic = "test"
    val numPartitions = 1
    val partition: Short = 0
    val tp = new TopicPartition(topic, partition)
    val numReplicationFactor: Short = 3
    createTopics(topic, numPartitions, numReplicationFactor)

    // Verify that the first replica is indeed the leader
    val originalLeader = currentLeader(tp)
    val assignment = currentAssignment(tp)
    assertEquals(originalLeader, assignment.head)

    // Reassign by putting the first replica to the end
    val newAssignment = assignment.drop(1) :+ assignment.head
    alterPartitionReassignment(tp, newAssignment)

    // Verify the reassignment is complete
    TestUtils.waitUntilTrue(() => {
      originalLeader != currentAssignment(tp).head
    }, "Reassignment should have completed", 5000)

    // Deprioritize the broker corresponding to the first replica
    val deprioritizedBroker = currentAssignment(tp).head
    setLeaderDeprioritizedList(Seq(deprioritizedBroker))

    // On Preferred leader election, neither the first replica nor the previous leader should become a leader
    createAdminClient().electLeaders(ElectionType.PREFERRED, Set(tp).asJava)
    TestUtils.waitUntilTrue(() => {
      val leader = currentLeader(tp)
      currentAssignment(tp).head != leader && originalLeader != leader
    }, "Leader should have changed", 5000)
  }

  private def currentAssignment(tp: TopicPartition): Seq[Int] = {
    val description = createAdminClient()
      .describeTopics(util.Collections.singleton(tp.topic()))
      .allTopicNames.get.asScala
      .get(tp.topic())
    description.get.partitions().get(tp.partition()).replicas().asScala.map(_.id()).toSeq
  }

  private def currentLeader(tp: TopicPartition): Int = {
    val description = createAdminClient()
      .describeTopics(util.Collections.singleton(tp.topic()))
      .allTopicNames.get.asScala
      .get(tp.topic())
    description.get.partitions().get(tp.partition()).leader().id()
  }

  private def setLeaderDeprioritizedList(brokerIds: Seq[Int]): Unit = {
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    configs.put(new ConfigResource(ConfigResource.Type.BROKER, ""),
      util.Collections.singletonList(
        new AlterConfigOp(new ConfigEntry(ServerLogConfigs.LEADER_DEPRIORITIZED_LIST_CONFIG, brokerIds.mkString(":")),
          AlterConfigOp.OpType.SET),
      ))
    createAdminClient().incrementalAlterConfigs(configs).all().get()
  }

  private def createTopics(topic: String, numPartitions: Int, numReplicationFactor: Short): Unit = {
    TestUtils.createTopicWithAdmin(createAdminClient(), topic, brokers, controllerServers, numPartitions, numReplicationFactor,
      topicConfig = new Properties())
  }

  private def alterPartitionReassignment(tp: TopicPartition, newAssignment: Seq[Int]): Unit = {
    var reassignments = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
    reassignments += tp -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
    createAdminClient().alterPartitionReassignments(reassignments.asJava).all().get()
  }
}
