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
package kafka.metrics

import com.yammer.metrics.core.Gauge
import kafka.controller.UnderReplicatedPartitionMetrics
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._

class KafkaControllerMetricsTest extends KafkaServerTestHarness with Logging {
  val numNodes = 2
  val numPartitions = 2

  override def generateConfigs: scala.collection.Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(numNodes, zkConnect, enableDeleteTopic = true).map(KafkaConfig.fromProps)

  @Test
  def testUrpSourceMetrics(): Unit = {
    val topic = "urp-source-topic"
    val nMessages = 10
    val replicationFactor = numNodes
    createTopic(topic, numPartitions, replicationFactor)
    TestUtils.generateAndProduceMessages(servers, topic, nMessages)
    val lastServer = servers.last
    val brokerId = lastServer.config.brokerId

    // Shuttingdown one server will make both topic partitions will have only one live replica, which should make those
    // partitions under replicated when more messages are sent which moves HW as min.insync.replicas = 1.
    lastServer.shutdown()
    lastServer.awaitShutdown()

    TestUtils.generateAndProduceMessages(servers, topic, nMessages)

    val urpMetrics = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .filter { case (k, v) => k.getName.equals(UnderReplicatedPartitionMetrics.URPS_CAUSED_BY_BROKER) }
    // check there is a metric with name UnderReplicatedMetrics.URPS_CAUSED_BY_BROKER for both the brokers.
    assertEquals(replicationFactor, urpMetrics.size)

    urpMetrics.foreach {
      case (k, v) =>
        val gauge = v.asInstanceOf[Gauge[Int]]
        val expected = if (k.getScope.equals(UnderReplicatedPartitionMetrics.TAG_BROKER_ID + "." + brokerId)) 2 else 0
        // check expected broker metric has the source of URPs.
        // Metrics should always have broker_id
        assertEquals(expected, gauge.value())
    }
  }

}
