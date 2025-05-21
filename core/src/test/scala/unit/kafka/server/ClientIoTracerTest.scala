/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Collections
import org.apache.kafka.common.metrics.{MetricConfig, Metrics}
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class ClientIoTracerTest {

  @Test
  def testNormalize(): Unit = {
    assertEquals("a_bc_efg__123__456-dc__01", ClientIoTracer.normalize("a bc.efg  123..456-dc .01"))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("Fetch", "Produce", "RemoteFetch"))
  def testGetSensorName(ioType: String): Unit = {
    val clientIoType: ClientIoType = getClientIoType(ioType)
    assertEquals(ioType + "_abc_123_0_topic01_byte-rate", ClientIoTracer.getSensorName(clientIoType, "abc", "123", 0, "topic01", "byte-rate"))
    assertEquals(ioType + "__123_1_topic01_byte-rate", ClientIoTracer.getSensorName(clientIoType, "", "123", 1, "topic01", "byte-rate"))
    assertEquals(ioType + "_abc__3_topic01_byte-rate", ClientIoTracer.getSensorName(clientIoType, "abc", "", 3, "topic01", "byte-rate"))

  }

  @ParameterizedTest
  @ValueSource(strings = Array("Fetch", "Produce", "RemoteFetch"))
  def testRecordByteRate(ioType: String): Unit = {
    val user = "abc"
    val clientId = "123"
    val topicName = "topic01"
    val time = new MockTime
    val metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time)
    try {
      val cit = new ClientIoTracer(metrics)
      val clientIoType: ClientIoType = getClientIoType(ioType)
      cit.recordByteRate(clientIoType, user, clientId, 0, topicName, 1024)
      val metricName1 = metrics.metricName("byte-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("byte-rate"),
        "ioType", ioType,
        "user", user,
        "client-id", clientId,
        "api-version", "0",
        "topic", topicName)
      // Assert that the request was recorded. Since its a rate the value cannot be measured exactly.
      assert(Double.unbox(metrics.metrics().get(metricName1).metricValue()) > 0)

      cit.recordByteRate(clientIoType, "", clientId, 1, topicName, 1024)
      val metricName2 = metrics.metricName("byte-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("byte-rate"),
        "ioType", ioType,
        "user", "",
        "client-id", clientId,
        "api-version", "1",
        "topic", topicName)
      // Assert that the request was recorded. Since its a rate the value cannot be measured exactly.
      assert(Double.unbox(metrics.metrics().get(metricName2).metricValue()) > 0)

      cit.recordByteRate(clientIoType, user, clientId, 2, topicName, 1024)
      val metricName3 = metrics.metricName("byte-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("byte-rate"),
        "ioType", ioType,
        "user", user,
        "client-id", clientId,
        "api-version", "2",
        "topic", topicName)
      // Assert that the request was recorded. Since its a rate the value cannot be measured exactly.
      assert(Double.unbox(metrics.metrics().get(metricName3).metricValue()) > 0)

      cit.recordByteRate(clientIoType, user, "", 3, topicName, 1024)
      val metricName4 = metrics.metricName("byte-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("byte-rate"),
        "ioType", ioType,
        "user", user,
        "client-id", "",
        "api-version", "3",
        "topic", topicName)
      // Assert that the request was recorded. Since its a rate the value cannot be measured exactly.
      assert(Double.unbox(metrics.metrics().get(metricName4).metricValue()) > 0)
    } finally {
      metrics.close()
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("Fetch", "Produce", "RemoteFetch"))
  def testRecordRequestRate(ioType: String): Unit = {
    val user = "abc"
    val clientId = "123"
    val apiVersion = 0.shortValue()
    val topicName = "topic01"
    val time = new MockTime
    val metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time)
    try {
      val cit = new ClientIoTracer(metrics)
      val clientIoType: ClientIoType = getClientIoType(ioType)
      cit.recordRequestRate(clientIoType, user, clientId, apiVersion, topicName)
      val metricName1 = metrics.metricName("request-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("request-rate"),
        "ioType", ioType,
        "user", user,
        "client-id", clientId,
        "api-version", apiVersion.toString,
        "topic", topicName)
      // Assert that the request was recorded. Since its a rate the value will not be exactly 1.
      assert(Double.unbox(metrics.metrics().get(metricName1).metricValue()) > 0)

      cit.recordRequestRate(clientIoType, "", clientId, apiVersion, topicName)
      val metricName2 = metrics.metricName("request-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("request-rate"),
        "ioType", ioType,
        "user", "",
        "client-id", clientId,
        "api-version", apiVersion.toString,
        "topic", topicName)
      // Assert that the request was recorded. Since its a rate the value will not be exactly 1.
      assert(Double.unbox(metrics.metrics().get(metricName2).metricValue()) > 0)
    } finally {
      metrics.close()
    }
  }

  private def getClientIoType(ioType: String) = {
    val clientIoType: ClientIoType = ioType match {
      case "Fetch" => ClientIoType.Fetch
      case "Produce" => ClientIoType.Produce
      case "RemoteFetch" => ClientIoType.RemoteFetch
      case _ => throw new IllegalArgumentException(s"Unknown ioType: $ioType")
    }
    clientIoType
  }
}
