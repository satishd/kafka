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

class ClientIoTracerTest {
  @Test
  def testNormalize(): Unit = {
    assertEquals("a_bc_efg__123__456-dc__01", ClientIoTracer.normalize("a bc.efg  123..456-dc .01"))
  }

  @Test
  def testGetSensorName(): Unit = {
    assertEquals("Fetch_abc_123_0_topic01_byte-rate", ClientIoTracer.getSensorName(ClientIoType.Fetch, "abc", "123", 0, "topic01", "byte-rate"))
    assertEquals("Fetch__123_1_topic01_byte-rate", ClientIoTracer.getSensorName(ClientIoType.Fetch, "", "123", 1, "topic01", "byte-rate"))
    assertEquals("Produce_abc_123_2_topic01_byte-rate", ClientIoTracer.getSensorName(ClientIoType.Produce, "abc", "123", 2, "topic01", "byte-rate"))
    assertEquals("Produce_abc__3_topic01_byte-rate", ClientIoTracer.getSensorName(ClientIoType.Produce, "abc", "", 3, "topic01", "byte-rate"))
  }

  @Test
  def testRecordByteRate(): Unit = {
    val user = "abc"
    val clientId = "123"
    val topicName = "topic01"
    val time = new MockTime
    val metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time)
    try {
      val cit = new ClientIoTracer(metrics)
      cit.recordByteRate(ClientIoType.Fetch, user, clientId, 0, topicName, 1024)
      val metricName1 = metrics.metricName("byte-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("byte-rate"),
        "ioType", "Fetch",
        "user", user,
        "client-id", clientId,
        "api-version", "0",
        "topic", topicName)
      // Assert that the request was recorded. Since its a rate the value cannot be measured exactly.
      assert(Double.unbox(metrics.metrics().get(metricName1).metricValue()) > 0)

      cit.recordByteRate(ClientIoType.Fetch, "", clientId, 1, topicName, 1024)
      val metricName2 = metrics.metricName("byte-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("byte-rate"),
        "ioType", "Fetch",
        "user", "",
        "client-id", clientId,
        "api-version", "1",
        "topic", topicName)
      // Assert that the request was recorded. Since its a rate the value cannot be measured exactly.
      assert(Double.unbox(metrics.metrics().get(metricName2).metricValue()) > 0)

      cit.recordByteRate(ClientIoType.Produce, user, clientId, 2, topicName, 1024)
      val metricName3 = metrics.metricName("byte-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("byte-rate"),
        "ioType", "Produce",
        "user", user,
        "client-id", clientId,
        "api-version", "2",
        "topic", topicName)
      // Assert that the request was recorded. Since its a rate the value cannot be measured exactly.
      assert(Double.unbox(metrics.metrics().get(metricName3).metricValue()) > 0)

      cit.recordByteRate(ClientIoType.Produce, user, "", 3, topicName, 1024)
      val metricName4 = metrics.metricName("byte-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("byte-rate"),
        "ioType", "Produce",
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

  @Test
  def testRecordRequestRate(): Unit = {
    val user = "abc"
    val clientId = "123"
    val apiVersion = 0.shortValue()
    val topicName = "topic01"
    val time = new MockTime
    val metrics = new Metrics(new MetricConfig(), Collections.emptyList(), time)
    try {
      val cit = new ClientIoTracer(metrics)
      cit.recordRequestRate(ClientIoType.Fetch, user, clientId, apiVersion, topicName)
      val metricName1 = metrics.metricName("request-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("request-rate"),
        "ioType", "Fetch",
        "user", user,
        "client-id", clientId,
        "api-version", apiVersion.toString,
        "topic", topicName)
      // Assert that the request was recorded. Since its a rate the value will not be exactly 1.
      assert(Double.unbox(metrics.metrics().get(metricName1).metricValue()) > 0)

      cit.recordRequestRate(ClientIoType.Fetch, "", clientId, apiVersion, topicName)
      val metricName2 = metrics.metricName("request-rate", "ClientIoTracer",
        "Tracking %s per (user/client-id, topic)".format("request-rate"),
        "ioType", "Fetch",
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
}
