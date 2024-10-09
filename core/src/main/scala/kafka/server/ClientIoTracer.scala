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

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.regex.Pattern
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.stats.Rate
import org.apache.kafka.common.metrics.{Metrics, Sensor}

/**
  * Keep track of I/O traffic of each clientId
  */

object ClientIoType {
  case object Fetch extends ClientIoType
  case object Produce extends ClientIoType
}

sealed trait ClientIoType

object ClientIoTracer {
  private val pattern = Pattern.compile("[\\. ]")

  def normalize(id: String): String = {
    pattern.matcher(id).replaceAll("_")
  }

  def getSensorName(ioType: ClientIoType, user: String, clientId: String, apiVersion: Short, topic: String, name: String): String = {
    String.join("_", ioType.toString, user, clientId, apiVersion.toString, topic, name)
  }
}

class ClientIoTracer(private val metrics: Metrics) {
  private val lock = new ReentrantReadWriteLock()
  private val sensorAccessor = new SensorAccess(lock, metrics)


  // Purge sensors after 1 hour of inactivity
  private val InactiveSensorExpirationTimeSeconds = 3600

  def recordByteRate(ioType: ClientIoType, user: String, clientId: String, apiVersion: Short, topic: String, sizeInBytes: Int): Unit = {
    val (normalizedUser: String, normalizedClientId: String, normalizedTopic: String) = normalizeTags(user, clientId, topic)
    val sensor = getOrCreateByteRateSensor(ioType, normalizedUser, normalizedClientId, apiVersion, normalizedTopic)
    sensor.record(sizeInBytes)
  }

  def recordRequestRate(ioType: ClientIoType, user: String, clientId: String, apiVersion: Short, topic: String): Unit = {
    val (normalizedUser: String, normalizedClientId: String, normalizedTopic: String) = normalizeTags(user, clientId, topic)
    val sensor = getOrCreateRequestRateSensor(ioType, normalizedUser, normalizedClientId, apiVersion, normalizedTopic)
    sensor.record()
  }

  private def normalizeTags(user: String, clientId: String, topic: String) = {
    val normalizedUser = if (user == null || user.equalsIgnoreCase("ANONYMOUS")) "" else ClientIoTracer.normalize(user)
    val normalizedClientId = if (clientId == null) "" else ClientIoTracer.normalize(clientId)
    val normalizedTopic = if (topic == null) "" else ClientIoTracer.normalize(topic)
    (normalizedUser, normalizedClientId, normalizedTopic)
  }

  private def getOrCreateByteRateSensor(ioType: ClientIoType, user: String, clientId: String, apiVersion: Short, topic: String): Sensor = {
    sensorAccessor.getOrCreate(
      ClientIoTracer.getSensorName(ioType, user, clientId, apiVersion, topic, "byte-rate"),
      InactiveSensorExpirationTimeSeconds,
      sensor => sensor.add(getMetricName(ioType, user, clientId, apiVersion, topic, "byte-rate"), new Rate())
    )
  }

  private def getOrCreateRequestRateSensor(ioType: ClientIoType, user: String, clientId: String, apiVersion: Short, topic: String): Sensor = {
    sensorAccessor.getOrCreate(
      ClientIoTracer.getSensorName(ioType, user, clientId, apiVersion, topic, "request-rate"),
      InactiveSensorExpirationTimeSeconds,
      sensor => sensor.add(getMetricName(ioType, user, clientId, apiVersion, topic, "request-rate"), new Rate())
    )
  }

  private def getMetricName(ioType: ClientIoType, user: String, clientId: String, apiVersion: Short, topic: String, name: String): MetricName = {
    metrics.metricName(name, "ClientIoTracer",
      "Tracking %s per (user/client-id, topic)".format(name),
      "ioType", ioType.toString,
      "user", user,
      "client-id", clientId,
      "api-version", apiVersion.toString,
      "topic", topic)
  }
}
