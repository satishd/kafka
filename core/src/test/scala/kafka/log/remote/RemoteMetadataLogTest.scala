package kafka.log.remote

import kafka.server.KafkaConfig
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.{RemoteLogSegmentId, RemoteLogSegmentMetadata}
import org.apache.kafka.common.record.SimpleRecord
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.util.{Collections, Properties, UUID}
import scala.jdk.CollectionConverters._

class RemoteMetadataLogTest {

  val time = new MockTime()

  @Test
  def testFormatRecordKeyValue(): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val timestamp = time.hiResClockMs()

    val metadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicPartition, UUID.randomUUID()), 5,
      10, timestamp, 2, timestamp, 100,
      RemoteLogSegmentMetadata.State.COPY_STARTED, Collections.emptyMap())

    val keyBytes = RemoteMetadataLog.keyToBytes(topicPartition.toString)
    val valueBytes = RemoteMetadataLog.valueToBytes(metadata)
    val remoteLogMetadataRecord = TestUtils.records(Seq(
      new SimpleRecord(keyBytes, valueBytes)
    )).records.asScala.head

    val actual = RemoteMetadataLog.formatRecordKeyAndValue(remoteLogMetadataRecord)
    assertEquals(topicPartition.toString, actual._1.get)
    assertEquals(metadata.toString, actual._2.get)
  }

  @Test
  def testFormatEmptyRecordKeyValue(): Unit = {
    val emptyBytes = Array[Byte]()
    val remoteLogMetadataRecord = TestUtils.records(Seq(
      new SimpleRecord(emptyBytes, null)
    )).records.asScala.head
    val actual = RemoteMetadataLog.formatRecordKeyAndValue(remoteLogMetadataRecord)
    assertTrue(actual._1.get.isEmpty)
    assertEquals("<EMPTY>", actual._2.get)
  }

  @Test
  def testRLMConfig(): Unit = {
    val m: Map[String, String] = Map(
      "advertised.host.name" -> "ducker03",
      "advertised.listeners" -> "SASL_SSL://ducker03:9095",
      "broker.id" -> "1",
      "group.initial.rebalance.delay.ms" -> "100",
      "inter.broker.listener.name" -> "SASL_SSL",
      "listener.security.protocol.map" -> "SASL_SSL:SASL_SSL",
      "listeners" -> "SASL_SSL://:9095",
      "log.dirs, /mnt/kafka/kafka-data-logs-1" -> "/mnt/kafka/kafka-data-logs-2",
      "offsets.topic.num.partitions" -> "3",
      "offsets.topic.replication.factor" -> "3",
      "port" -> "9092",
      "sasl.enabled.mechanisms" -> "GSSAPI",
      "sasl.kerberos.service.name" -> "kafka",
      "sasl.mechanism.inter.broker.protocol" -> "GSSAPI",
      "socket.receive.buffer.bytes" -> "65536",
      "ssl.endpoint.identification.algorithm" -> "HTTPS",
      "ssl.key.password" -> "test-ks-passwd",
      "ssl.keystore.location" -> "/mnt/security/test.keystore.jks",
      "ssl.keystore.password" -> "test-ks-passwd",
      "ssl.keystore.type" -> "JKS",
      "ssl.truststore.location" -> "/mnt/security/test.truststore.jks",
      "ssl.truststore.password" -> "test-ts-passwd",
      "ssl.truststore.type" -> "JKS",
      "zookeeper.connect" -> "ducker02:2181",
      "zookeeper.connection.timeout.ms" -> "18000",
      "zookeeper.session.timeout.ms" -> "18000",
      "zookeeper.set.acl" -> "false",
      "zookeeper.ssl.client.enable" -> "false",
      "zookeeper.ssl.keystore.location" -> "/mnt/security/test.keystore.jks",
      "zookeeper.ssl.keystore.password" -> "test-ks-passwd",
      "zookeeper.ssl.truststore.location" -> "/mnt/security/test.truststore.jks",
      "zookeeper.ssl.truststore.password" -> "test-ts-passwd",
      "remote.log.metadata.hello" -> "world",
      "remote.log.x.y" -> "z"
    )
    val props: Properties = new Properties()
    m.foreach {
      case (k, v) => props.put(k, v)
    }

    val keys: Array[String] = Array("ssl.truststore.location", "sasl.kerberos.service.name", "remote.log.metadata.hello")
    val rlm = RemoteLogManager.createRemoteLogManagerConfig(KafkaConfig.fromProps(props))
    for (key <- keys) {
      assertEquals(m.get(key), rlm.rlmmProps.get(key))
    }
    assertFalse(rlm.rlmmProps.contains("remote.log.x.y"))
    rlm.rlmmProps.foreach {
      case(k, v) => println("%s=%s".format(k, v))
    }
  }
}