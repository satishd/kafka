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
package kafka.log.remote

import java.io.{ByteArrayInputStream, File, InputStream}
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer
import java.util.{Collections, Optional, Properties}
import java.{lang, util}

import kafka.cluster.EndPoint
import kafka.log.remote.RemoteLogManager.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX
import kafka.log._
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.server._
import kafka.server.checkpoints.LazyOffsetCheckpoints
import kafka.utils.{MockScheduler, MockTime, TestUtils}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.easymock.EasyMock
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue, assertFalse}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

class RemoteLogManagerTest {

  val brokerId = 101
  val topicPartition = new TopicPartition("test-topic", 0)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val metrics = new Metrics

  val rsmConfig: Map[String, Any] = Map(REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "url" -> "foo.url",
    REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "timout.ms" -> 1000L)
  val rlmConfig = RemoteLogManagerConfig(remoteLogStorageEnable = true, "kafka.log.remote.MockRemoteStorageManager", "", rsmConfig, 1024, 60000, 2, 10, 10, 30000, "kafka.log.remote.MockRemoteLogMetadataManager", rlmmProps = Map.empty)

  var logConfig: LogConfig = _
  var tmpDir: File = _
  var replicaManager: ReplicaManager = _
  var logManager: LogManager = _
  var rlmMock: RemoteLogManager = EasyMock.createMock(classOf[RemoteLogManager])

  @BeforeEach
  def setup(): Unit = {
    val logProps = createLogProperties(Map.empty)
    logConfig = LogConfig(logProps)

    tmpDir = TestUtils.tempDir()
    val logDir1 = TestUtils.randomPartitionLogDir(tmpDir)
    val logDir2 = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig,
      CleanerConfig(enableCleaner = false), time, rlmConfig)
    logManager.startup()

    val brokerProps = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect)
    brokerProps.put(KafkaConfig.LogDirsProp, Seq(logDir1, logDir2).map(_.getAbsolutePath).mkString(","))
    val brokerConfig = KafkaConfig.fromProps(brokerProps)
    val kafkaZkClient: KafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
    val quotaManagers = QuotaFactory.instantiate(brokerConfig, metrics, time, "")
    val alterIsrManager = TestUtils.createAlterIsrManager()
    replicaManager = new ReplicaManager(
      config = brokerConfig, metrics, time, zkClient = kafkaZkClient, new MockScheduler(time),
      logManager, Option(rlmMock), new AtomicBoolean(false), quotaManagers,
      brokerTopicStats, new MetadataCache(brokerId), new LogDirFailureChannel(brokerConfig.logDirs.size), alterIsrManager)

    EasyMock.expect(kafkaZkClient.getEntityConfigs(EasyMock.anyString(), EasyMock.anyString())).andReturn(
      logProps).anyTimes()
    EasyMock.expect(
      kafkaZkClient.conditionalUpdatePath(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject(),
        EasyMock.anyObject()))
      .andReturn((true, 0)).anyTimes()
    EasyMock.replay(kafkaZkClient)
  }

  @AfterEach
  def tearDown(): Unit = {
    EasyMock.reset(rlmMock)
    brokerTopicStats.close()
    metrics.close()

    logManager.shutdown()
    Utils.delete(tmpDir)
    logManager.liveLogDirs.foreach(Utils.delete)
    replicaManager.shutdown(checkpointHW = false)
  }

  private def createLogProperties(overrides: Map[String, String]): Properties = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 1024: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 10000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 300000: java.lang.Integer)
    overrides.foreach { case (k, v) => logProps.put(k, v) }
    logProps
  }

  @Test
  def testRSMConfigInvocation():Unit = {

    def logFetcher(tp: TopicPartition): Option[Log] = logManager.getLog(tp)

    def lsoUpdater(tp: TopicPartition, los: Long): Unit = {}

    // this should initialize RSM
    val logsDirTmp = Files.createTempDirectory("kafka-").toString
    val remoteLogManager = new RemoteLogManager(logFetcher, lsoUpdater, rlmConfig, time, 1, "", logsDirTmp, new BrokerTopicStats)
    val securityProtocol = SecurityProtocol.PLAINTEXT
    remoteLogManager.onEndpointCreated(EndPoint( "localhost", 9092, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol))

    assertTrue(rsmConfig.count { case (k, v) => MockRemoteStorageManager.configs.get(k) == v } == rsmConfig.size)
    assertEquals(MockRemoteStorageManager.configs.get(KafkaConfig.RemoteLogRetentionBytesProp),
      rlmConfig.remoteLogRetentionBytes)
    assertEquals(MockRemoteStorageManager.configs.get(KafkaConfig.RemoteLogRetentionMillisProp),
      rlmConfig.remoteLogRetentionMillis)
  }

  @Test
  def testRemoteLogRecordsFetch(): Unit = {
    // return the lastOffset to verify when out of range offsets are requested.
    EasyMock.expect(rlmMock.close()).anyTimes()
    EasyMock.replay(rlmMock)

    val leaderEpoch = 1
    val partition = replicaManager.createPartition(topicPartition)

    val leaderState = new LeaderAndIsrPartitionState()
      .setControllerEpoch(1)
      .setLeader(brokerId)
      .setLeaderEpoch(leaderEpoch)
      .setIsr(Collections.singletonList(brokerId))
      .setZkVersion(1)
      .setReplicas(Collections.singletonList(brokerId))
      .setIsNew(true)
    partition.makeLeader(leaderState, new LazyOffsetCheckpoints(replicaManager.highWatermarkCheckpoints))

    val recordsArray = Array(new SimpleRecord("k1".getBytes, "v1".getBytes),
      new SimpleRecord("k2".getBytes, "v2".getBytes),
      new SimpleRecord("k3".getBytes, "v3".getBytes),
      new SimpleRecord("k4".getBytes, "v4".getBytes))
    val inputRecords: util.List[SimpleRecord] = util.Arrays.asList(recordsArray: _*)

    val inputMemoryRecords = Map(topicPartition -> MemoryRecords.withRecords(0L, CompressionType.NONE, leaderEpoch,
      recordsArray: _*))

    replicaManager.appendRecords(timeout = 30000, requiredAcks = 1, internalTopicsAllowed = true,
      origin = AppendOrigin.Client, entriesPerPartition = inputMemoryRecords, responseCallback = _ => ())

    def logReadResultFor(fetchOffset: Long): LogReadResult = {
      val partitionInfo = Seq((topicPartition, new PartitionData(fetchOffset, 0, 1000,
        Optional.of(leaderEpoch))))
      val readRecords = replicaManager.readFromLocalLog(100, fetchOnlyFromLeader = true, FetchTxnCommitted, 1000,
        hardMaxBytesLimit = false, partitionInfo, UnboundedQuota, None)
      if (readRecords.isEmpty) null else readRecords.last._2
    }

    val logReadResult_0 = logReadResultFor(0L)
    val receivedRecords = logReadResult_0.info.records
    // check the records are same
    val result = new util.ArrayList[SimpleRecord]()
    receivedRecords.records().forEach(new Consumer[Record] {
      override def accept(t: Record): Unit = result.add(new SimpleRecord(t))
    })
    assertEquals(inputRecords, result)

    val outOfRangeOffset = logManager.getLog(topicPartition).get.logEndOffset + 1

    // fetching offsets beyond local log would result in fetching from remote log, it is mocked to return lastOffset,
    //nextLocalOffset should be lastOffset +1
    val logReadResult = logReadResultFor(outOfRangeOffset)
    // fetch response should have no records as it is to indicate that the requested fetch messages are in
    // remote tier and the next offset available locally is sent as `nextLocalOffset` so that follower replica can
    // start fetching that for local storage.
    assertTrue(logReadResult.info.records.sizeInBytes() == 0)
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

object MockRemoteStorageManager {
  var configs: util.Map[String, _] = _
}

class MockRemoteStorageManager extends RemoteStorageManager {


  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {
    MockRemoteStorageManager.configs = configs
  }

  override def copyLogSegment(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, logSegmentData: LogSegmentData): Unit = {
  }

  override def fetchLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata,
                                   startPosition: lang.Long,
                                   endPosition: lang.Long): InputStream = new ByteArrayInputStream(Array.emptyByteArray)

  override def fetchOffsetIndex(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): InputStream = new ByteArrayInputStream(
    Array.emptyByteArray)

  override def fetchTimestampIndex(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): InputStream = new ByteArrayInputStream(
    Array.emptyByteArray)

  override def deleteLogSegment(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = {}
}

class MockRemoteLogMetadataManager extends RemoteLogMetadataManager {
  override def putRemoteLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = {}

  override def remoteLogSegmentMetadata(topicPartition: TopicPartition, offset: Long, epochForOffset: Int): RemoteLogSegmentMetadata = {
    null
  }

  override def earliestLogOffset(tp: TopicPartition, leaderEpoch: Int): Optional[lang.Long] = {
    Optional.empty()
  }

  override def highestLogOffset(tp: TopicPartition, leaderEpoch: Int): Optional[lang.Long] = {
    Optional.empty()
  }

  override def deleteRemoteLogSegmentMetadata(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = {}

  override def listRemoteLogSegments(topicPartition: TopicPartition,
                                     minOffset: Long): util.Iterator[RemoteLogSegmentMetadata] = Collections.emptyIterator()

  override def onPartitionLeadershipChanges(leaderPartitions: util.Set[TopicPartition],
                                            followerPartitions: util.Set[TopicPartition]): Unit = {}

  override def onStopPartitions(partitions: util.Set[TopicPartition]): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def close(): Unit = {}
}