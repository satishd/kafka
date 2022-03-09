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

import kafka.cluster.Partition
import kafka.log._
import kafka.server._
import kafka.server.checkpoints.{LeaderEpochCheckpoint, LeaderEpochCheckpointFile}
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.errors.OffsetOutOfRangeException
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{KafkaException, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
import org.apache.kafka.server.log.remote.storage._
import org.easymock.{CaptureType, EasyMock}
import org.easymock.EasyMock._
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}
import java.util.{Collections, Optional, Properties}
import java.{lang, util}

import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

class RemoteLogManagerTest {

  val clusterId = "test-cluster-id"
  val brokerId = 0
  val topicPartition = new TopicPartition("test-topic", 0)
  val topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), topicPartition)
  val time = new MockTime()
  val brokerTopicStats = new BrokerTopicStats
  val logsDir: String = Files.createTempDirectory("kafka-").toString
  val cache = new LeaderEpochFileCache(topicPartition, () => 0, checkpoint())
  val rlmConfig: RemoteLogManagerConfig = createRLMConfig()

  @AfterEach
  def afterEach(): Unit = {
    Utils.delete(Paths.get(logsDir).toFile)
  }

  @Test
  def testRLMConfig(): Unit = {
    val key = "hello"
    val rlmmConfigPrefix = RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX
    val props: Properties = new Properties()
    props.put(rlmmConfigPrefix + key, "world")
    props.put("remote.log.metadata.y", "z")

    val rlmConfig = createRLMConfig(props)
    val rlmmConfig = rlmConfig.remoteLogMetadataManagerProps()
    assertEquals(props.get(rlmmConfigPrefix + key), rlmmConfig.get(key))
    assertFalse(rlmmConfig.containsKey("remote.log.metadata.y"))
  }

  @Test
  def testTopicIdCacheUpdates(): Unit = {
    val leaderTopicIdPartition =  new TopicIdPartition(Uuid.randomUuid(),
      new TopicPartition("Leader", 0))
    val followerTopicIdPartition = new TopicIdPartition(Uuid.randomUuid(),
      new TopicPartition("Follower", 0))
    val topicIds: util.Map[String, Uuid] = Map(
      leaderTopicIdPartition.topicPartition().topic() -> leaderTopicIdPartition.topicId(),
      followerTopicIdPartition.topicPartition().topic() -> followerTopicIdPartition.topicId()
    ).asJava

    def mockPartition(topicIdPartition: TopicIdPartition) = {
      val tp = topicIdPartition.topicPartition()

      val partition: Partition = niceMock(classOf[Partition])
      expect(partition.topicPartition).andReturn(tp).anyTimes()
      expect(partition.topic).andReturn(tp.topic()).anyTimes()
      expect(partition.log).andReturn(None).anyTimes()

      partition
    }

    val mockLeaderPartition = mockPartition(leaderTopicIdPartition)
    val mockFollowerPartition = mockPartition(followerTopicIdPartition)

    val rlmm: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmm.listRemoteLogSegments(anyObject()))
      .andAnswer(() => Iterator[RemoteLogSegmentMetadata]().asJava).anyTimes()

    replay(rlmm, mockLeaderPartition, mockFollowerPartition)

    val remoteLogManager = new RemoteLogManager(_ => None,
      (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmm
    }

    // We use the internal implementation details of fetchRemoteLogSegmentMetadata to check that the topic is
    // in the cache
    def verifyInCache(topicIdPartitions: TopicIdPartition*): Unit = {
      topicIdPartitions.foreach(topicIdPartition => {
        assertDoesNotThrow(() => remoteLogManager.fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), 0, 0))
      })
    }

    def verifyNotInCache(topicIdPartitions: TopicIdPartition*): Unit = {
      topicIdPartitions.foreach(topicIdPartition => {
        assertThrows(classOf[KafkaException],
          () => remoteLogManager.fetchRemoteLogSegmentMetadata(topicIdPartition.topicPartition(), 0, 0))
      })
    }

    verifyNotInCache(followerTopicIdPartition, leaderTopicIdPartition)
    // Load cache
    remoteLogManager.onLeadershipChange(Set(mockLeaderPartition), Set(mockFollowerPartition), topicIds)
    verifyInCache(followerTopicIdPartition, leaderTopicIdPartition)

    // Evict from cache
    remoteLogManager.stopPartitions(Set(leaderTopicIdPartition.topicPartition()), delete = true, (_, _) => {})
    verifyNotInCache(leaderTopicIdPartition)
    verifyInCache(followerTopicIdPartition)

    // Evict from cache
    remoteLogManager.stopPartitions(Set(followerTopicIdPartition.topicPartition()), delete = true, (_, _) => {})
    verifyNotInCache(leaderTopicIdPartition, followerTopicIdPartition)
  }

  @Test
  def testFindHighestRemoteOffsetOnEmptyRemoteStorage(): Unit = {
    cache.assign(0, 0)
    cache.assign(1, 500)

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache))

    val rlmmManager: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()

    replay(log, rlmmManager)
    val remoteLogManager = new RemoteLogManager(_ => Option(log),
      (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
    }
    assertEquals(-1L, remoteLogManager.findHighestRemoteOffset(topicIdPartition))
  }

  @Test
  def testFindHighestRemoteOffset(): Unit = {
    cache.assign(0, 0)
    cache.assign(1, 500)

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache))

    val rlmmManager: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt())).andAnswer(() => {
      val epoch = getCurrentArgument[Int](1)
      if (epoch == 0) Optional.of(200) else Optional.empty()
    }).anyTimes()

    replay(log, rlmmManager)
    val remoteLogManager = new RemoteLogManager(_ => Option(log),
      (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
    }
    assertEquals(200L, remoteLogManager.findHighestRemoteOffset(topicIdPartition))
  }

  @Test
  def testFindNextSegmentMetadata(): Unit = {
    cache.assign(0, 0)
    cache.assign(1, 30)
    cache.assign(2, 100)

    val logConfig: LogConfig = createNiceMock(classOf[LogConfig])
    expect(logConfig.compact).andReturn(false).anyTimes()
    expect(logConfig.remoteStorageEnable).andReturn(true).anyTimes()

    val log: Log = createNiceMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    expect(log.config).andReturn(logConfig).anyTimes()

    val nextSegmentLeaderEpochs = new util.HashMap[Integer, lang.Long]
    nextSegmentLeaderEpochs.put(0, 0)
    nextSegmentLeaderEpochs.put(1, 30)
    nextSegmentLeaderEpochs.put(2, 100)
    val nextSegmentMetadata: RemoteLogSegmentMetadata =
      new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
        100, 199, -1L, brokerId, -1L, 1024, nextSegmentLeaderEpochs)
    val rlmmManager: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.remoteLogSegmentMetadata(EasyMock.eq(topicIdPartition), anyInt(), anyLong()))
      .andAnswer(() => {
        val epoch = getCurrentArgument[Int](1)
        val nextOffset = getCurrentArgument[Long](2)
        if (epoch == 2 && nextOffset >= 100L && nextOffset <= 199L)
          Optional.of(nextSegmentMetadata)
        else
          Optional.empty()
      }).anyTimes()
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(topicIdPartition)).andReturn(Collections.emptyIterator()).anyTimes()

    val topic = topicIdPartition.topicPartition().topic()
    val partition: Partition = createMock(classOf[Partition])
    expect(partition.topic).andReturn(topic).anyTimes()
    expect(partition.topicPartition).andReturn(topicIdPartition.topicPartition()).anyTimes()
    expect(partition.log).andReturn(Option(log)).anyTimes()
    expect(partition.getLeaderEpoch).andReturn(0).anyTimes()
    replay(logConfig, log, partition, rlmmManager)

    val remoteLogManager = new RemoteLogManager(_ => Option(log),
      (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
    }
    remoteLogManager.onLeadershipChange(Set(partition), Set(), Collections.singletonMap(topic, topicIdPartition.topicId()))

    val segmentLeaderEpochs = new util.HashMap[Integer, lang.Long]()
    segmentLeaderEpochs.put(0, 0)
    segmentLeaderEpochs.put(1, 30)
    // end offset is set to 99, the next offset to search in remote storage is 100
    var segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      0, 99, -1L, brokerId, -1L, 1024, segmentLeaderEpochs)
    assertEquals(Option(nextSegmentMetadata), remoteLogManager.findNextSegmentMetadata(segmentMetadata))

    // end offset is set to 105, the next offset to search in remote storage is 106
    segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      0, 105, -1L, brokerId, -1L, 1024, segmentLeaderEpochs)
    assertEquals(Option(nextSegmentMetadata), remoteLogManager.findNextSegmentMetadata(segmentMetadata))

    segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      0, 200, -1L, brokerId, -1L, 1024, segmentLeaderEpochs)
    assertEquals(None, remoteLogManager.findNextSegmentMetadata(segmentMetadata))
  }

  @Test
  def testAddAbortedTransactions(): Unit = {
    val baseOffset = 45
    val timeIdx = new TimeIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 30 * 12)
    val txnIdx = new TransactionIndex(baseOffset, TestUtils.tempFile())
    val offsetIdx = new OffsetIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 4 * 8)
    offsetIdx.append(baseOffset + 0, 0)
    offsetIdx.append(baseOffset + 1, 100)
    offsetIdx.append(baseOffset + 2, 200)
    offsetIdx.append(baseOffset + 3, 300)

    val rsmManager: ClassLoaderAwareRemoteStorageManager = createNiceMock(classOf[ClassLoaderAwareRemoteStorageManager])
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.OFFSET))).andReturn(new FileInputStream(offsetIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TIMESTAMP))).andReturn(new FileInputStream(timeIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TRANSACTION))).andReturn(new FileInputStream(txnIdx.file)).anyTimes()

    val records: Records = createNiceMock(classOf[Records])
    expect(records.sizeInBytes()).andReturn(150).anyTimes()
    val fetchDataInfo = FetchDataInfo(LogOffsetMetadata(baseOffset, Log.UnknownOffset, 0), records)

    var upperBoundOffsetCapture: Option[Long] = None

    replay(rsmManager, records)
    val remoteLogManager =
      new RemoteLogManager(_ => None, (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
        override private[remote] def collectAbortedTransactions(startOffset: Long,
                                                                upperBoundOffset: Long,
                                                                segmentMetadata: RemoteLogSegmentMetadata,
                                                                accumulator: List[AbortedTxn] => Unit): Unit = {
          upperBoundOffsetCapture = Option(upperBoundOffset)
        }
      }

    val segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      45, 99, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 45))

    // If base-offset=45 and fetch-size=150, then the upperBoundOffset=47
    val actualFetchDataInfo = remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)
    assertTrue(actualFetchDataInfo.abortedTransactions.isDefined)
    assertTrue(actualFetchDataInfo.abortedTransactions.get.isEmpty)
    assertEquals(Option(47), upperBoundOffsetCapture)

    // If base-offset=45 and fetch-size=301, then the entry won't exists in the offset index, returns next
    // remote/local segment base offset.
    upperBoundOffsetCapture = None
    reset(records)
    expect(records.sizeInBytes()).andReturn(301).anyTimes()
    replay(records)
    remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)
    assertEquals(Option(100), upperBoundOffsetCapture)
  }

  @Test
  def testCollectAbortedTransactionsIteratesNextRemoteSegment(): Unit = {
    cache.assign(0, 0)

    val logConfig: LogConfig = createNiceMock(classOf[LogConfig])
    expect(logConfig.compact).andReturn(false).anyTimes()
    expect(logConfig.remoteStorageEnable).andReturn(true).anyTimes()

    val log: Log = createNiceMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    expect(log.config).andReturn(logConfig).anyTimes()
    expect(log.logSegments).andReturn(Iterable.empty).anyTimes()

    val baseOffset = 45
    val timeIdx = new TimeIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 30 * 12)
    val txnIdx = new TransactionIndex(baseOffset, TestUtils.tempFile())
    val offsetIdx = new OffsetIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 4 * 8)
    offsetIdx.append(baseOffset + 0, 0)
    offsetIdx.append(baseOffset + 1, 100)
    offsetIdx.append(baseOffset + 2, 200)
    offsetIdx.append(baseOffset + 3, 300)

    val nextTxnIdx = new TransactionIndex(100L, TestUtils.tempFile())
    val abortedTxns = List(
      new AbortedTxn(producerId = 0L, firstOffset = 50, lastOffset = 105, lastStableOffset = 60),
      new AbortedTxn(producerId = 1L, firstOffset = 55, lastOffset = 120, lastStableOffset = 100)
    )
    abortedTxns.foreach(nextTxnIdx.append)

    val nextSegmentMetadata: RemoteLogSegmentMetadata =
      new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
        100, 199, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 100))
    val rlmmManager: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.remoteLogSegmentMetadata(EasyMock.eq(topicIdPartition), anyInt(), anyLong()))
      .andAnswer(() => {
        val epoch = getCurrentArgument[Int](1)
        val nextOffset = getCurrentArgument[Long](2)
        if (epoch == 0 && nextOffset >= 100L && nextOffset <= 199L)
          Optional.of(nextSegmentMetadata)
        else
          Optional.empty()
      }).anyTimes()
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(topicIdPartition)).andReturn(Collections.emptyIterator()).anyTimes()

    val rsmManager: ClassLoaderAwareRemoteStorageManager = createNiceMock(classOf[ClassLoaderAwareRemoteStorageManager])
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.OFFSET))).andReturn(new FileInputStream(offsetIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TIMESTAMP))).andReturn(new FileInputStream(timeIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TRANSACTION))).andAnswer(() => {
      val segmentMetadata = getCurrentArgument[RemoteLogSegmentMetadata](0)
      if (segmentMetadata.equals(nextSegmentMetadata)) {
        new FileInputStream(nextTxnIdx.file)
      } else {
        new FileInputStream(txnIdx.file)
      }
    }).anyTimes()

    val records: Records = createNiceMock(classOf[Records])
    expect(records.sizeInBytes()).andReturn(301).anyTimes()
    val fetchDataInfo = FetchDataInfo(LogOffsetMetadata(baseOffset, Log.UnknownOffset, 0), records)

    val topic = topicIdPartition.topicPartition().topic()
    val partition: Partition = createMock(classOf[Partition])
    expect(partition.topic).andReturn(topic).anyTimes()
    expect(partition.topicPartition).andReturn(topicIdPartition.topicPartition()).anyTimes()
    expect(partition.log).andReturn(Option(log)).anyTimes()
    expect(partition.getLeaderEpoch).andReturn(0).anyTimes()

    replay(logConfig, log, rlmmManager, rsmManager, records, partition)
    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
      }
    remoteLogManager.onLeadershipChange(Set(partition), Set(), Collections.singletonMap(topic, topicIdPartition.topicId()))

    // If base-offset=45 and fetch-size=301, then the entry won't exists in the offset index, returns next
    // remote/local segment base offset.
    val segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      45, 99, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 45))
    val expectedFetchDataInfo = remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)

    assertTrue(expectedFetchDataInfo.abortedTransactions.isDefined)
    assertEquals(abortedTxns.map(_.asAbortedTransaction), expectedFetchDataInfo.abortedTransactions.get)
  }

  @Test
  def testCollectAbortedTransactionsIteratesNextLocalSegment(): Unit = {
    cache.assign(0, 0)

    val logConfig: LogConfig = createNiceMock(classOf[LogConfig])
    expect(logConfig.compact).andReturn(false).anyTimes()
    expect(logConfig.remoteStorageEnable).andReturn(true).anyTimes()

    val baseOffset = 45
    val timeIdx = new TimeIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 30 * 12)
    val txnIdx = new TransactionIndex(baseOffset, TestUtils.tempFile())
    val offsetIdx = new OffsetIndex(nonExistentTempFile(), baseOffset, maxIndexSize = 4 * 8)
    offsetIdx.append(baseOffset + 0, 0)
    offsetIdx.append(baseOffset + 1, 100)
    offsetIdx.append(baseOffset + 2, 200)
    offsetIdx.append(baseOffset + 3, 300)

    val nextTxnIdx = new TransactionIndex(100L, TestUtils.tempFile())
    val abortedTxns = List(
      new AbortedTxn(producerId = 0L, firstOffset = 50, lastOffset = 105, lastStableOffset = 60),
      new AbortedTxn(producerId = 1L, firstOffset = 55, lastOffset = 120, lastStableOffset = 100)
    )
    abortedTxns.foreach(nextTxnIdx.append)

    val logSegment: LogSegment = createNiceMock(classOf[LogSegment])
    expect(logSegment.txnIndex).andReturn(nextTxnIdx).anyTimes()

    val log: Log = createNiceMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    expect(log.config).andReturn(logConfig).anyTimes()
    expect(log.logSegments).andReturn(List(logSegment)).anyTimes()

    val rlmmManager: RemoteLogMetadataManager = createNiceMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.remoteLogSegmentMetadata(EasyMock.eq(topicIdPartition), anyInt(), anyLong()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(topicIdPartition)).andReturn(Collections.emptyIterator()).anyTimes()

    val rsmManager: ClassLoaderAwareRemoteStorageManager = createNiceMock(classOf[ClassLoaderAwareRemoteStorageManager])
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.OFFSET))).andReturn(new FileInputStream(offsetIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TIMESTAMP))).andReturn(new FileInputStream(timeIdx.file)).anyTimes()
    expect(rsmManager.fetchIndex(anyObject(), EasyMock.eq(IndexType.TRANSACTION))).andReturn(new FileInputStream(txnIdx.file)).anyTimes()

    val records: Records = createNiceMock(classOf[Records])
    expect(records.sizeInBytes()).andReturn(301).anyTimes()
    val fetchDataInfo = FetchDataInfo(LogOffsetMetadata(baseOffset, Log.UnknownOffset, 0), records)

    val topic = topicIdPartition.topicPartition().topic()
    val partition: Partition = createMock(classOf[Partition])
    expect(partition.topic).andReturn(topic).anyTimes()
    expect(partition.topicPartition).andReturn(topicIdPartition.topicPartition()).anyTimes()
    expect(partition.log).andReturn(Option(log)).anyTimes()
    expect(partition.getLeaderEpoch).andReturn(0).anyTimes()

    replay(logConfig, logSegment, log, rlmmManager, rsmManager, records, partition)
    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteLogMetadataManager(): RemoteLogMetadataManager = rlmmManager
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
      }
    remoteLogManager.onLeadershipChange(Set(partition), Set(), Collections.singletonMap(topic, topicIdPartition.topicId()))

    // If base-offset=45 and fetch-size=301, then the entry won't exists in the offset index, returns next
    // remote/local segment base offset.
    val segmentMetadata = new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
      45, 99, -1L, brokerId, -1L, 1024, Collections.singletonMap(0, 45))
    val expectedFetchDataInfo = remoteLogManager.addAbortedTransactions(baseOffset, segmentMetadata, fetchDataInfo)

    assertTrue(expectedFetchDataInfo.abortedTransactions.isDefined)
    assertEquals(abortedTxns.map(_.asAbortedTransaction), expectedFetchDataInfo.abortedTransactions.get)
  }

  @Test
  def testGetClassLoaderAwareRemoteStorageManager(): Unit = {
    val rsmManager: ClassLoaderAwareRemoteStorageManager = createNiceMock(classOf[ClassLoaderAwareRemoteStorageManager])
    val remoteLogManager =
      new RemoteLogManager(_ => None, (_, _) => {}, rlmConfig, time, 1, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
      }
    assertEquals(rsmManager, remoteLogManager.storageManager())
  }

  @ParameterizedTest(name = "testDeleteLogSegmentDueToRetentionTimeBreach segmentCount={0} deletableSegmentCount={1}")
  @CsvSource(value = Array("50, 0", "50, 1", "50, 23", "50, 50"))
  def testDeleteLogSegmentDueToRetentionTimeBreach(segmentCount: Int, deletableSegmentCount: Int): Unit = {
    val recordsPerSegment = 100
    val epochCheckpoints = Seq(0 -> 0, 1 -> 20, 3 -> 50, 4 -> 100)
    epochCheckpoints.foreach { case (epoch, startOffset) => cache.assign(epoch, startOffset) }
    val currentLeaderEpoch = epochCheckpoints.last._1

    val logConfig: LogConfig = createMock(classOf[LogConfig])
    expect(logConfig.retentionMs).andReturn(1).anyTimes()
    expect(logConfig.retentionSize).andReturn(-1).anyTimes()

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    expect(log.config).andReturn(logConfig).anyTimes()
    expect(log.size).andReturn(0).anyTimes()

    var logStartOffset: Option[Long] = None
    val rsmManager: ClassLoaderAwareRemoteStorageManager = createMock(classOf[ClassLoaderAwareRemoteStorageManager])
    val rlmmManager: RemoteLogMetadataManager = createMock(classOf[RemoteLogMetadataManager])
    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, startOffset) => logStartOffset = Option(startOffset), rlmConfig, time,
        brokerId, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
        override private[remote] def createRemoteLogMetadataManager() = rlmmManager
      }
    val segmentMetadataList = listRemoteLogSegmentMetadataByTime(segmentCount, deletableSegmentCount, recordsPerSegment)
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(topicIdPartition)).andReturn(segmentMetadataList.iterator.asJava).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(EasyMock.eq(topicIdPartition), anyInt())).andAnswer(() => {
      val leaderEpoch = getCurrentArgument[Int](1)
      if (leaderEpoch == 0)
        segmentMetadataList.take(1).iterator.asJava
      else if (leaderEpoch == 4)
        segmentMetadataList.drop(1).iterator.asJava
      else
        Collections.emptyIterator()
    }).anyTimes()
    expect(rlmmManager.updateRemoteLogSegmentMetadata(anyObject(classOf[RemoteLogSegmentMetadataUpdate]))).anyTimes()

    val args1 = newCapture[RemoteLogSegmentMetadata](CaptureType.ALL)
    expect(rsmManager.deleteLogSegmentData(capture(args1))).anyTimes()
    replay(logConfig, log, rlmmManager, rsmManager)

    val rlmTask = new remoteLogManager.RLMTask(topicIdPartition)
    rlmTask.convertToLeader(currentLeaderEpoch)
    rlmTask.handleExpiredRemoteLogSegments()

    assertEquals(deletableSegmentCount, args1.getValues.size())
    if (deletableSegmentCount > 0) {
      val expectedEndMetadata = segmentMetadataList(deletableSegmentCount-1)
      assertEquals(segmentMetadataList.head, args1.getValues.asScala.head)
      assertEquals(expectedEndMetadata, args1.getValues.asScala.reverse.head)
      assertEquals(expectedEndMetadata.endOffset()+1, logStartOffset.get)
    }
    verify(logConfig, log, rlmmManager, rsmManager)
  }

  @ParameterizedTest(name = "testDeleteLogSegmentDueToRetentionSizeBreach segmentCount={0} deletableSegmentCount={1}")
  @CsvSource(value = Array("50, 0", "50, 1", "50, 23", "50, 50"))
  def testDeleteLogSegmentDueToRetentionSizeBreach(segmentCount: Int, deletableSegmentCount: Int): Unit = {
    val recordsPerSegment = 100
    val epochCheckpoints = Seq(0 -> 0, 1 -> 20, 3 -> 50, 4 -> 100)
    epochCheckpoints.foreach { case (epoch, startOffset) => cache.assign(epoch, startOffset) }
    val currentLeaderEpoch = epochCheckpoints.last._1

    val localLogSegmentsSize = 500L
    val retentionSize = (segmentCount - deletableSegmentCount) * 100 + localLogSegmentsSize
    val logConfig: LogConfig = createMock(classOf[LogConfig])
    expect(logConfig.retentionMs).andReturn(-1).anyTimes()
    expect(logConfig.retentionSize).andReturn(retentionSize).anyTimes()

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    expect(log.config).andReturn(logConfig).anyTimes()
    expect(log.size).andReturn(localLogSegmentsSize).anyTimes()

    var logStartOffset: Option[Long] = None
    val rsmManager: ClassLoaderAwareRemoteStorageManager = createMock(classOf[ClassLoaderAwareRemoteStorageManager])
    val rlmmManager: RemoteLogMetadataManager = createMock(classOf[RemoteLogMetadataManager])
    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, startOffset) => logStartOffset = Option(startOffset), rlmConfig, time,
        brokerId, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
        override private[remote] def createRemoteLogMetadataManager() = rlmmManager
      }
    val segmentMetadataList = listRemoteLogSegmentMetadata(segmentCount, recordsPerSegment)
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(topicIdPartition)).andReturn(segmentMetadataList.iterator.asJava).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(EasyMock.eq(topicIdPartition), anyInt())).andAnswer(() => {
      val leaderEpoch = getCurrentArgument[Int](1)
      if (leaderEpoch == 0)
        segmentMetadataList.take(1).iterator.asJava
      else if (leaderEpoch == 4)
        segmentMetadataList.drop(1).iterator.asJava
      else
        Collections.emptyIterator()
    }).anyTimes()
    expect(rlmmManager.updateRemoteLogSegmentMetadata(anyObject(classOf[RemoteLogSegmentMetadataUpdate]))).anyTimes()

    val args1 = newCapture[RemoteLogSegmentMetadata](CaptureType.ALL)
    expect(rsmManager.deleteLogSegmentData(capture(args1))).anyTimes()
    replay(logConfig, log, rlmmManager, rsmManager)

    val rlmTask = new remoteLogManager.RLMTask(topicIdPartition)
    rlmTask.convertToLeader(currentLeaderEpoch)
    rlmTask.handleExpiredRemoteLogSegments()

    assertEquals(deletableSegmentCount, args1.getValues.size())
    if (deletableSegmentCount > 0) {
      val expectedEndMetadata = segmentMetadataList(deletableSegmentCount-1)
      assertEquals(segmentMetadataList.head, args1.getValues.asScala.head)
      assertEquals(expectedEndMetadata, args1.getValues.asScala.reverse.head)
      assertEquals(expectedEndMetadata.endOffset()+1, logStartOffset.get)
    }
    verify(logConfig, log, rlmmManager, rsmManager)
  }

  @ParameterizedTest(name = "testDeleteLogSegmentDueToRetentionTimeAndSizeBreach segmentCount={0} deletableSegmentCountByTime={1} deletableSegmentCountBySize={2}")
  @CsvSource(value = Array("50, 0, 0", "50, 1, 5", "50, 23, 15", "50, 50, 50"))
  def testDeleteLogSegmentDueToRetentionTimeAndSizeBreach(segmentCount: Int,
                                                          deletableSegmentCountByTime: Int,
                                                          deletableSegmentCountBySize: Int): Unit = {
    val recordsPerSegment = 100
    val epochCheckpoints = Seq(0 -> 0, 1 -> 20, 3 -> 50, 4 -> 100)
    epochCheckpoints.foreach { case (epoch, startOffset) => cache.assign(epoch, startOffset) }
    val currentLeaderEpoch = epochCheckpoints.last._1

    val localLogSegmentsSize = 500L
    val retentionSize = (segmentCount - deletableSegmentCountBySize) * 100 + localLogSegmentsSize
    val logConfig: LogConfig = createMock(classOf[LogConfig])
    expect(logConfig.retentionMs).andReturn(1).anyTimes()
    expect(logConfig.retentionSize).andReturn(retentionSize).anyTimes()

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    expect(log.config).andReturn(logConfig).anyTimes()
    expect(log.size).andReturn(localLogSegmentsSize).anyTimes()

    var logStartOffset: Option[Long] = None
    val rsmManager: ClassLoaderAwareRemoteStorageManager = createMock(classOf[ClassLoaderAwareRemoteStorageManager])
    val rlmmManager: RemoteLogMetadataManager = createMock(classOf[RemoteLogMetadataManager])
    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, startOffset) => logStartOffset = Option(startOffset), rlmConfig, time,
        brokerId, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
        override private[remote] def createRemoteLogMetadataManager() = rlmmManager
      }
    val segmentMetadataList = listRemoteLogSegmentMetadataByTime(segmentCount, deletableSegmentCountByTime, recordsPerSegment)
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), anyInt()))
      .andReturn(Optional.empty()).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(topicIdPartition)).andReturn(segmentMetadataList.iterator.asJava).anyTimes()
    expect(rlmmManager.listRemoteLogSegments(EasyMock.eq(topicIdPartition), anyInt())).andAnswer(() => {
      val leaderEpoch = getCurrentArgument[Int](1)
      if (leaderEpoch == 0)
        segmentMetadataList.take(1).iterator.asJava
      else if (leaderEpoch == 4)
        segmentMetadataList.drop(1).iterator.asJava
      else
        Collections.emptyIterator()
    }).anyTimes()
    expect(rlmmManager.updateRemoteLogSegmentMetadata(anyObject(classOf[RemoteLogSegmentMetadataUpdate]))).anyTimes()

    val args1 = newCapture[RemoteLogSegmentMetadata](CaptureType.ALL)
    expect(rsmManager.deleteLogSegmentData(capture(args1))).anyTimes()
    replay(logConfig, log, rlmmManager, rsmManager)

    val rlmTask = new remoteLogManager.RLMTask(topicIdPartition)
    rlmTask.convertToLeader(currentLeaderEpoch)
    rlmTask.handleExpiredRemoteLogSegments()

    val deletableSegmentCount = Math.max(deletableSegmentCountBySize, deletableSegmentCountByTime)
    assertEquals(deletableSegmentCount, args1.getValues.size())
    if (deletableSegmentCount > 0) {
      val expectedEndMetadata = segmentMetadataList(deletableSegmentCount-1)
      assertEquals(segmentMetadataList.head, args1.getValues.asScala.head)
      assertEquals(expectedEndMetadata, args1.getValues.asScala.reverse.head)
      assertEquals(expectedEndMetadata.endOffset()+1, logStartOffset.get)
    }
    verify(logConfig, log, rlmmManager, rsmManager)
  }

  @Test
  def testGetLeaderEpochCheckpoint(): Unit = {
    val epochs = Seq(EpochEntry(0, 33), EpochEntry(1, 43), EpochEntry(2, 99), EpochEntry(3, 105))
    epochs.foreach(epochEntry => cache.assign(epochEntry.epoch, epochEntry.startOffset))

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    replay(log)
    val remoteLogManager = new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, brokerId = 1,
      clusterId, logsDir, brokerTopicStats)

    var actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = -1, endOffset = 200).read()
    assertEquals(epochs.take(4), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = -1, endOffset = 105).read()
    assertEquals(epochs.take(3), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = -1, endOffset = 100).read()
    assertEquals(epochs.take(3), actual)

    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 34, endOffset = 200).read()
    assertEquals(Seq(EpochEntry(0, 34)) ++ epochs.slice(1, 4), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 43, endOffset = 200).read()
    assertEquals(epochs.slice(1, 4), actual)

    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 34, endOffset = 100).read()
    assertEquals(Seq(EpochEntry(0, 34)) ++ epochs.slice(1, 3), actual)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 34, endOffset = 30).read()
    assertTrue(actual.isEmpty)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 101, endOffset = 101).read()
    assertTrue(actual.isEmpty)
    actual = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = 101, endOffset = 102).read()
    assertEquals(Seq(EpochEntry(2, 101)), actual)
  }

  @Test
  def testGetLeaderEpochCheckpointEncoding(): Unit = {
    val epochs = Seq(EpochEntry(0, 33), EpochEntry(1, 43), EpochEntry(2, 99), EpochEntry(3, 105))
    epochs.foreach(epochEntry => cache.assign(epochEntry.epoch, epochEntry.startOffset))

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    replay(log)
    val remoteLogManager = new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, brokerId = 1,
      clusterId, logsDir, brokerTopicStats)

    val tmpFile = TestUtils.tempFile()
    val checkpoint = remoteLogManager.getLeaderEpochCheckpoint(log, startOffset = -1, endOffset = 200)
    assertEquals(epochs, checkpoint.read())

    Files.write(tmpFile.toPath, checkpoint.readAsByteBuffer().array())
    assertEquals(epochs, new LeaderEpochCheckpointFile(tmpFile).read())
  }

  /**
    * This test verifies the calculation of the tier lag associated to a topic-partition after upload to the
    * remote storage.
    */
  @Test
  def updateTotalTierLagAfterSegmentUpload(): Unit = {
    //
    // In order to validate the tier lag calculation, a segment upload is exercised via
    // RLMTask#copyLogSegmentsToRemote(). The following logs are present in the local storage before upload:
    //
    // - SegmentToUpload
    // - SegmentNotToUpload
    // - ActiveSegment
    //
    // with the following configuration:
    //
    //                    SegmentToUpload        SegmentNotToUpload      ActiveSegment
    // --------------  -------------------------------------------------------------------
    //    ...    110|  | 111     ...      159 | 160 161   ...    169 | 170    180  ...
    // --------------  -------------------------------------------------------------------
    //                   ELO                        LSO
    // ..............  ....................................................................
    //  Tiered Log                                 Local log
    //
    // SegmentNotToUpload contains the LSO and is not uploaded. ActiveSegment is not uploaded. Only SegmentToUpload
    // is uploaded. The tier lag at the end of the execution of the RLM task is expected to be 169 - 159 = 10.
    //
    // Note: this test is unreadable. The RLM needs to be refactored to make it testable.
    //
    val epochCheckpoints = Seq(0 -> 0, 1 -> 20, 3 -> 50, 4 -> 100)
    epochCheckpoints.foreach { case (epoch, startOffset) => cache.assign(epoch, startOffset) }

    val activeSegment: LogSegment = createMock(classOf[LogSegment])
    expect(activeSegment.baseOffset).andReturn(170).anyTimes()

    val fileRecords: FileRecords = createMock(classOf[FileRecords])
    val file: File = createMock(classOf[File])

    val lazyOffsetIndex: LazyIndex[OffsetIndex] = createMock(classOf[LazyIndex[OffsetIndex]])
    val lazyTimeIndex: LazyIndex[TimeIndex] = createMock(classOf[LazyIndex[TimeIndex]])
    val offsetIndex: OffsetIndex = createMock(classOf[OffsetIndex])
    val timeIndex: TimeIndex = createMock(classOf[TimeIndex])
    val txIndex: TransactionIndex = createMock(classOf[TransactionIndex])

    expect(lazyOffsetIndex.get).andReturn(offsetIndex)
    expect(lazyTimeIndex.get).andReturn(timeIndex)
    expect(offsetIndex.path).andReturn(Paths.get("home", "pinkf"))
    expect(timeIndex.path).andReturn(Paths.get("home", "nellyf"))
    expect(txIndex.path).andReturn(Paths.get("home", "amyl"))

    val segmentToUpload: LogSegment = createMock(classOf[LogSegment])
    expect(segmentToUpload.baseOffset).andReturn(111).anyTimes()
    expect(segmentToUpload.log).andReturn(fileRecords).anyTimes()
    expect(segmentToUpload.readNextOffset).andReturn(160)
    expect(segmentToUpload.largestTimestamp).andReturn(8L)
    expect(segmentToUpload.lazyOffsetIndex).andReturn(lazyOffsetIndex)
    expect(segmentToUpload.lazyTimeIndex).andReturn(lazyTimeIndex)
    expect(segmentToUpload.txnIndex).andReturn(txIndex)

    val segmentNotToUpload: LogSegment = createMock(classOf[LogSegment])
    expect(segmentNotToUpload.baseOffset).andReturn(160).anyTimes()
    expect(segmentNotToUpload.log).andReturn(fileRecords).anyTimes()
    expect(segmentNotToUpload.readNextOffset).andReturn(170)
    expect(segmentNotToUpload.largestTimestamp).andReturn(8L)
    expect(segmentNotToUpload.lazyOffsetIndex).andReturn(lazyOffsetIndex)
    expect(segmentNotToUpload.lazyTimeIndex).andReturn(lazyTimeIndex)
    expect(segmentNotToUpload.txnIndex).andReturn(txIndex)

    expect(fileRecords.file).andReturn(file).anyTimes()
    expect(fileRecords.sizeInBytes()).andReturn(1024)
    expect(file.getName).andReturn("00000000000111.log").anyTimes()
    expect(file.toPath).andReturn(Paths.get("home", "philc")).anyTimes()

    val logConfig: LogConfig = createMock(classOf[LogConfig])

    val producerStateManager: ProducerStateManager = createMock(classOf[ProducerStateManager])
    expect(producerStateManager.fetchSnapshot(anyLong())).andReturn(Some(file))

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache)).anyTimes()
    expect(log.config).andReturn(logConfig).anyTimes()
    expect(log.producerStateManager).andReturn(producerStateManager).anyTimes()

    expect(log.lastStableOffset).andReturn(161)
    expect(log.activeSegment).andReturn(activeSegment).anyTimes()
    expect(log.logStartOffset).andReturn(0)
    expect(log.logSegments(EasyMock.eq(111L), EasyMock.eq(161L))).andReturn(Seq(segmentToUpload, segmentNotToUpload))
    expect(log.updateRemoteIndexHighestOffset(EasyMock.eq(159L)))

    var logStartOffset: Option[Long] = None
    val rsmManager: ClassLoaderAwareRemoteStorageManager = createMock(classOf[ClassLoaderAwareRemoteStorageManager])

    val rlmmManager: RemoteLogMetadataManager = createMock(classOf[RemoteLogMetadataManager])
    expect(rlmmManager.addRemoteLogSegmentMetadata(anyObject(classOf[RemoteLogSegmentMetadata]))).anyTimes()
    expect(rlmmManager.updateRemoteLogSegmentMetadata(anyObject(classOf[RemoteLogSegmentMetadataUpdate]))).anyTimes()

    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), EasyMock.eq(0)))
      .andReturn(Optional.of(19L)).anyTimes()
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), EasyMock.eq(1)))
      .andReturn(Optional.of(49L)).anyTimes()
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), EasyMock.eq(3)))
      .andReturn(Optional.of(99L)).anyTimes()
    expect(rlmmManager.highestOffsetForEpoch(EasyMock.eq(topicIdPartition), EasyMock.eq(4)))
      .andReturn(Optional.of(110L)).anyTimes()

    replay(log, rlmmManager, activeSegment, segmentToUpload, segmentNotToUpload, fileRecords, file,
      producerStateManager, lazyOffsetIndex, lazyTimeIndex, offsetIndex, timeIndex, txIndex)

    val remoteLogManager =
      new RemoteLogManager(_ => Option(log), (_, startOffset) => logStartOffset = Option(startOffset), rlmConfig, time,
        brokerId, clusterId, logsDir, brokerTopicStats) {
        override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager = rsmManager
        override private[remote] def createRemoteLogMetadataManager() = rlmmManager
      }

    val task = new remoteLogManager.RLMTask(topicIdPartition)
    task.convertToLeader(4)

    task.copyLogSegmentsToRemote()

    val lag = brokerTopicStats.tierLagStats(topicIdPartition.topicPartition().topic()).lag()
    assertEquals(10L, lag)
  }

  @Test
  def testReadThrowsWhenWeCannotFindEpochAssociatedWithOffset(): Unit = {
    val partitionData = new PartitionData(0, 0, 0, Optional.empty())

    val fetchInfo: RemoteStorageFetchInfo = createMock(classOf[RemoteStorageFetchInfo])
    expect(fetchInfo.fetchMaxBytes).andReturn(0).anyTimes()
    expect(fetchInfo.topicPartition).andReturn(topicPartition)
    expect(fetchInfo.fetchInfo).andReturn(partitionData)
    expect(fetchInfo.fetchIsolation).andReturn(FetchTxnCommitted)

    replay(fetchInfo)

    val remoteLogManager = new RemoteLogManager(_ => Option.empty, (_, _) => {}, rlmConfig, time, brokerId = 1,
      clusterId, logsDir, brokerTopicStats)

    assertThrows(classOf[OffsetOutOfRangeException], () => remoteLogManager.read(fetchInfo))
  }

  @Test
  def testReadThrowsWhenWeCanFindEpochAssociatedWithOffsetRemoteMetadata(): Unit = {
    val cache: LeaderEpochFileCache = createMock(classOf[LeaderEpochFileCache])
    expect(cache.epochForOffset(anyLong())).andReturn(Option.empty)

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache))

    val partitionData = new PartitionData(0, 0, 0, Optional.empty())

    val fetchInfo: RemoteStorageFetchInfo = createMock(classOf[RemoteStorageFetchInfo])
    expect(fetchInfo.fetchMaxBytes).andReturn(0)
    expect(fetchInfo.topicPartition).andReturn(topicPartition)
    expect(fetchInfo.fetchInfo).andReturn(partitionData)
    expect(fetchInfo.fetchIsolation).andReturn(FetchTxnCommitted)

    replay(fetchInfo, log, cache)

    val remoteLogManager = new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, brokerId = 1,
      clusterId, logsDir, brokerTopicStats)

    assertThrows(classOf[OffsetOutOfRangeException], () => remoteLogManager.read(fetchInfo))
  }

  @Test
  def testReadPasses(): Unit = {
    val rsmManager: ClassLoaderAwareRemoteStorageManager = createMock(classOf[ClassLoaderAwareRemoteStorageManager])
    expect(rsmManager.fetchLogSegment(anyObject(), anyInt())).andReturn(createMock(classOf[FileInputStream]))

    val rlsMetadata: RemoteLogSegmentMetadata = createMock(classOf[RemoteLogSegmentMetadata])

    val cache: LeaderEpochFileCache = createMock(classOf[LeaderEpochFileCache])
    expect(cache.epochForOffset(anyLong())).andReturn(Option(1))

    val log: Log = createMock(classOf[Log])
    expect(log.leaderEpochCache).andReturn(Option(cache))

    val partitionData = new PartitionData(0, 0, 0, Optional.empty())

    val fetchInfo: RemoteStorageFetchInfo = createMock(classOf[RemoteStorageFetchInfo])
    expect(fetchInfo.fetchMaxBytes).andReturn(0)
    expect(fetchInfo.topicPartition).andReturn(topicPartition)
    expect(fetchInfo.fetchInfo).andReturn(partitionData)
    expect(fetchInfo.fetchIsolation).andReturn(FetchTxnCommitted)

    replay(fetchInfo, log, cache, rsmManager)

    val remoteLogManager = new RemoteLogManager(_ => Option(log), (_, _) => {}, rlmConfig, time, brokerId = 1,
      clusterId, logsDir, brokerTopicStats) {
      override private[remote] def createRemoteStorageManager(): ClassLoaderAwareRemoteStorageManager =
        rsmManager

      override def fetchRemoteLogSegmentMetadata(tp: TopicPartition,
                                                 epochForOffset: Int,
                                                 offset: Long): Optional[RemoteLogSegmentMetadata] =
        Optional.of(rlsMetadata)

      override def lookupPositionForOffset(remoteLogSegmentMetadata: RemoteLogSegmentMetadata,
                                           offset: Long): Int = 1

      override private[remote] def findFirstBatch(remoteLogInputStream: RemoteLogInputStream, offset: Long): RecordBatch = null
    }

    val fetchDataInfo = remoteLogManager.read(fetchInfo)
    assertEquals(0, fetchDataInfo.fetchOffsetMetadata.messageOffset)
    assertEquals(MemoryRecords.EMPTY, fetchDataInfo.records)
    assertEquals(Some(List.empty), fetchDataInfo.abortedTransactions)
  }

  private def listRemoteLogSegmentMetadataByTime(segmentCount: Int,
                                                 deletableSegmentCount: Int,
                                                 recordsPerSegment: Int): List[RemoteLogSegmentMetadata] = {
    val result = mutable.Buffer.empty[RemoteLogSegmentMetadata]
    for (idx <- 0 until segmentCount) {
      val timestamp = if (idx < deletableSegmentCount) time.milliseconds()-1 else time.milliseconds()
      val startOffset = idx * recordsPerSegment
      val endOffset = startOffset + recordsPerSegment - 1
      val segmentLeaderEpochs = truncateAndGetLeaderEpochs(cache, startOffset, endOffset)
      result += new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
        startOffset, endOffset, timestamp, brokerId, timestamp, 100, segmentLeaderEpochs)
    }
    result.toList
  }

  private def listRemoteLogSegmentMetadata(segmentCount: Int, recordsPerSegment: Int): List[RemoteLogSegmentMetadata] = {
    listRemoteLogSegmentMetadataByTime(segmentCount, 0, recordsPerSegment)
  }

  private def nonExistentTempFile(): File = {
    val file = TestUtils.tempFile()
    file.delete()
    file
  }

  private def createRLMConfig(props: Properties = new Properties): RemoteLogManagerConfig = {
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true.toString)
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
    val config = new AbstractConfig(RemoteLogManagerConfig.CONFIG_DEF, props)
    new RemoteLogManagerConfig(config)
  }

  private def checkpoint(): LeaderEpochCheckpoint = {
    new LeaderEpochCheckpoint {
      private var epochs: Seq[EpochEntry] = Seq()
      override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
      override def read(): Seq[EpochEntry] = this.epochs
    }
  }

  private def truncateAndGetLeaderEpochs(cache: LeaderEpochFileCache,
                                         startOffset: Long,
                                         endOffset: Long): util.Map[Integer, lang.Long] = {
    val myCheckpoint = checkpoint()
    val myCache = cache.writeTo(myCheckpoint)
    myCache.truncateFromStart(startOffset)
    myCache.truncateFromEnd(endOffset)
    myCheckpoint.read().map(e => Integer.valueOf(e.epoch) -> lang.Long.valueOf(e.startOffset)).toMap.asJava
  }
}
