/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.cluster.BrokerEndPoint
import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.util.MockBlockingSender
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.errors.{FencedLeaderEpochException, UnknownLeaderEpochException}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.Mockito.mock

import java.util
import scala.collection.Seq

class RemoteLeaderEndPointTest {

    val topicPartition = new TopicPartition("test", 5)
    val currentLeaderEpoch = 10
    val logStartOffset = 20
    val localLogStartOffset = 100
    val logEndOffset = 300
    var cache: LeaderEpochFileCache = _
    var blockingSend: MockBlockingSender = _
    var endPoint: LeaderEndPoint = _

    @BeforeEach
    def setUp(): Unit = {
        val time = new MockTime
        val logPrefix = "remote-leader-endpoint"
        val sourceBroker: BrokerEndPoint = BrokerEndPoint(0, "localhost", 9092)
        val props = TestUtils.createBrokerConfig(sourceBroker.id, TestUtils.MockZkConnect, port = sourceBroker.port)
        val fetchSessionHandler = new FetchSessionHandler(new LogContext(logPrefix), sourceBroker.id)
        val config = KafkaConfig.fromProps(props)
        val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
        blockingSend = new MockBlockingSender(offsets = new util.HashMap[TopicPartition, EpochEndOffset](),
            sourceBroker = sourceBroker, time = time)
        endPoint = new RemoteLeaderEndPoint(logPrefix, blockingSend, fetchSessionHandler,
            config, replicaManager, QuotaFactory.UnboundedQuota, () => MetadataVersion.MINIMUM_KRAFT_VERSION)

        val checkpoint: LeaderEpochCheckpoint = new LeaderEpochCheckpoint {
            private var epochs: Seq[EpochEntry] = Seq()
            override def write(epochs: Iterable[EpochEntry]): Unit = this.epochs = epochs.toSeq
            override def read(): Seq[EpochEntry] = this.epochs
        }
        cache = new LeaderEpochFileCache(topicPartition, checkpoint)
        cache.assign(epoch = 5, startOffset = 20)
        cache.assign(epoch = 6, startOffset = 50)
        cache.assign(epoch = 7, startOffset = 250)

        blockingSend.add(topicPartition, cache, currentLeaderEpoch)
        blockingSend.addOffsets(topicPartition, logStartOffset, localLogStartOffset, logEndOffset)
    }

    @Test
    def fetchLatestOffset(): Unit = {
        assertEquals((7, logEndOffset), endPoint.fetchLatestOffset(topicPartition, currentLeaderEpoch))
        assertThrows(classOf[FencedLeaderEpochException], () => endPoint.fetchLatestOffset(topicPartition, currentLeaderEpoch - 1))
        assertThrows(classOf[UnknownLeaderEpochException], () => endPoint.fetchLatestOffset(topicPartition, currentLeaderEpoch + 1))
    }

    @Test
    def fetchEarliestOffset(): Unit = {
        assertEquals((5, logStartOffset), endPoint.fetchEarliestOffset(topicPartition, currentLeaderEpoch))
        assertThrows(classOf[FencedLeaderEpochException], () => endPoint.fetchEarliestOffset(topicPartition, currentLeaderEpoch - 1))
        assertThrows(classOf[UnknownLeaderEpochException], () => endPoint.fetchEarliestOffset(topicPartition, currentLeaderEpoch + 1))
    }

    @Test
    def fetchEarliestLocalOffset(): Unit = {
        assertEquals((6, localLogStartOffset), endPoint.fetchEarliestLocalOffset(topicPartition, currentLeaderEpoch))
        assertThrows(classOf[FencedLeaderEpochException], () => endPoint.fetchEarliestLocalOffset(topicPartition, currentLeaderEpoch - 1))
        assertThrows(classOf[UnknownLeaderEpochException], () => endPoint.fetchEarliestLocalOffset(topicPartition, currentLeaderEpoch + 1))
    }
}