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
package kafka.server.epoch.util

import kafka.cluster.BrokerEndPoint
import kafka.server.BlockingSend
import kafka.server.epoch.{EpochEntry, LeaderEpochFileCache}
import org.apache.kafka.clients.{ClientRequest, ClientResponse, MockClient, NetworkClientUtils}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult}
import org.apache.kafka.common.message.{FetchResponseData, ListOffsetsResponseData, OffsetForLeaderEpochResponseData}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.AbstractRequest.Builder
import org.apache.kafka.common.requests.{AbstractRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse, OffsetsForLeaderEpochResponse, FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.utils.{SystemTime, Time}
import org.apache.kafka.common.{Node, TopicIdPartition, TopicPartition, Uuid}

import java.net.SocketTimeoutException
import java.util
import scala.collection.Map
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
  * Stub network client used for testing the ReplicaFetcher, wraps the MockClient used for consumer testing
  *
  * The common case is that there is only one OFFSET_FOR_LEADER_EPOCH request/response. So, the
  * response to OFFSET_FOR_LEADER_EPOCH is 'offsets' map. If the test needs to set another round of
  * OFFSET_FOR_LEADER_EPOCH with different offsets in response, it should update offsets using
  * setOffsetsForNextResponse
  */
class MockBlockingSender(offsets: java.util.Map[TopicPartition, EpochEndOffset],
                         sourceBroker: BrokerEndPoint,
                         time: Time)
  extends BlockingSend {

  private val client = new MockClient(new SystemTime)
  var fetchCount = 0
  var epochFetchCount = 0
  var listOffsetsCount = 0
  var lastUsedOffsetForLeaderEpochVersion = -1
  var callback: Option[() => Unit] = None
  var currentOffsets: util.Map[TopicPartition, EpochEndOffset] = offsets
  var fetchPartitionData: Map[TopicPartition, FetchResponseData.PartitionData] = Map.empty
  var topicIds: Map[String, Uuid] = Map.empty
  private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port)

  private val leaderEpochFiles: mutable.Map[TopicPartition, LeaderEpochFileCache] = mutable.Map()
  private val currentLeaderEpochs: mutable.Map[TopicPartition, Int] = mutable.Map()
  private val offsetHolderByPartition: mutable.Map[TopicPartition, OffsetHolder] = mutable.Map()

  def setEpochRequestCallback(postEpochFunction: () => Unit): Unit = {
    callback = Some(postEpochFunction)
  }

  def setOffsetsForNextResponse(newOffsets: util.Map[TopicPartition, EpochEndOffset]): Unit = {
    currentOffsets = newOffsets
  }

  def setFetchPartitionDataForNextResponse(partitionData: Map[TopicPartition, FetchResponseData.PartitionData]): Unit = {
    fetchPartitionData = partitionData
  }

  def setIdsForNextResponse(topicIds: Map[String, Uuid]): Unit = {
    this.topicIds = topicIds
  }

  /**
   * Add the given leader epoch cache for the topic-partition.
   * Set the value of the current leader epoch for this partition on the leader.
   */
  def add(topicPartition: TopicPartition,
          leaderEpochFileCache: LeaderEpochFileCache,
          currentLeaderEpoch: Int): MockBlockingSender = {
    leaderEpochFiles.put(topicPartition, leaderEpochFileCache)
    currentLeaderEpochs.put(topicPartition, currentLeaderEpoch)
    this
  }

  def addOffsets(topicPartition: TopicPartition,
                 logStartOffset: Int,
                 localLogStartOffset: Int,
                 logEndOffset: Int): MockBlockingSender = {
    offsetHolderByPartition.put(topicPartition, OffsetHolder(logStartOffset, localLogStartOffset, logEndOffset))
    this
  }

  override def brokerEndPoint(): BrokerEndPoint = sourceBroker

  override def sendRequest(requestBuilder: Builder[_ <: AbstractRequest]): ClientResponse = {
    if (!NetworkClientUtils.awaitReady(client, sourceNode, time, 500))
      throw new SocketTimeoutException(s"Failed to connect within 500 ms")

    //Send the request to the mock client
    val clientRequest = request(requestBuilder)
    client.send(clientRequest, time.milliseconds())

    //Create a suitable response based on the API key
    val response = requestBuilder.apiKey() match {
      case ApiKeys.OFFSET_FOR_LEADER_EPOCH =>
        callback.foreach(_.apply())
        epochFetchCount += 1
        lastUsedOffsetForLeaderEpochVersion = requestBuilder.latestAllowedVersion()

        val data = new OffsetForLeaderEpochResponseData()
        currentOffsets.forEach((tp, offsetForLeaderPartition) => {
          var topic = data.topics.find(tp.topic)
          if (topic == null) {
            topic = new OffsetForLeaderTopicResult()
              .setTopic(tp.topic)
            data.topics.add(topic)
          }
          topic.partitions.add(offsetForLeaderPartition)
        })

        new OffsetsForLeaderEpochResponse(data)

      case ApiKeys.FETCH =>
        fetchCount += 1
        val partitionData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
        fetchPartitionData.foreach { case (tp, data) => partitionData.put(new TopicIdPartition(topicIds.getOrElse(tp.topic(), Uuid.ZERO_UUID), tp), data) }
        fetchPartitionData = Map.empty
        topicIds = Map.empty
        FetchResponse.of(Errors.NONE, 0,
          if (partitionData.isEmpty) JFetchMetadata.INVALID_SESSION_ID else 1,
          partitionData)

      case ApiKeys.LIST_OFFSETS =>
        listOffsetsCount += 1
        val listOffsetsPartitionResponsePerTopic: mutable.Map[String, mutable.Buffer[ListOffsetsPartitionResponse]] = mutable.Map()
        val listOffsetsRequest = clientRequest.requestBuilder().build().asInstanceOf[ListOffsetsRequest]
        listOffsetsRequest.data().topics().asScala
          .flatMap(topic => {
            listOffsetsPartitionResponsePerTopic.put(topic.name(), mutable.Buffer[ListOffsetsPartitionResponse]())
            Seq(topic.name()).zip(topic.partitions().asScala)
          })
          .foreach { case (topic, partition) =>
            val topicPartition = new TopicPartition(topic, partition.partitionIndex())

            val listOffsetsPartitionResponse = {
              val currentLeaderEpoch = currentLeaderEpochs(topicPartition)
              val leaderEpochCache = leaderEpochFiles(topicPartition)

              if (currentLeaderEpoch > partition.currentLeaderEpoch()) {
                new ListOffsetsPartitionResponse()
                  .setPartitionIndex(partition.partitionIndex())
                  .setErrorCode(Errors.FENCED_LEADER_EPOCH.code)
              } else if (currentLeaderEpoch < partition.currentLeaderEpoch()) {
                new ListOffsetsPartitionResponse()
                  .setPartitionIndex(partition.partitionIndex())
                  .setErrorCode(Errors.UNKNOWN_LEADER_EPOCH.code)
              } else {
                val foundEntry = {
                  val offsetHolder = offsetHolderByPartition(topicPartition)

                  if (partition.timestamp() == ListOffsetsRequest.EARLIEST_TIMESTAMP)
                    EpochEntry(leaderEpochCache.earliestEntry.get.epoch, offsetHolder.logStartOffset)
                  else if (partition.timestamp() == ListOffsetsRequest.LATEST_TIMESTAMP)
                    EpochEntry(leaderEpochCache.latestEpoch.get, offsetHolder.logEndOffset)
                  else if (partition.timestamp() == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)
                    EpochEntry(leaderEpochCache.epochForOffset(offsetHolder.localLogStartOffset).get, offsetHolder.localLogStartOffset)
                  else
                    EpochEntry(-1, -1)
                }
                val result = new ListOffsetsPartitionResponse()
                  .setPartitionIndex(partition.partitionIndex())
                  .setErrorCode(Errors.NONE.code())
                  .setLeaderEpoch(foundEntry.epoch)
                  .setTimestamp(RecordBatch.NO_TIMESTAMP)
                  .setOffset(foundEntry.startOffset)
                result
              }
            }
            listOffsetsPartitionResponsePerTopic(topic).addOne(listOffsetsPartitionResponse)
          }

        val result = listOffsetsPartitionResponsePerTopic.map((_: (String, mutable.Buffer[ListOffsetsPartitionResponse])) match {
          case (topic, listOffsetsPartitionResponse) =>
            new ListOffsetsTopicResponse()
            .setName(topic)
            .setPartitions(listOffsetsPartitionResponse.toList.asJava)
        })
        val listOffsetsResponseData = new ListOffsetsResponseData()
          .setTopics(result.toList.asJava)
        new ListOffsetsResponse(listOffsetsResponseData)

      case _ =>
        throw new UnsupportedOperationException
    }

    //Use mock client to create the appropriate response object
    client.respondFrom(response, sourceNode)
    client.poll(30, time.milliseconds()).iterator().next()
  }

  private def request(requestBuilder: Builder[_ <: AbstractRequest]): ClientRequest = {
    client.newClientRequest(
      sourceBroker.id.toString,
      requestBuilder,
      time.milliseconds(),
      true)
  }

  override def initiateClose(): Unit = {}

  override def close(): Unit = {}

  private case class OffsetHolder(logStartOffset: Int, localLogStartOffset: Int, logEndOffset: Int)
}
