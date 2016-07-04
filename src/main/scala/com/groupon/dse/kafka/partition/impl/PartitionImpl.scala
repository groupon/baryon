/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.groupon.dse.kafka.partition.impl

import com.groupon.dse.kafka.common.KafkaException.LeaderNotAvailableException
import com.groupon.dse.kafka.common._
import com.groupon.dse.kafka.partition.{Leader, Offsets, Partition}
import kafka.api.{FetchRequestBuilder, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkException
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.slf4j.LoggerFactory

/**
  * Representation of a single Kafka version 0.8.* partition
  *
  * @param topic Name of the topic in the partition
  * @param partitionNum Partition identifier
  */
class PartitionImpl(topic: String, partitionNum: Int) extends Partition(topic, partitionNum) {

  private lazy val lagGauge = UserMetricsSystem.gauge(s"baryon.lag.$topic.$partitionNum")
  private lazy val inputBytesMeter = UserMetricsSystem.meter(s"baryon.inputBytesRate.$topic.$partitionNum")
  private lazy val inputRecordsMeter = UserMetricsSystem.meter(s"baryon.inputRecordsRate.$topic.$partitionNum")
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Obtain the max and min offset for a [[Partition]]
    *
    * @param consumer Kafka consumer client
    * @return Instance of [[Offsets]]
    */
  @throws[KafkaException]
  override def offsets(consumer: SimpleConsumer): Offsets = {
    val topicAndPartition = TopicAndPartition(topic, partitionNum)

    val min = consumer.earliestOrLatestOffset(topicAndPartition,
      OffsetRequest.EarliestTime,
      this.partitionNum)

    val max = consumer.earliestOrLatestOffset(topicAndPartition,
      OffsetRequest.LatestTime,
      this.partitionNum)

    val offsets = Offsets(min, max)

    logger.debug(s"Offsets fetched for partition: $partitionIdentifier. Offsets: $offsets")

    offsets
  }

  /**
    * Provide the N latest valid offset values
    *
    * @param count Maximum valid offset values to be fetched
    * @param consumer Kafka consumer client
    * @return Sequence of the N latest valid offsets
    */
  @throws[KafkaException]
  override def offsetRange(count: Int, consumer: SimpleConsumer): Seq[Long] = {
    val topicAndPartition = TopicAndPartition(topic, partitionNum)

    val offsetRequest = OffsetRequest(Map[TopicAndPartition, PartitionOffsetRequestInfo](
      topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, count)))

    val response = consumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets(topicAndPartition)

    val offsets = response.error match {
      case ErrorMapping.NoError => response.offsets
      case _ => throw KafkaException(response.error)
    }

    logger.debug(s"Offsets ranges fetched for partition: $partitionIdentifier. Offset ranges: $offsets")

    offsets
  }

  /**
    * Fetch messages for the particular [[Partition]]
    *
    * @param startOffset Offset to start fetching from
    * @param fetchSize Data to fetch in a single api call
    * @param stateKey Partition key associated with the message set
    * @param consumer Kafka consumer client
    * @return Iterable list of [[WrappedMessage]]
    */
  @throws[KafkaException]
  override def fetchMessages(startOffset: Long,
                             fetchSize: Int,
                             stateKey: String,
                             consumer: SimpleConsumer)
  : Iterable[WrappedMessage] = {
    val fetchResponse = consumer.fetch(
      new FetchRequestBuilder()
        .clientId(this.partitionIdentifier)
        .addFetch(topic, partitionNum, startOffset, fetchSize)
        .build())

    fetchResponse.errorCode(topic, partitionNum) match {
      case ErrorMapping.NoError => {
        val messageSet = fetchResponse.messageSet(topic, partitionNum)

        val messageBuffer = scala.collection.mutable.ArrayBuffer.empty[WrappedMessage]

        // Offset after the last message of the fetch that this message belongs to
        val batchEndOffset = startOffset + messageSet.size

        // Kafka may return messages with offsets before the requested offset with compressed messages
        messageSet.dropWhile(_.offset < startOffset).foreach(m => {
          val payload = m.message.payload
          val ar = new Array[Byte](payload.limit())
          payload.get(ar)
          messageBuffer += WrappedMessage(topic, partitionNum, stateKey, ar, m.offset, m.nextOffset, batchEndOffset)
        })
        lagGauge.set(fetchResponse.highWatermark(topic, partitionNum) - batchEndOffset)
        inputBytesMeter.mark(messageSet.validBytes)
        inputRecordsMeter.mark(messageSet.size)

        messageBuffer.toIterable
      }
      case _ => throw KafkaException(fetchResponse.errorCode(topic, partitionNum))
    }
  }

  /**
    * Obtain the [[Leader]] for the current partition
    *
    * @param zkClient Client to interact with the Kafka Zookeeper
    * @return [[Leader]] for the current partition
    */
  @throws[LeaderNotAvailableException]
  @throws[ZkException]
  override def leader(zkClient: ZkClient): Leader = {
    val cluster = ZkUtils.getCluster(zkClient)
    val leaderId = ZkUtils.getLeaderForPartition(zkClient, topic, partitionNum).getOrElse(-1)
    leaderId match {
      case -1 => throw new LeaderNotAvailableException(s"Could not find leaderId for $topic:$partitionNum")
      case _ => {
        val leader = cluster.getBroker(leaderId)
        leader.isDefined match {
          case true => {
            val curLeader = Leader(leader.get.host, leader.get.port)
            logger.debug(s"Leader fetched for partition: $partitionIdentifier. Leader: $curLeader")
            curLeader
          }
          case _ => throw new LeaderNotAvailableException(s"Broker corresponding to leaderId=$leaderId " +
            s"does not exist or is unavailable")
        }
      }
    }
  }

  /**
    * Unique [[Partition]] identifier
    *
    * @return [[Partition]] identifier
    */
  override def partitionIdentifier: String = s"$topic/$partitionNum"
}
