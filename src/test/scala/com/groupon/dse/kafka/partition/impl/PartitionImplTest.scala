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

import com.groupon.dse.configs.KafkaServerConfig
import com.groupon.dse.kafka.common.ConsumerClientBuilder
import com.groupon.dse.kafka.common.KafkaException.{LeaderNotAvailableException, OffsetOutOfRangeException}
import com.groupon.dse.kafka.partition.Leader
import com.groupon.dse.testutils.{EmbeddedKafka, EmbeddedSpark, TestDefaults}
import com.groupon.dse.zookeeper.ZkClientBuilder
import kafka.consumer.SimpleConsumer
import kafka.producer.{Producer, ProducerConfig}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}


class PartitionImplTest extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll {

  val kafkaTopic = TestDefaults.TestTopic
  val clientId = TestDefaults.TestClientId
  val clientName = TestDefaults.TestClientName
  val zkConnTimeout = 10000
  val zkSessionTimeout = 10000
  var kafkaServerConfigs: KafkaServerConfig = _
  var producer: Producer[String, Array[Byte]] = _
  var partition: PartitionImpl = _
  var embeddedKafka: EmbeddedKafka = _
  var embeddedSpark: EmbeddedSpark = _
  var partitionKey: String = _
  var zkClient: ZkClient = _
  var zkConnect: String = _
  var leader: Leader = _
  var consumer: SimpleConsumer = _

  override def beforeAll() = {
    embeddedSpark = new EmbeddedSpark(getClass.getName, 4, 500)
    UserMetricsSystem.initialize(embeddedSpark.getStreamingContext.sparkContext)
  }

  override def afterAll() = {
    embeddedSpark.stop
  }

  before {
    embeddedKafka = new EmbeddedKafka
    embeddedKafka.startCluster()
    producer = new Producer[String, Array[Byte]](new ProducerConfig(embeddedKafka.kafkaProducerProperties))
    kafkaServerConfigs = TestDefaults.testKafkaServerConfig(embeddedKafka.zkServer.connectString)
    partition = new PartitionImpl(kafkaTopic, 0)
    partitionKey = TestDefaults.TestPathPrefix + "/" + partition.partitionIdentifier
    leader = Leader("localhost", embeddedKafka.kafkaPort)
    consumer = ConsumerClientBuilder.newInstance(leader, kafkaServerConfigs, clientName)
    zkConnect = embeddedKafka.zkServer.connectString
    zkClient = ZkClientBuilder(zkConnect, zkConnTimeout, zkSessionTimeout)
  }

  after {
    embeddedKafka.stopCluster()
    zkClient.close()
  }

  "Max offset " should "equal n for test_message sent n times" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    assert(partition.offsets(consumer).max == 4)
  }

  "Min offset" should "equal 0 for test_message sent n times" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    assert(partition.offsets(consumer).min == 0)
  }

  "The size of the offset range" should "be 2 for test_message sent n times [min,max]" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    val offsets = partition.offsetRange(2, consumer)
    assert(offsets.size == 2)
  }

  "The offset range" should "equal [0,n] for test_message sent n times" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    val offsets = partition.offsetRange(2, consumer)
    offsets.foreach(println)
    val list = List(0, 4)
    assert(list.sorted == offsets.sorted)
  }

  it should "throw an exception if leader is incorrect" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    intercept[Exception] {
      val consumer = ConsumerClientBuilder.newInstance(Leader("localhost1", embeddedKafka.kafkaPort),
        kafkaServerConfigs,
        clientName)
      assert(partition.offsetRange(3, consumer).max == 4)
    }
  }

  "The number of messages fetched " should "be n if [startoffset = maxoffset - n]" in {
    embeddedKafka.sendMessage(7, producer, kafkaTopic)
    var messages = partition.fetchMessages(2,
      TestDefaults.TestTopicFetchSize,
      partitionKey,
      consumer)
    assert(messages.size == 5)
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    messages = partition.fetchMessages(5,
      TestDefaults.TestTopicFetchSize,
      partitionKey,
      consumer)
    assert(messages.size == 6)

  }

  it should "throw an OffsetOutOfRangeException exception if startoffset > maxoffset while fetching" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    intercept[OffsetOutOfRangeException] {
      partition.fetchMessages(7,
        TestDefaults.TestTopicFetchSize,
        partitionKey,
        consumer)
    }
  }

  "The partition identifier" should "be topic/partitionNum " in {
    assert(partition.partitionIdentifier == s"$kafkaTopic/0")
  }

  it should "return localhost:PORT if leader is found for topic:partition" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    assert(partition.leader(zkClient).host == "localhost")
    assert(partition.leader(zkClient).port == embeddedKafka.kafkaPort)
  }

  it should "throw LeaderNotAvailableException if leader is not found for partition" in {
    val newPartition = new PartitionImpl("random_topic", 0)
    intercept[LeaderNotAvailableException] {
      newPartition.leader(zkClient)
    }
  }
}
