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

package com.groupon.dse.spark.dstreams

import java.util.Properties

import com.groupon.dse.configs.{KafkaServerConfig, ReceiverConfigBuilder, ReceiverConfigs}
import com.groupon.dse.kafka.common._
import com.groupon.dse.kafka.partition.impl.PartitionImpl
import com.groupon.dse.kafka.partition.{Leader, Partition}
import com.groupon.dse.testutils.{EmbeddedKafka, EmbeddedSpark, TestDefaults}
import kafka.consumer.SimpleConsumer
import kafka.producer.{Producer, ProducerConfig}
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec}

import scala.collection.mutable.{HashMap => Cache, ListBuffer}

class KafkaLowLevelConsumerTest extends FlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {

  val receiverId: Int = 0
  val totalReceivers: Int = 1
  var embeddedKafka: EmbeddedKafka = _
  var embeddedSpark: EmbeddedSpark = _
  var producer: Producer[String, Array[Byte]] = _
  var kafkaTopic = TestDefaults.TestTopic
  var kafkaLowLevelConsumer: KafkaLowLevelConsumer = _
  var receiverConfigs: ReceiverConfigs = _
  var topic: String = _
  var partition: Partition = _
  var partitionKey: String = _
  var leader: Leader = _
  var currentState: State = _
  var kafkaServerConfig: KafkaServerConfig = _
  var consumer: SimpleConsumer = _

  override def beforeAll {
    embeddedKafka = new EmbeddedKafka
    embeddedKafka.startCluster()
    producer = new Producer[String, Array[Byte]](new ProducerConfig(embeddedKafka.kafkaProducerProperties))
    // Use a mock Receiver since majority of the KafkaLowLevelConsumer methods we test do not use Receiver
    kafkaLowLevelConsumer = new KafkaLowLevelConsumer(mock(classOf[KafkaLowLevelReceiver]))
    embeddedSpark = new EmbeddedSpark(getClass.getName, 4, 500)
    UserMetricsSystem.initialize(embeddedSpark.getStreamingContext.sparkContext)
  }

  override def afterAll: Unit = {
    embeddedKafka.stopCluster()
    embeddedSpark.stop
  }

  override def beforeEach(): Unit = {
    receiverConfigs = ReceiverConfigBuilder(testEnvProperties(kafkaTopic, embeddedKafka.zkServer.connectString))
    topic = System.currentTimeMillis().toString
    partition = new PartitionImpl(topic, 0)
    partitionKey = TestDefaults.TestPathPrefix + "/" + partition.partitionIdentifier
    leader = Leader("localhost", embeddedKafka.kafkaPort)
    currentState = State(0, System.currentTimeMillis())
    kafkaServerConfig = TestDefaults.testKafkaServerConfig(embeddedKafka.zkServer.connectString)
    consumer = ConsumerClientBuilder.newInstance(leader, kafkaServerConfig, s"${topic}_client")
  }

  "completePartitionFetchCycle" should "sleep for the appropriate times" in {
    var start = System.currentTimeMillis()
    kafkaLowLevelConsumer.completePartitionFetchCycle(receiverConfigs, true)
    assert(System.currentTimeMillis() - start >= receiverConfigs.receiverRestIntervalOnSuccess)

    start = System.currentTimeMillis()
    kafkaLowLevelConsumer.completePartitionFetchCycle(receiverConfigs, false)
    assert(System.currentTimeMillis() - start >= receiverConfigs.receiverRestIntervalOnFail)
  }

  "retrieveMessages " should "return a single block with all messages by default" in {
    val numMessages = 10
    val message = Array.fill(10)(0.toByte)

    embeddedKafka.sendMessage(numMessages, producer, topic, message)
    val msgIter = kafkaLowLevelConsumer.retrieveMessages(partition,
      partitionKey,
      currentState.offset,
      consumer,
      receiverConfigs)

    assert(msgIter.size == 1)
    assert(msgIter.head.size == numMessages)
    msgIter.head.foreach(wm => assert(wm.payload sameElements message))
  }

  "retrieveMessages " should "return an empty iterable when topic:partition exist but nothing to consume" in {
    embeddedKafka.sendMessage(1, producer, topic)
    val msgIter = kafkaLowLevelConsumer.retrieveMessages(partition,
      partitionKey,
      1,
      consumer,
      receiverConfigs)
    assert(msgIter.isEmpty)
  }

  "retrieveMessages " should "throw exception when topic:partition does not exist" in
    intercept[KafkaException] {
      kafkaLowLevelConsumer.retrieveMessages(partition,
        partitionKey,
        currentState.offset,
        consumer,
        receiverConfigs)
    }

  "retrieveMessages " should "return multiple blocks of messages" in {
    val props = testEnvProperties(kafkaTopic, embeddedKafka.zkServer.connectString)
    props.setProperty("topic.repartition.factor", "4")
    receiverConfigs = ReceiverConfigBuilder(props)
    val numMessages = 20
    val message = Array.fill(10)(0.toByte)

    embeddedKafka.sendMessage(numMessages, producer, topic, message)
    val msgIter = kafkaLowLevelConsumer.retrieveMessages(partition,
      partitionKey,
      currentState.offset,
      consumer,
      receiverConfigs)

    assert(msgIter.size == receiverConfigs.topicRepartitionFactor)
    msgIter.foreach(iter => {
      assert(iter.size == (numMessages / receiverConfigs.topicRepartitionFactor))
      iter.foreach(wm => assert(wm.payload sameElements message))
    })
  }

  "retrieveMessages " should "return a message per block if the number of messages is too small to repartition" in {
    val props = testEnvProperties(kafkaTopic, embeddedKafka.zkServer.connectString)
    props.setProperty("topic.repartition.factor", "5")
    receiverConfigs = ReceiverConfigBuilder(props)
    val numMessages = 4
    val message = Array.fill(10)(0.toByte)

    embeddedKafka.sendMessage(numMessages, producer, topic, message)
    val msgIter = kafkaLowLevelConsumer.retrieveMessages(partition,
      partitionKey,
      currentState.offset,
      consumer,
      receiverConfigs)

    assert(msgIter.size == numMessages)
    msgIter.foreach(iter => {
      assert(iter.size == 1)
      iter.foreach(wm => assert(wm.payload sameElements message))
    })
  }

  "refreshLocalCache" should "populate the local caches" in {
    val stateCache = Cache.empty[Partition, State]
    val stateKeyCache = Cache.empty[Partition, String]
    embeddedKafka.sendMessage(1, producer, topic)

    val partitionLeaderMap = Map(partition -> leader)
    kafkaLowLevelConsumer.refreshLocalCaches(stateCache,
      stateKeyCache,
      partitionLeaderMap,
      receiverConfigs)

    assert(stateCache.size == 1)
    assert(stateKeyCache.size == 1)
    assert(stateCache.contains(partition))
    assert(stateKeyCache.contains(partition))
    assert(stateCache(partition).offset == currentState.offset)
    assert(stateKeyCache(partition) == partitionKey)
  }

  "refresh" should "return a partition->leader mapping when single topic:partition present" in {
    val stateCache = Cache.empty[Partition, State]
    val stateKeyCache = Cache.empty[Partition, String]
    val partitionListCache = ListBuffer.empty[Partition]
    val curReceiverConfig = ReceiverConfigBuilder(testEnvProperties(topic, embeddedKafka.zkServer.connectString))
    embeddedKafka.sendMessage(1, producer, topic)

    val partitionLeaderMap = kafkaLowLevelConsumer.refresh(stateCache,
      stateKeyCache,
      curReceiverConfig,
      receiverId,
      totalReceivers,
      partitionListCache)

    assert(stateCache.size == 1)
    assert(stateKeyCache.size == 1)
    assert(stateCache.contains(partition))
    assert(stateKeyCache.contains(partition))
    assert(stateCache(partition).offset == currentState.offset)
    assert(stateKeyCache(partition) == partitionKey)

    assert(partitionLeaderMap.size == 1)
    assert(partitionLeaderMap.get(partition) == leader)
  }

  "refresh" should "return a partition->leader mapping when two topic:partition present" in {
    val stateCache = Cache.empty[Partition, State]
    val stateKeyCache = Cache.empty[Partition, String]
    val partitionListCache = ListBuffer.empty[Partition]
    val secondTopic = s"${topic}_2"

    val curReceiverConfig = ReceiverConfigBuilder(
      testEnvProperties(
        s"${topic},${secondTopic}",
        embeddedKafka.zkServer.connectString))

    // Send data for first topic thereby creating a partition
    embeddedKafka.sendMessage(1, producer, topic)

    var partitionLeaderMap = kafkaLowLevelConsumer.refresh(stateCache,
      stateKeyCache,
      curReceiverConfig,
      receiverId,
      totalReceivers,
      partitionListCache)

    assert(partitionLeaderMap.get.size == 1)
    assert(partitionLeaderMap.get(partition) == leader)

    // Send data for second topic
    val secondPartition = new PartitionImpl(secondTopic, 0)
    val secondLeader = Leader("localhost", embeddedKafka.kafkaPort)
    embeddedKafka.sendMessage(1, producer, secondTopic)

    partitionLeaderMap = kafkaLowLevelConsumer.refresh(stateCache,
      stateKeyCache,
      curReceiverConfig,
      receiverId,
      totalReceivers,
      partitionListCache)

    assert(stateCache.size == 2)
    assert(stateKeyCache.size == 2)
    assert(stateCache.contains(partition))
    assert(stateKeyCache.contains(partition))
    assert(stateCache.contains(secondPartition))
    assert(stateKeyCache.contains(secondPartition))
    assert(partitionLeaderMap.get.size == 2)
    assert(partitionLeaderMap.get(secondPartition) == secondLeader)
    assert(partitionLeaderMap.get(partition) == leader)
  }

  "refresh" should "handle Zookeeper connection exception" in {
    val stateCache = Cache.empty[Partition, State]
    val stateKeyCache = Cache.empty[Partition, String]
    val partitionListCache = ListBuffer.empty[Partition]
    val curKafka = new EmbeddedKafka
    curKafka.startCluster()

    val curReceiverConfig = ReceiverConfigBuilder(testEnvProperties(topic, curKafka.zkServer.connectString))
    curKafka.sendMessage(1, producer, topic)

    curKafka.zkServer.cleanShutdown()

    val partitionLeaderMap = kafkaLowLevelConsumer.refresh(stateCache,
      stateKeyCache,
      curReceiverConfig,
      receiverId,
      totalReceivers,
      partitionListCache)

    assert(partitionLeaderMap.isEmpty)

    curKafka.stopCluster()
  }

  "blockingRefresh" should "return a partition->leader mapping when single topic:partition present" in {
    val stateCache = Cache.empty[Partition, State]
    val stateKeyCache = Cache.empty[Partition, String]
    val partitionListCache = ListBuffer.empty[Partition]
    val curReceiverConfig = ReceiverConfigBuilder(testEnvProperties(topic, embeddedKafka.zkServer.connectString))
    embeddedKafka.sendMessage(1, producer, topic)

    val partitionLeaderMap = kafkaLowLevelConsumer.blockingRefresh(stateCache,
      stateKeyCache,
      curReceiverConfig,
      receiverId,
      totalReceivers,
      partitionListCache)

    assert(stateCache.size == 1)
    assert(stateKeyCache.size == 1)
    assert(stateCache.contains(partition))
    assert(stateKeyCache.contains(partition))
    assert(stateCache(partition).offset == currentState.offset)
    assert(stateKeyCache(partition) == partitionKey)

    assert(partitionLeaderMap.size == 1)
    assert(partitionLeaderMap(partition) == leader)
  }

  "processPartition" should "return FetchSuccess" in {
    val stateCache = Cache(partition -> State(0, System.currentTimeMillis()))
    val stateKeyCache = Cache(partition -> partitionKey)
    receiverConfigs.stateController.setState(partitionKey, State(0, System.currentTimeMillis()))

    embeddedKafka.sendMessage(10, producer, topic)

    val (curReceiver, curConsumer) = receiverAndConsumer

    val outcome = curConsumer.processPartition(stateCache,
      stateKeyCache,
      partition,
      Some(consumer),
      curReceiver)

    assert(outcome == Outcome.FetchSuccess)
  }

  "processPartition" should "return NothingToFetch when policy is not met" in {
    val stateCache = Cache(partition -> State(1, System.currentTimeMillis()))
    val stateKeyCache = Cache(partition -> partitionKey)
    receiverConfigs.stateController.setState(partitionKey, State(0, System.currentTimeMillis()))

    embeddedKafka.sendMessage(10, producer, topic)

    val (curReceiver, curConsumer) = receiverAndConsumer

    val outcome = curConsumer.processPartition(stateCache,
      stateKeyCache,
      partition,
      Some(consumer),
      curReceiver)

    assert(outcome == Outcome.NothingToFetch)
  }

  "processPartition" should "return NothingToFetch when no messages to consume" in {
    val stateCache = Cache(partition -> State(10, System.currentTimeMillis()))
    val stateKeyCache = Cache(partition -> partitionKey)
    receiverConfigs.stateController.setState(partitionKey, State(10, System.currentTimeMillis()))

    embeddedKafka.sendMessage(10, producer, topic)

    val (curReceiver, curConsumer) = receiverAndConsumer

    val outcome = curConsumer.processPartition(stateCache,
      stateKeyCache,
      partition,
      Some(consumer),
      curReceiver)

    assert(outcome == Outcome.NothingToFetch)
  }

  "processPartition" should "return NoOutcome when no consumer" in {
    val stateCache = Cache(partition -> State(0, System.currentTimeMillis()))
    val stateKeyCache = Cache(partition -> partitionKey)
    receiverConfigs.stateController.setState(partitionKey, State(0, System.currentTimeMillis()))

    embeddedKafka.sendMessage(10, producer, topic)

    val (curReceiver, curConsumer) = receiverAndConsumer

    val outcome = curConsumer.processPartition(stateCache,
      stateKeyCache,
      partition,
      None,
      curReceiver)

    assert(outcome == Outcome.NoOutcome)
  }

  private def testEnvProperties(kafkaTopic: String, brokerZkEndpoint: String): Properties = {
    val properties = new Properties()
    properties.setProperty("topics", kafkaTopic)
    properties.setProperty("kafka.broker.zk.connect", brokerZkEndpoint)
    properties.setProperty("topic.start.offset", "-2")
    properties.setProperty("topic.consumption.policy", "OFFSET")
    properties.setProperty("topic.consumption.blocking", "true")

    // We have to use Zookeeper based StateController since the Memory based one
    // does not allow you to consume from the beginning
    properties.setProperty("statecontroller.type", "ZOOKEEPER")
    properties.setProperty("statecontroller.zk.connect", brokerZkEndpoint)
    properties.setProperty("statecontroller.zk.root", TestDefaults.TestPathPrefix)

    // Keeping a low fetch size so that multiple fetches happen
    properties.setProperty("topic.fetch.size.bytes", "750")

    properties
  }

  private def receiverAndConsumer: (KafkaLowLevelReceiver, KafkaLowLevelConsumer) = {
    val curReceiver = mock(classOf[KafkaLowLevelReceiver])
    doNothing().when(curReceiver).store(_: Iterator[WrappedMessage])
    when(curReceiver.receiverConfigs).thenReturn(receiverConfigs)

    (curReceiver, new KafkaLowLevelConsumer(curReceiver))
  }
}