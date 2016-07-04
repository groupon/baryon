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

package com.groupon.dse.kafka.common

import java.util.Properties

import com.groupon.dse.configs.{ReceiverConfigBuilder, ReceiverConfigs}
import com.groupon.dse.kafka.cluster.impl.ClusterImpl
import com.groupon.dse.kafka.common.KafkaException.{InvalidMessageException, InvalidMessageSizeException, LeaderNotAvailableException, OffsetOutOfRangeException}
import com.groupon.dse.kafka.controllers.StateController
import com.groupon.dse.kafka.partition.{Leader, Partition}
import com.groupon.dse.spark.dstreams.KafkaLowLevelReceiver
import com.groupon.dse.testutils._
import com.groupon.dse.zookeeper.ZkClientBuilder
import kafka.producer.{Producer, ProducerConfig}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.scalatest.{BeforeAndAfter, FlatSpec}

class KafkaExceptionHandlerTest extends FlatSpec with BeforeAndAfter {

  val clazz = getClass.getName
  val localState = State(1, 123232323)
  val topic = "topic1"
  val receiverId = 0
  val totalReceivers = 1
  val clientName = "TestClient"
  val zkConnTimeout = 10000
  val zkSessionTimeout = 10000
  var embeddedSpark: EmbeddedSpark = _
  var receiver: KafkaLowLevelReceiver = _
  var embeddedKafka: EmbeddedKafka = _
  var stateController: StateController = _
  var embeddedZookeeper: EmbeddedZookeeper = _
  var ssc: StreamingContext = _
  var partition: Partition = _
  var leader: Leader = _
  var receiverConfigs: ReceiverConfigs = _
  var zkClient: ZkClient = _
  var consumerCache: ConsumerClientCache = _

  before {
    embeddedKafka = new EmbeddedKafka
    embeddedKafka.startCluster()
    val producer = new Producer[String, Array[Byte]](new ProducerConfig(embeddedKafka.kafkaProducerProperties))
    val zkConnect = s"127.0.0.1:${embeddedKafka.zkPort}"
    val kafkaServerConfigs = TestDefaults.testKafkaServerConfig(zkConnect)
    val cluster = new ClusterImpl(kafkaServerConfigs)
    zkClient = ZkClientBuilder(zkConnect, zkConnTimeout, zkSessionTimeout)

    embeddedZookeeper = new EmbeddedZookeeper(TestUtils.choosePorts(1)(0))

    embeddedSpark = new EmbeddedSpark(clazz, 4, 500)
    ssc = embeddedSpark.getStreamingContext
    val properties = new Properties()
    properties.setProperty("topics", topic)
    properties.setProperty("kafka.broker.zk.connect", embeddedKafka.zkServer.connectString)
    properties.setProperty("topic.start.offset", "-2")
    properties.setProperty("topic.consumption.policy", "OFFSET")
    properties.setProperty("statecontroller.type", "ZOOKEEPER")
    properties.setProperty("statecontroller.zk.connect", embeddedZookeeper.connectString)
    properties.setProperty("statecontroller.zk.root", TestDefaults.TestPathPrefix)

    receiverConfigs = ReceiverConfigBuilder(properties)
    stateController = receiverConfigs.stateController

    consumerCache = new ConsumerClientCache(receiverConfigs.kafkaServerConfigs, clazz)

    receiver = new KafkaLowLevelReceiver(
      ssc.sparkContext,
      receiverConfigs,
      receiverId,
      totalReceivers,
      StorageLevel.MEMORY_AND_DISK_SER_2
    )


    val topics = Array(topic)
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))
    partition = cluster.partitions(topics.toList, zkClient)(0)
    leader = Leader("localhost", embeddedKafka.kafkaPort)
  }

  after {
    zkClient.close()
    embeddedSpark.stop
    embeddedKafka.stopCluster()
    embeddedZookeeper.cleanShutdown()
    stateController.close
  }

  "Handle exception " should "catch InvalidMessageSizeException" in {
    val outcome = KafkaExceptionHandler.handleException(
      InvalidMessageSizeException("something is wrong"),
      partition,
      localState,
      consumerCache,
      receiver,
      clientName)

    assert(outcome == Outcome.FetchFailure)

    val partitionKey = stateController.generateStateKey(partition)
    assert(stateController.getState(partitionKey).offset == 2)
  }

  "Handle exception " should "catch InvalidMessageException" in {
    val outcome = KafkaExceptionHandler.handleException(
      InvalidMessageException("something is wrong"),
      partition,
      localState,
      consumerCache,
      receiver,
      clientName)

    assert(outcome == Outcome.FetchFailure)

    val partitionKey = stateController.generateStateKey(partition)
    assert(stateController.getState(partitionKey).offset == 2)
  }

  "Handle exception " should "catch OffsetOutOfRangeException" in {
    val outcome = KafkaExceptionHandler.handleException(
      OffsetOutOfRangeException("something is wrong"),
      partition,
      localState,
      consumerCache,
      receiver,
      clientName)

    assert(outcome == Outcome.FetchFailure)

    val partitionKey = stateController.generateStateKey(partition)
    assert(stateController.getState(partitionKey).offset == 1)
  }

  "Handle exception " should "catch LeaderNotAvailableException" in {
    val outcome = KafkaExceptionHandler.handleException(
      LeaderNotAvailableException("something is wrong"),
      partition,
      localState,
      consumerCache,
      receiver,
      clientName)

    assert(outcome == Outcome.FetchFailure)
    assert(consumerCache.get(partition).isEmpty)
  }
}
