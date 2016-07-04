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

package com.groupon.dse.kafka.partition

import java.util.Properties

import com.groupon.dse.configs.{KafkaServerConfig, ReceiverConfigBuilder, ReceiverConfigs}
import com.groupon.dse.kafka.cluster.impl.ClusterImpl
import com.groupon.dse.kafka.common.State
import com.groupon.dse.kafka.controllers.StateController
import com.groupon.dse.kafka.partition.impl.PartitionImpl
import com.groupon.dse.kafka.policy.OffsetBasedFetchPolicy
import com.groupon.dse.testutils.{EmbeddedKafka, TestDefaults}
import com.groupon.dse.zookeeper.ZkClientBuilder
import kafka.producer.{Producer, ProducerConfig}
import org.I0Itec.zkclient.ZkClient
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.collection._

class PartitionUtilsTest extends FlatSpec with BeforeAndAfter {

  val clientId = TestDefaults.TestClientId
  val clientName = TestDefaults.TestClientName
  val topics = TestDefaults.TestTopic
  val zkConnTimeout = 10000
  val zkSessionTimeout = 10000
  var kafkaServerConfigs: KafkaServerConfig = _
  var producer: Producer[String, Array[Byte]] = _
  var embeddedKafka: EmbeddedKafka = _
  var cluster: ClusterImpl = _
  var stateController: StateController = _
  var zkClient: ZkClient = _
  var zkConnect: String = _
  var receiverConfig: ReceiverConfigs = _

  before {
    embeddedKafka = new EmbeddedKafka
    embeddedKafka.startCluster()
    producer = new Producer[String, Array[Byte]](new ProducerConfig(embeddedKafka.kafkaProducerProperties))
    zkConnect = embeddedKafka.zkServer.connectString
    cluster = new ClusterImpl(kafkaServerConfigs)

    val properties = new Properties()
    properties.setProperty("topics", TestDefaults.TestTopic)
    properties.setProperty("kafka.broker.zk.connect", embeddedKafka.zkServer.connectString)
    properties.setProperty("statecontroller.type", "ZOOKEEPER")
    properties.setProperty("statecontroller.zk.connect", embeddedKafka.zkServer.connectString)
    properties.setProperty("statecontroller.zk.root", TestDefaults.TestPathPrefix)
    properties.setProperty("topic.start.offset", "-2")

    receiverConfig = ReceiverConfigBuilder(properties)
    stateController = receiverConfig.stateController
    kafkaServerConfigs = receiverConfig.kafkaServerConfigs

    zkClient = ZkClientBuilder(zkConnect, zkConnTimeout, zkSessionTimeout)
  }

  after {
    zkClient.close()
    embeddedKafka.stopCluster()
  }

  "Filter partitions " should " return 2 even indexed partitions" in {
    val topics = Array("topic1", "topic2", "topic3", "topic4")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val allPartitions = cluster.partitions(topics.toSeq, zkClient)
    val id = 0
    val total = 2
    val filteredPartitions = PartitionUtils.partitionsForReceiver(allPartitions, id, total)
    val expected = List(allPartitions(0).topic, allPartitions(2).topic)
    val filteredTopics = filteredPartitions.map(p => p.topic).toList
    assert(filteredTopics.sorted == expected.sorted)
  }

  "Filter partitions " should " return 2 odd indexed partitions" in {
    val topics = Array("topic1", "topic2", "topic3", "topic4")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val allPartitions = cluster.partitions(topics.toSeq, zkClient)
    val id = 1
    val total = 2
    val filteredPartitions = PartitionUtils.partitionsForReceiver(allPartitions, id, total)
    val expected = List(allPartitions(1).topic, allPartitions(3).topic)
    val obtained = filteredPartitions.map(p => p.topic).toList
    assert(obtained.sorted == expected.sorted)
  }

  "Filter partitions " should " return empty" in {
    val topics = Array("topic1")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val allPartitions = cluster.partitions(topics.toSeq, zkClient)
    val id = 1
    val total = 2
    val filteredPartitions = PartitionUtils.partitionsForReceiver(allPartitions, id, total)
    val obtained = filteredPartitions.map(p => p.topic).toList
    assert(obtained.isEmpty)
  }

  it should "throw IllegalArgumentException if id greater than receiver count" in {
    intercept[IllegalArgumentException] {
      val topics = Array("topic1")
      topics.foreach(embeddedKafka.sendMessage(1, producer, _))

      val allPartitions = cluster.partitions(topics.toSeq, zkClient)
      val id = 2
      val total = 2
      PartitionUtils.partitionsForReceiver(allPartitions, id, total)
    }
  }

  "Fetch leader " should " return map with 1 partition->leader entry" in {
    val topics = Array("topic1")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val partitions = cluster.partitions(topics.toSeq, zkClient)
    val expected = Map(partitions(0) -> Leader("localhost", embeddedKafka.kafkaPort))

    val obtained = PartitionUtils.partitionLeaderMap(cluster, partitions, kafkaServerConfigs)
    assert(expected == obtained)
  }

  "Fetch leader " should " return map 2 partition->leader entries" in {
    val topics = Array("topic1", "topic2")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val partitions = cluster.partitions(topics.toSeq, zkClient)
    val expected = Map(
      partitions(0) -> Leader("localhost", embeddedKafka.kafkaPort),
      partitions(1) -> Leader("localhost", embeddedKafka.kafkaPort))

    val obtained = PartitionUtils.partitionLeaderMap(cluster, partitions, kafkaServerConfigs)
    assert(expected == obtained)
  }

  "PartitionLeaderMap" should "not contain an entry for a partition with no leader" in {
    val topics = Array("topic1")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val partition = new PartitionImpl("topic1", 2)
    assert(!PartitionUtils.partitionLeaderMap(cluster, Seq(partition), kafkaServerConfigs).contains(partition))

  }

  "Generate keys " should " return map with N keys" in {
    val topics = Array("topic1")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val partition = cluster.partitions(topics.toSeq, zkClient)(0)

    val key = TestDefaults.TestPathPrefix + "/" + partition.partitionIdentifier
    val expected = Map(partition -> key)

    val obtained = PartitionUtils.partitionStateKeys(Seq(partition), stateController)
    assert(expected == obtained)
  }

  "Init state " should " return 1 for initial state and 1 for zk state" in {
    val topics = Array("topic1")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val partition = cluster.partitions(topics.toSeq, zkClient)(0)
    val leader = Leader("localhost", embeddedKafka.kafkaPort)

    val properties = new Properties()
    properties.setProperty("topics", TestDefaults.TestTopic)
    properties.setProperty("kafka.broker.zk.connect", embeddedKafka.zkServer.connectString)
    properties.setProperty("statecontroller.type", "ZOOKEEPER")
    properties.setProperty("statecontroller.zk.connect", embeddedKafka.zkServer.connectString)
    properties.setProperty("statecontroller.zk.root", TestDefaults.TestPathPrefix)
    properties.setProperty("topic.start.offset", "-1")

    receiverConfig = ReceiverConfigBuilder(properties)
    stateController = receiverConfig.stateController
    kafkaServerConfigs = receiverConfig.kafkaServerConfigs

    val expected = 1
    val obtained = PartitionUtils.initializePartitionState(partition, leader, receiverConfig, clientName).offset
    val expectedZkState = 1

    assert(expected == obtained)
    assert(expectedZkState == stateController.getState(stateController.generateStateKey(partition)).offset)
  }

  "Init state " should " return 0 for initial state and 0 for zk state" in {
    val topics = Array("topic1")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val partition = cluster.partitions(topics.toSeq, zkClient)(0)
    val leader = Leader("localhost", embeddedKafka.kafkaPort)

    val expected = 0
    val obtained = PartitionUtils.initializePartitionState(partition, leader, receiverConfig, clientName).offset
    val expectedZkState = 0

    assert(expected == obtained)
    assert(expectedZkState == stateController.getState(stateController.generateStateKey(partition)).offset)
  }

  "Init state " should " return zk state" in {
    val topics = Array("topic1")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val partition = cluster.partitions(topics.toSeq, zkClient)(0)
    val leader = Leader("localhost", embeddedKafka.kafkaPort)
    val policy = new OffsetBasedFetchPolicy(1, 10235, -2)

    val key = stateController.generateStateKey(partition)
    val value: Long = 12
    val mockState = State(value, 1212121212)
    stateController.setState(key, mockState)

    val obtained = PartitionUtils.initializePartitionState(partition, leader, receiverConfig, clientName).offset

    assert(value == obtained)
  }

  "Init state cache " should " return partition->state mapping" in {
    val topics = Array("topic1", "topic2")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val partitions = cluster.partitions(topics.toSeq, zkClient)
    val plMap = mutable.Map.empty[Partition, Leader]
    partitions.foreach(p => {
      plMap += (p -> Leader("localhost", embeddedKafka.kafkaPort))
    })

    val expected = mutable.ArrayBuffer.empty[Long]
    partitions.foreach(p => {
      expected += 0
    })

    val obtained = PartitionUtils.initializePartitionStates(plMap.toMap, receiverConfig, clientName)
      .toList
      .map(v => v._2.offset)
      .toArray

    assert(expected.toArray.deep == obtained.deep)
  }

  "Partition List for all receivers" should "return all the partitions for the topics provided" in {
    val topics = Array("topic1", "topic2")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val properties = new Properties()
    properties.setProperty("topics", topics.mkString(","))
    properties.setProperty("kafka.broker.zk.connect", embeddedKafka.zkServer.connectString)
    properties.setProperty("statecontroller.type", "ZOOKEEPER")
    properties.setProperty("statecontroller.zk.connect", embeddedKafka.zkServer.connectString)
    properties.setProperty("statecontroller.zk.root", TestDefaults.TestPathPrefix)
    val rConfig = ReceiverConfigBuilder(properties)

    val expected = cluster.partitions(topics.toList, zkClient)
    val obtained = PartitionUtils.partitionsForAllReceivers(rConfig)
    assert(expected.toList == obtained)
  }

  "Partition leader map " should " filter the partitions and return the partition->leader mapping " in {
    val topics = Array("topic1", "topic2")
    topics.foreach(embeddedKafka.sendMessage(1, producer, _))

    val properties = new Properties()
    properties.setProperty("topics", topics.mkString(","))
    properties.setProperty("kafka.broker.zk.connect", embeddedKafka.zkServer.connectString)
    properties.setProperty("statecontroller.type", "ZOOKEEPER")
    properties.setProperty("statecontroller.zk.connect", embeddedKafka.zkServer.connectString)
    properties.setProperty("statecontroller.zk.root", TestDefaults.TestPathPrefix)
    val rConfig = ReceiverConfigBuilder(properties)

    val id = 0
    val totalReceivers = 2

    val allPartitions = PartitionUtils.partitionsForAllReceivers(rConfig)
    val myPartition = cluster.partitions(topics.toList, zkClient)(0)
    val expected = Map(myPartition -> Leader("localhost", embeddedKafka.kafkaPort))
    val obtained = PartitionUtils.partitionLeaderMapForReceiver(rConfig, id, totalReceivers, allPartitions)
    assert(expected == obtained)
  }

  "Filter " should "return everything when all topics names are valid" in {
    val allTopics = Seq("test_json", "test1", "test_random", "other_topic")
    val topics = Seq("test1", "test_random")
    val returned = PartitionUtils.filterTopics(allTopics, topics)
    val expected = Seq("test1", "test_random")
    assert(returned.sameElements(expected))
  }

  "Filter " should "only return the valid topic names" in {
    val allTopics = Seq("test_json", "test1", "test_random", "other_topic")
    val topics = Seq("test1", "invalid")
    val returned = PartitionUtils.filterTopics(allTopics, topics)
    val expected = Seq("test1")
    assert(returned.sameElements(expected))
  }

  "Filter" should "return an empty list if the master topic list is empty" in {
    val allTopics = Seq("")
    val topics = Seq("test1", "invalid")
    val returned = PartitionUtils.filterTopics(allTopics, topics)
    val expected = Seq.empty[String]
    assert(returned.sameElements(expected))
  }
}
