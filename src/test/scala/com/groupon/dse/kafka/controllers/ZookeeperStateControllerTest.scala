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

package com.groupon.dse.kafka.controllers

import com.groupon.dse.configs.KafkaServerConfig
import com.groupon.dse.kafka.common.{ConsumerClientBuilder, State, WrappedMessage}
import com.groupon.dse.kafka.partition.Leader
import com.groupon.dse.kafka.partition.impl.PartitionImpl
import com.groupon.dse.testutils._
import kafka.consumer.SimpleConsumer
import kafka.producer.{Producer, ProducerConfig}
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.streaming.StreamingContext
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}

class ZookeeperStateControllerTest extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll {

  val kafkaTopic = TestDefaults.TestTopic
  val clientName = TestDefaults.TestClientName
  val clazz = getClass.getName
  val zkConnTimeout = 5000
  val zkSessionTimeout = 500
  var kafkaServerConfigs: KafkaServerConfig = _
  var producer: Producer[String, Array[Byte]] = _
  var partition: PartitionImpl = _
  var partitionKey: String = _
  var embeddedKafka: EmbeddedKafka = _
  var embeddedSpark: EmbeddedSpark = _
  var ssc: StreamingContext = _
  var stateController: ZookeeperStateController = _
  var zookeeper: EmbeddedZookeeper = _
  var leader: Leader = _
  var consumer: SimpleConsumer = _

  override def beforeAll() = {
    embeddedSpark = new EmbeddedSpark(clazz, 4, 500)
    ssc = embeddedSpark.getStreamingContext
    UserMetricsSystem.initialize(ssc.sparkContext)
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

    zookeeper = new EmbeddedZookeeper(TestUtils.choosePorts(1)(0))

    stateController = new ZookeeperStateController(
      zookeeper.connectString,
      s"/spark_consumer/test_consumer",
      zkConnTimeout,
      zkSessionTimeout)

    leader = Leader("localhost", embeddedKafka.kafkaPort)
    consumer = ConsumerClientBuilder.newInstance(leader, kafkaServerConfigs, clientName)
  }

  after {
    stateController.close
    embeddedKafka.stopCluster()
    zookeeper.shutdown()
  }

  it should "throw KeyDoesNotExistException if state is not set yet for the key" in {
    intercept[KeyDoesNotExistException] {
      stateController.getState("/invalid_path")
    }
  }

  "ZookeepStateController with zkClient un-initialized" should "handle temporary ZK unavailability " in {
    /*
    Sequence of events in this test:
     - Controller created
     - Zookeeper cluster initialized but not started (autoStart set to false)
     - Controller initiates interaction with Zookeeper(tries to create ZkClient) but since its not started it keeps re-trying
     - Zookeeper cluster starts in a new thread. Zookeeper does start before the ZkConnectionTimeout for the client expires
     - Controller finally connects to Zookeeper (ZkClient created successfully) and completes interaction successfully
     */
    val testPort = TestUtils.choosePorts(1)(0)
    val testZkEndpoint = s"127.0.0.1:${testPort}"
    val testZkController = new ZookeeperStateController(testZkEndpoint, s"/spark_consumer/test_consumer", zkConnTimeout, zkSessionTimeout)
    val zk = new EmbeddedZookeeper(testPort, false)
    new Thread {
      override def run() {
        Thread.sleep(1000)
        zk.start()
      }
    }.start()

    testZkController.setState("/valid_path", State(1234, System.currentTimeMillis()))
    zk.cleanShutdown()
  }

  "ZookeepStateController with zkClient un-initialized" should "throw exception when ZK does not recover in time " in {
    intercept[StateControllerConnectionException] {
      /*
      Sequence of events in this test:
       - Controller created
       - Zookeeper cluster initialized but not started (autoStart set to false)
       - Controller initiates interaction with Zookeeper(tries to create ZkClient) but since its not started it keeps re-trying
       - Zookeeper cluster starts in a new thread. Zookeeper does NOT start before the ZkConnectionTimeout for the client expires
       - Controller fails to connect to Zookeeper (or rather ZkClient could not be initialized) and the interaction fails
       */
      val testPort = TestUtils.choosePorts(1)(0)
      val testZkEndpoint = s"127.0.0.1:${testPort}"
      val testZkController = new ZookeeperStateController(testZkEndpoint, s"/spark_consumer/test_consumer", 500, zkSessionTimeout)
      val zk = new EmbeddedZookeeper(testPort, false)
      new Thread {
        override def run() {
          Thread.sleep(2000)
          zk.start()
        }
      }.start()

      testZkController.setState("/valid_path", State(1234, System.currentTimeMillis()))
      zk.cleanShutdown()
    }
  }

  "ZookeepStateController with zkClient initialized" should "handle temporary ZK unavailability before interaction initiated" in {
    /*
    Sequence of events in this test:
     - Controller created
     - Zookeeper cluster starts
     - Controller interacts successfully with Zookeeper (in this step the ZkClient gets initialized)
     - Zookeeper cluster is stopped and later started in a new thread
     - Controller interacts with the Zookeeper again (using the already created ZkClient), but because of the sleep
        the interaction occurs after the Zookeeper cluster has re-started
     */
    val testPort = TestUtils.choosePorts(1)(0)
    val testSnapshotDir = TestUtils.tempDir
    val testLogDir = TestUtils.tempDir

    val testZkEndpoint = s"127.0.0.1:${testPort}"
    val testZkController = new ZookeeperStateController(testZkEndpoint, s"/spark_consumer/test_consumer", zkConnTimeout, zkSessionTimeout)
    val zk = new EmbeddedZookeeper(testPort, testSnapshotDir, testLogDir, false)
    zk.start()

    testZkController.setState("/valid_path", State(1234, System.currentTimeMillis()))
    zk.stop()

    new Thread {
      override def run = {
        zk.start()
      }
    }.start()

    Thread.sleep(2000)
    assert(testZkController.isKeyPresent("/valid_path") == true)
    zk.cleanShutdown()
  }

  "ZookeepStateController with zkClient initialized" should "handle temporary ZK unavailability after interaction initiated" in {
    /*
    Sequence of events in this test:
     - Controller created
     - Zookeeper cluster starts
     - Controller interacts successfully with Zookeeper (in this step the ZkClient gets initialized)
     - Zookeeper cluster is stopped and later started in a new thread
     - Controller interacts with the Zookeeper again (using the already created ZkClient), however this interaction is intiated
        when the cluster is still down.
     */
    val testPort = TestUtils.choosePorts(1)(0)
    val testSnapshotDir = TestUtils.tempDir
    val testLogDir = TestUtils.tempDir

    val testZkEndpoint = s"127.0.0.1:${testPort}"
    val testZkController = new ZookeeperStateController(testZkEndpoint, s"/spark_consumer/test_consumer", zkConnTimeout, zkSessionTimeout)
    val zk = new EmbeddedZookeeper(testPort, testSnapshotDir, testLogDir, false)
    zk.start()

    testZkController.setState("/valid_path", State(1234, System.currentTimeMillis()))
    zk.stop()

    new Thread {
      override def run = {
        Thread.sleep(3000)
        zk.start()
      }
    }.start()

    assert(testZkController.isKeyPresent("/valid_path") == true)
    zk.cleanShutdown()
  }

  it should "return the correct state offset if key exists " in {
    val state = State(1234, System.currentTimeMillis())
    val output = stateController.setState("/valid_path", state)
    assert(output ==("/valid_path", state))
    assert(stateController.getState("/valid_path").offset == 1234)
  }

  "The state key" should "equal pathPrefix/partitionIdentifier for a give partition" in {
    assert(stateController.generateStateKey(new PartitionImpl("Test_topic", 0)) == "/spark_consumer/test_consumer/Test_topic/0")
  }

  it should "return false if key does not exist in the ZK" in {
    assert(!stateController.isKeyPresent("/invalid_key"))
  }

  it should "return true if key does exist in the ZK" in {
    stateController.setState("/valid_path", State(1234, System.currentTimeMillis()))
    assert(stateController.isKeyPresent("/valid_path"))
  }

  "StateController" should "compute the max offset from the RDD list" in {
    embeddedKafka.sendMessage(1, producer, kafkaTopic)
    val messages = partition.fetchMessages(0,
      TestDefaults.TestTopicFetchSize,
      partitionKey,
      consumer)

    val msgRdd = ssc.sparkContext.parallelize[WrappedMessage](messages.toList)
    val state = stateController.setState(msgRdd)(0)._2
    assert(state.offset == 1)
  }
}
