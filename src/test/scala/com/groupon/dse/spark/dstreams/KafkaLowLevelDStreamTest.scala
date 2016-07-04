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

import com.groupon.dse.configs.ReceiverConfigBuilder
import com.groupon.dse.testutils._
import kafka.producer.{Producer, ProducerConfig}
import org.apache.spark.storage.StorageLevel
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}

/**
  * The test starts a kafka cluster, sends messages and uses the kafka low level receiver to consume from a single partition.
  */
class KafkaLowLevelDStreamTest extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll {

  // This single test case performs a complete end to end testing for the DStream Receiver
  "The number of fetched and sent messages/bytes" should "be the same" in {
    // Prepare the environment
    val kafkaTopic = TestDefaults.TestTopic
    val totalReceivers = 1
    val receiverId = 0
    val embeddedKafka = new EmbeddedKafka
    embeddedKafka.startCluster()
    val producer = new Producer[String, Array[Byte]](new ProducerConfig(embeddedKafka.kafkaProducerProperties))
    val embeddedSpark = new EmbeddedSpark(getClass.getName, 4, 500)
    val ssc = embeddedSpark.getStreamingContext
    val receiverConfigs = ReceiverConfigBuilder(testEnvProperties(kafkaTopic, embeddedKafka.zkServer.connectString))
    val stateController = receiverConfigs.stateController

    val dStream = new KafkaLowLevelDStream(ssc,
      receiverConfigs,
      receiverId,
      totalReceivers,
      StorageLevel.MEMORY_AND_DISK_SER_2
    )

    // Processing layer
    var result = 0
    var sum = 0

    val numSend = 100
    embeddedKafka.sendMessage(numSend, producer, kafkaTopic)

    dStream.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        //Gets all the messages in the RDD and calculates the total number of messages and the total number of bytes
        val ret = rdd.collect()
        result += ret.length
        ret.foreach { v =>
          sum += v.payload.length
        }
        stateController.setState(rdd)
      }
    })
    dStream.print()

    ssc.start()
    // TODO: 15 sec is just an arbitrary sleep interval based on trial and error.
    // We should find a better way to determine this
    Thread.sleep(15000)
    ssc.stop()

    //Check if we get the same number of messages we send
    assert(result == numSend)
    //Assert if sent num bytes equals received num bytes
    assert(numSend * 9 == sum)

    // Cleanup after the test is over
    embeddedKafka.stopCluster()
    stateController.close
    embeddedSpark.stop
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
}
