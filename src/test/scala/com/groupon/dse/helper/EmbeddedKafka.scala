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

package com.groupon.dse.testutils

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer}
import kafka.server.{KafkaConfig, KafkaServer}


/**
  * KafkaCluster is used to start/stop a kafka cluster. It starts an embedded zookeeper and a kafka broker for testing purposes.
  */
class EmbeddedKafka {
  var zkPort: Int = _
  var kafkaPort: Int = _
  var zkServer: EmbeddedZookeeper = _
  var kafkaServer: KafkaServer = _

  /**
    * Starts a zookeeper and kafka broker
    */
  def startCluster(): Unit = {
    zkServer = startZkServer()
    kafkaServer = startKafkaServer(zkServer.connectString)
  }

  /**
    * Starts a local kafka broker using a randomly selected port
    * @param zkConnect The zookeeper connect string
    * @return the Kafka server object
    */
  def startKafkaServer(zkConnect: String) = {
    kafkaPort = TestUtils.choosePorts(1)(0)
    val logDir = TestUtils.tempDir
    val kafkaBrokerProperties = new Properties() {
      put("broker.id", "0")
      put("log.dirs", String.valueOf(logDir))
      put("zookeeper.connect", zkConnect)
      put("port", kafkaPort.toString)
      put("host.name", "localhost")
    }
    val server = new KafkaServer(new KafkaConfig(kafkaBrokerProperties))
    server.startup()
    server
  }

  /**
    * Starts a local zookeeper using a randomly selected port
    * @return the Embedded Zookeeper object
    */
  def startZkServer() = {
    zkPort = TestUtils.choosePorts(1)(0)
    val server = new EmbeddedZookeeper(zkPort)
    server
  }

  /**
    * Stops a zookeeper and kafka broker
    */
  def stopCluster(): Unit = {
    if (kafkaServer != null)
      kafkaServer.shutdown()
    if (zkServer != null)
      zkServer.cleanShutdown()
  }

  /**
    * @return Properties required by the producer
    */
  def kafkaProducerProperties = new Properties() {
    put("metadata.broker.list", s"127.0.0.1:${kafkaPort}")
    put("request.required.acks", "-1")
    put("producer.type", "sync")

  }

  /**
    * @return Properties required by the consumer
    */
  def kafkaConsumerProperties = new Properties() {
    put("group.id", "unit-test-id")
    put("zookeeper.connect", s"127.0.0.1:${zkPort}")
    put("zookeeper.session.timeout.ms", "6000")
    put("auto.offset.reset", "smallest")
    put("auto.commit.interval.ms", "10")
    put("consumer.id", "consumerid")
    put("consumer.timeout.ms", "-1")
    put("rebalance.max.retries", "4")
    put("num.consumer.fetchers", "2")
  }

  def sleep(ms: Long) = Thread.sleep(ms)

  /**
    * Send a message to the kafka broker
    * @param cnt The number of time to send the message
    * @param producer The producer object to use to send
    * @param kafkaTopic The topic to send to
    * @param message Message to send
    */
  def sendMessage(cnt: Int,
                  producer: Producer[String, Array[Byte]],
                  kafkaTopic: String,
                  message: Array[Byte] = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9)): Unit = {
    for (x <- 1 to cnt)
      producer.send(new KeyedMessage[String, Array[Byte]](kafkaTopic, message))
  }

}
