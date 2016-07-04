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

package com.groupon.dse.kafka.cluster.impl

import com.groupon.dse.configs.KafkaServerConfig
import com.groupon.dse.testutils.{EmbeddedKafka, TestDefaults}
import com.groupon.dse.zookeeper.ZkClientBuilder
import kafka.producer.{Producer, ProducerConfig}
import org.I0Itec.zkclient.ZkClient
import org.scalatest.{BeforeAndAfter, FlatSpec}

class ClusterImplTest extends FlatSpec with BeforeAndAfter {

  val kafkaTopic = TestDefaults.TestTopic
  val zkConnTimeout = 10000
  val zkSessionTimeout = 10000
  var producer: Producer[String, Array[Byte]] = _
  var embeddedKafka: EmbeddedKafka = _
  var cluster: ClusterImpl = _
  var zkConnect: String = _
  var kafkaServerConfigs: KafkaServerConfig = _
  var zkClient: ZkClient = _

  before {
    embeddedKafka = new EmbeddedKafka
    embeddedKafka.startCluster()
    producer = new Producer[String, Array[Byte]](new ProducerConfig(embeddedKafka.kafkaProducerProperties))
    zkConnect = embeddedKafka.zkServer.connectString
    kafkaServerConfigs = TestDefaults.testKafkaServerConfig(zkConnect)
    cluster = new ClusterImpl(kafkaServerConfigs)
    zkClient = ZkClientBuilder(zkConnect, zkConnTimeout, zkSessionTimeout)
  }

  after {
    zkClient.close()
    embeddedKafka.stopCluster()
  }

  "The topic list" must "have size 0 before producing" in {
    assert(cluster.topics(zkClient).size == 0)
  }

  "The topic list" must "have size 1 after producing" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    assert(cluster.topics(zkClient).size == 1)
  }

  "The number of partitions for a topic" should "be 1 for 1 valid topic" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    assert(cluster.partitions(List(kafkaTopic), zkClient).size == 1)
  }

  "The number of partitions" should "be 0 for an invalid topic" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    assert(cluster.partitions(List("invalid_topic"), zkClient).size == 0)
  }

  "The number of partitions" should "be 1 for a valid and invalid topic" in {
    embeddedKafka.sendMessage(4, producer, kafkaTopic)
    assert(cluster.partitions(List(kafkaTopic, "invalid_topic"), zkClient).size == 1)
  }

}
