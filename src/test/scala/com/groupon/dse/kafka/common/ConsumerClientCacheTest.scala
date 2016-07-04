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

import com.groupon.dse.configs.KafkaServerConfig
import com.groupon.dse.kafka.partition.impl.PartitionImpl
import com.groupon.dse.testutils.{EmbeddedKafka, TestDefaults}
import kafka.consumer.SimpleConsumer
import kafka.producer.{Producer, ProducerConfig}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class ConsumerClientCacheTest extends FlatSpec with BeforeAndAfter {

  val kafkaTopic = TestDefaults.TestTopic
  val clientId = TestDefaults.TestClientId
  val clientName = TestDefaults.TestClientName
  var kafkaServerConfigs: KafkaServerConfig = _
  var partition: PartitionImpl = _
  var embeddedKafka: EmbeddedKafka = _
  var partitionKey: String = _
  var consumerClientCache: ConsumerClientCache = _

  before {
    embeddedKafka = new EmbeddedKafka
    embeddedKafka.startCluster()

    val producer = new Producer[String, Array[Byte]](new ProducerConfig(embeddedKafka.kafkaProducerProperties))
    embeddedKafka.sendMessage(1, producer, kafkaTopic)

    kafkaServerConfigs = TestDefaults.testKafkaServerConfig(embeddedKafka.zkServer.connectString)
    partition = new PartitionImpl(kafkaTopic, 0)
    partitionKey = TestDefaults.TestPathPrefix + "/" + partition.partitionIdentifier
    consumerClientCache = new ConsumerClientCache(kafkaServerConfigs, "test_client")
  }

  after {
    embeddedKafka.stopCluster()
  }

  "Contains on start " should " return false " in {
    assert(!consumerClientCache.contains(partition))
  }


  "Get " should " return None for initial run " in {
    assert(consumerClientCache.get(partition) == None)
  }

  "Get " should " return Some SimpleConsumer client if presetn in cache " in {
    consumerClientCache.getWithLoad(partition)
    assert(consumerClientCache.get(partition).isDefined)
    assert(consumerClientCache.get(partition).get.isInstanceOf[SimpleConsumer])
  }

  "Get with load " should " always return a client " in {
    assert(consumerClientCache.getWithLoad(partition).get.isInstanceOf[SimpleConsumer])
  }

  "Remove " should " remove the partition from the cache " in {
    consumerClientCache.getWithLoad(partition)
    consumerClientCache.remove(partition)
    assert(!consumerClientCache.contains(partition))
  }
}
