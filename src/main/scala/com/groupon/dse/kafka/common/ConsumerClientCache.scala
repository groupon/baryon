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

import com.groupon.dse.configs.{AppConfigs, KafkaServerConfig}
import com.groupon.dse.kafka.common.KafkaException.LeaderNotAvailableException
import com.groupon.dse.kafka.partition.{Leader, Partition}
import com.groupon.dse.zookeeper.ZkClientBuilder
import kafka.consumer.SimpleConsumer
import org.I0Itec.zkclient.exception.ZkException
import org.slf4j.LoggerFactory

/**
  * Cache for storing SimpleConsumer instances for [[Partition]] objects
  */
class ConsumerClientCache(serverConfig: KafkaServerConfig, clientName: String) extends Serializable {

  private val logger = LoggerFactory.getLogger(getClass)
  private val consumerClientCache = scala.collection.mutable.HashMap.empty[Partition, SimpleConsumer]

  /**
    * Get the SimpleConsumer client created for the Partition.
    * Insert new entry if it does not already exist
    *
    * @param partition [[Partition]] for which a consumer client is required
    * @return Cached SimpleConsumer instance
    */
  def getWithLoad(partition: Partition): Option[SimpleConsumer] = {
    if (!consumerClientCache.contains(partition)) {
      val zkConnTimeout = AppConfigs.ZookeeperStateControllerConnTimeoutMs._2.toInt
      val zkSessionTimeout = AppConfigs.ZookeeperStateControllerSessionTimeoutMs._2.toInt
      val zkClient = ZkClientBuilder(serverConfig.brokerZk, zkConnTimeout, zkSessionTimeout)
      try {
        val leader = partition.leader(zkClient)
        val client = ConsumerClientBuilder.newInstance(leader, serverConfig, clientName)
        consumerClientCache += partition -> client
      } catch {
        case lna: LeaderNotAvailableException => {
          logger.error(s"Cannot create consumer object for partition: $partition. Leader unavailable.")
          None
        }
        case zke: ZkException => {
          logger.error(s"Zookeeper exception while creating consumer object for partition: $partition", zke)
          None
        }

      } finally {
        zkClient.close()
      }
    }
    Some(consumerClientCache(partition))
  }

  /**
    * Get the SimpleConsumer client created for the Partition. Return None if no entry found
    *
    * @param partition [[Partition]] for which a consumer client is required
    * @return Cached SimpleConsumer instance
    */
  def get(partition: Partition): Option[SimpleConsumer] = {
    if (consumerClientCache.contains(partition)) {
      Some(consumerClientCache(partition))
    } else {
      None
    }
  }

  /**
    * Remove from cache the SimpleConsumer instance corresponding to the [[Partition]]
    *
    * @param partition [[Partition]] for which the the consumer client entry should be removed
    */
  def remove(partition: Partition): Unit = {
    if (consumerClientCache.contains(partition)) {
      consumerClientCache(partition).close()
      consumerClientCache.remove(partition)
    }
  }

  /**
    * Check if a [[Partition]] object is present in cache
    *
    * @param partition [[Partition]] object key
    * @return True if the [[Partition]] object key exists
    */
  def contains(partition: Partition): Boolean = consumerClientCache.contains(partition)
}

/**
  * Builder for Kafka SimpleConsumer clients
  */
object ConsumerClientBuilder {

  /**
    * Create a new Kafka SimpleConsumer instance
    *
    * @param leader [[Leader]] contains the Kafka broker:port to connect to
    * @param serverConfig Kafka server configs
    * @param clientName Name of the client
    * @return
    */
  def newInstance(leader: Leader,
                  serverConfig: KafkaServerConfig,
                  clientName: String)
  : SimpleConsumer = new SimpleConsumer(
    leader.host,
    leader.port,
    serverConfig.socketTimeout,
    serverConfig.socketBufferSize,
    clientName
  )
}
