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

import com.groupon.dse.configs.{KafkaServerConfig, ReceiverConfigs}
import com.groupon.dse.kafka.cluster.impl.ClusterImpl
import com.groupon.dse.kafka.common.KafkaException.LeaderNotAvailableException
import com.groupon.dse.kafka.common.{ConsumerClientBuilder, State}
import com.groupon.dse.kafka.controllers.{KeyDoesNotExistException, StateController, StateControllerConnectionException}
import com.groupon.dse.zookeeper.ZkClientBuilder
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkException
import org.slf4j.LoggerFactory


/**
  * Utility class to operate on various [[Partition]] objects
  */
object PartitionUtils {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Generate [[StateController]] keys for each [[Partition]]
    *
    * @param partitions List of [[Partition]] object for which state key needs to be generated
    * @param stateController [[StateController]] object to manage [[Partition]] state
    * @return [[Partition]] to key mapping
    */
  def partitionStateKeys(partitions: Seq[Partition],
                         stateController: StateController)
  : Map[Partition, String] = {
    val partitionKeyMap = scala.collection.mutable.HashMap.empty[Partition, String]
    partitions.foreach(p => partitionKeyMap += (p -> stateController.generateStateKey(p)))
    partitionKeyMap.toMap
  }

  /**
    * Load the initial state for the partitions
    *
    * @param partitionLeaderMap [[Partition]] to [[Leader]] mapping
    * @return [[Partition]] to [[State]] mapping
    */
  @throws[StateControllerConnectionException]
  def initializePartitionStates(partitionLeaderMap: Map[Partition, Leader],
                                receiverConfigs: ReceiverConfigs,
                                clientName: String)
  : Map[Partition, State] = {
    val cache = scala.collection.mutable.HashMap.empty[Partition, State]
    val partitions = partitionLeaderMap.keys
    partitions.foreach(p => {
      cache += (p -> initializePartitionState(p,
        partitionLeaderMap(p),
        receiverConfigs,
        clientName))
    })
    cache.toMap
  }

  /**
    * Get the local state for [[Partition]] if present, else generate key
    * and set the initial state
    *
    * @param partition [[Partition]] object
    * @param leader [[Leader]] for the given [[Partition]]
    * @return [[State]] of the partition
    */
  @throws[StateControllerConnectionException]
  def initializePartitionState(partition: Partition,
                               leader: Leader,
                               receiverConfigs: ReceiverConfigs,
                               clientName: String)
  : State = {
    val stateController = receiverConfigs.stateController
    val partitionKey = stateController.generateStateKey(partition)
    val consumer = ConsumerClientBuilder.newInstance(leader, receiverConfigs.kafkaServerConfigs, clientName)
    try {
      stateController.getState(partitionKey)
    } catch {
      case kde: KeyDoesNotExistException => {
        val startOffset = receiverConfigs.fetchPolicy.startOffset match {
          case -1 => {
            val offset = partition.offsets(consumer).max
            logger.info(s"Initial state for partition: ${partition.partitionIdentifier} set to max available offset " +
              s"value of $offset")
            offset
          }
          case -2 => {
            val offset = partition.offsets(consumer).min
            logger.info(s"Initial state for partition: ${partition.partitionIdentifier} set to min available offset " +
              s"value of $offset")
            offset
          }
          case offset: Long => {
            logger.info(s"Initial state for partition: ${partition.partitionIdentifier} set to offset: $offset")
            offset
          }
        }
        val currentTime = System.currentTimeMillis()
        val controllerState = State(startOffset, currentTime)

        logger.warn(s"Key does not exist for partition: $partition. " +
          s"Creating a new one with value set to $controllerState")

        stateController.setState(partitionKey, controllerState)
        controllerState
      }
    } finally {
      consumer.close()
    }
  }

  /**
    * Compute [[Partition]] to [[Leader]] mapping for a given receiver
    *
    * @param receiverConfigs Global configs shared by all receivers
    * @param receiverId Receiver id
    * @param totalReceivers Total number of receivers started by the driver
    * @return Mapping between [[Partition]] and their [[Leader]]
    */
  @throws[ZkException]
  def partitionLeaderMapForReceiver(receiverConfigs: ReceiverConfigs,
                                    receiverId: Int,
                                    totalReceivers: Int,
                                    partitions: List[Partition])
  : Map[Partition, Leader] = {
    val cluster = new ClusterImpl(receiverConfigs.kafkaServerConfigs)

    val receiverPartitions = partitionsForReceiver(
      partitions,
      receiverId,
      totalReceivers)

    val partitionAndLeaders = partitionLeaderMap(
      cluster,
      receiverPartitions,
      receiverConfigs.kafkaServerConfigs)

    partitionAndLeaders
  }

  /**
    * Filter partitions based on the receiver id
    *
    * @param partitions All partitions
    * @param receiverId Id of the current receiver
    * @param totalReceivers Total receivers assigned
    * @return Subset of partitions filtered for the current receiver
    */
  @throws[IllegalArgumentException]
  def partitionsForReceiver(partitions: Seq[Partition],
                            receiverId: Int,
                            totalReceivers: Int)
  : Seq[Partition] = {
    if (receiverId >= totalReceivers) {
      throw new IllegalArgumentException("receiverId is greater than total receiver count")
    }
    if (partitions.length < totalReceivers) {
      logger.warn("More receivers than partitions. Certain receivers will be idle.")
    }

    val receiverPartitions = scala.collection.mutable.ArrayBuffer.empty[Partition]
    for (i <- receiverId until partitions.length by totalReceivers) {
      receiverPartitions += partitions(i)
    }
    receiverPartitions.toSeq
  }

  /**
    * Fetch leaders for the given partition list
    *
    * @param cluster [[ClusterImpl]] object to interact with Kafka
    * @param partitions List of partitions
    * @param serverConfigs [[KafkaServerConfig]] associated with the current Receiver
    * @return [[Partition]] to [[Leader]] mapping
    */
  @throws[ZkException]
  def partitionLeaderMap(cluster: ClusterImpl,
                         partitions: Seq[Partition],
                         serverConfigs: KafkaServerConfig)
  : Map[Partition, Leader] = {
    val partitionLeaderMap = scala.collection.mutable.HashMap.empty[Partition, Leader]

    val zkClient = zkClientForReceiver(serverConfigs)
    try {
      partitions.foreach(p => {
        try {
          partitionLeaderMap += p -> p.leader(zkClient)
        } catch {
          case lna: LeaderNotAvailableException => logger.error(s"Could not fetch leader for partition: $p. " +
            s"No data will be fetched for the partition until next refresh.")

          case zke: ZkException => logger.error("Zookeeper exception while fetching leader for partition: $p . " +
            s"No data will be fetched for the partition until next refresh.")
        }
      })
    } finally {
      zkClient.close()
    }
    partitionLeaderMap.toMap
  }

  /**
    * Create a ZkClient instance with configs specific to the current Receiver
    *
    * @param serverConfigs [[KafkaServerConfig]] associated with the current Receiver
    * @return ZkClient instance
    */
  private def zkClientForReceiver(serverConfigs: KafkaServerConfig)
  : ZkClient = ZkClientBuilder(
    serverConfigs.brokerZk,
    serverConfigs.zkConnectionTimeout,
    serverConfigs.zkSessionTimeout)

  /**
    * Compute global list of [[Partition]] for the all provided topics.
    *
    * @param receiverConfigs Global configs shared by all receivers
    * @return List of all [[Partition]] for the provided topics
    */
  def partitionsForAllReceivers(receiverConfigs: ReceiverConfigs)
  : List[Partition] = {
    val cluster = new ClusterImpl(receiverConfigs.kafkaServerConfigs)
    val zkClient = zkClientForReceiver(receiverConfigs.kafkaServerConfigs)

    val topicNames = receiverConfigs.topicFetcher.fetchTopics.map(topic => topic.name)

    if (topicNames.isEmpty) return List()

    val validTopics = filterTopics(cluster.topics(zkClient), topicNames)

    try {
      val partitions = cluster.partitions(
        validTopics,
        zkClient)

      partitions.toList
    } finally {
      zkClient.close
    }
  }

  /**
    * Given the list of all topics and the list of topics provided by the fetcher, filter the invalid topics,
    * i.e. topics not present in the list of all topics in the cluster
    *
    * @param clusterTopics List of all topics
    * @param topics List of topics provided by the topic fetcher
    * @return Valid list of topics to be consumed
    */
  def filterTopics(clusterTopics: Seq[String],
                   topics: Seq[String])
  : Seq[String] = {
    // Return empty if no topics to process
    if (clusterTopics.isEmpty) {
      return Seq()
    }

    val validTopics = topics.filter(t => clusterTopics.contains(t))
    validTopics.distinct
  }
}
