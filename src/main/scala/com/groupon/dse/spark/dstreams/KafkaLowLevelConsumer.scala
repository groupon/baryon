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

import com.groupon.dse.configs.ReceiverConfigs
import com.groupon.dse.kafka.common.Outcome._
import com.groupon.dse.kafka.common.{Outcome, _}
import com.groupon.dse.kafka.controllers.StateControllerConnectionException
import com.groupon.dse.kafka.partition.{Leader, Partition, PartitionUtils}
import kafka.consumer.SimpleConsumer
import org.I0Itec.zkclient.exception.ZkException
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.slf4j.LoggerFactory

import scala.collection.mutable.{HashMap => Cache, ListBuffer}

/**
  * Consumer class for fetching messages corresponding to a set of [[Partition]] objects
  *
  * @param receiver [[KafkaLowLevelReceiver]] instance
  */
class KafkaLowLevelConsumer(receiver: KafkaLowLevelReceiver)
  extends Thread {

  private lazy val refreshTimer = UserMetricsSystem.timer("baryon.receiver.refreshTimer")
  private lazy val kafkaExceptionMeter = UserMetricsSystem.meter("baryon.exception.KafkaExceptionRate")
  private lazy val kafkaZkExceptionMeter = UserMetricsSystem.meter("baryon.exception.KafkaZkExceptionRate")
  private lazy val stateControllerConnectionExceptionMeter = UserMetricsSystem.meter("baryon.exception.StateControllerConnectionExceptionRate")
  private lazy val unknownExceptionMeter = UserMetricsSystem.meter("baryon.exception.UnknownExceptionRate")
  private val logger = LoggerFactory.getLogger(classOf[KafkaLowLevelReceiver])
  private val name = s"receiver_${receiver.receiverId}"

  override def run(): Unit = {
    logger.info(s"KafkaLowLevelConsumer thread started for $name.")

    // Data structures required for each Receiver thread
    val receiverConfigs = receiver.receiverConfigs
    val receiverId = receiver.receiverId
    val totalReceivers = receiver.totalReceivers

    val stateCache = Cache.empty[Partition, State]
    val stateKeyCache = Cache.empty[Partition, String]
    val consumerCache = new ConsumerClientCache(receiver.receiverConfigs.kafkaServerConfigs, name)
    val partitionListCache = ListBuffer.empty[Partition]

    // Fetch partitions before starting any further execution
    var partitionLeaderMap = blockingRefresh(
      stateCache,
      stateKeyCache,
      receiverConfigs,
      receiverId,
      totalReceivers,
      partitionListCache)

    var lastCheckTime = System.currentTimeMillis()

    while (!receiver.isStopped()) {
      // Periodically refresh partition list
      if (System.currentTimeMillis() - lastCheckTime > receiver.receiverConfigs.partitionRefreshIntervalMs) {
        partitionLeaderMap = refreshTimer.time({
          refresh(
            stateCache,
            stateKeyCache,
            receiverConfigs,
            receiverId,
            totalReceivers,
            partitionListCache).getOrElse(Map.empty[Partition, Leader])
        })
        lastCheckTime = System.currentTimeMillis()
      }

      val partitions = partitionLeaderMap.keys
      var hasValidFetchOccurred = false
      if (partitions.nonEmpty) {
        partitions.foreach(partition => {
          var outcome: Outcome = Outcome.NoOutcome
          try {
            outcome = processPartition(
              stateCache,
              stateKeyCache,
              partition,
              consumerCache.getWithLoad(partition),
              receiver)

            if (outcome == Outcome.FetchSuccess && !hasValidFetchOccurred) {
              hasValidFetchOccurred = true
            }
          } catch {
            case sc: StateControllerConnectionException => {
              logger.error("Could not connect to underlying state storage for fetching state. Skipping partition")
              stateControllerConnectionExceptionMeter.mark()
              outcome = Outcome.FetchFailure
            }
            case ke: KafkaException => {
              consumerCache.remove(partition)
              logger.error(s"Encountered kafka exception while consuming data for $partition", ke)
              kafkaExceptionMeter.mark()
              outcome = KafkaExceptionHandler.handleException(ke,
                partition,
                stateCache(partition),
                consumerCache,
                receiver,
                name
              )
              val stateController = receiverConfigs.stateController
              val partitionStateKey = stateController.generateStateKey(partition)
              stateCache += partition -> stateController.getState(partitionStateKey)
              logger.info(s"Reset local cache info for partition: $partition to ${stateCache(partition)}")
            }
            case e: Exception => {
              consumerCache.remove(partition)
              logger.error("Unknown exception while fetching data from Kafka", e)
              unknownExceptionMeter.mark()
              outcome = Outcome.FetchFailure
            }
          }
        })
        completePartitionFetchCycle(receiver.receiverConfigs, hasValidFetchOccurred)
      }
    }
  }

  /**
    * Process a [[Partition]]
    *
    * @param partition [[Partition]] to process
    * @return
    */
  @throws[StateControllerConnectionException]
  def processPartition(stateCache: Cache[Partition, State],
                       stateKeyCache: Cache[Partition, String],
                       partition: Partition,
                       consumer: Option[SimpleConsumer],
                       receiver: KafkaLowLevelReceiver)
  : Outcome = {
    var outcome = Outcome.NoOutcome
    val partitionStateKey = stateKeyCache(partition)
    val localState = stateCache(partition)
    val stateController = receiver.receiverConfigs.stateController
    val fetchPolicy = receiver.receiverConfigs.fetchPolicy

    // Proceed only if a valid consumer client present
    if (consumer.isEmpty) {
      logger.error(s"No consumer available for partition: $partition. Skipping.")
    } else {
      // Note: Maybe this is an overkill, but not sure how we achieve async coordination
      val storedState = stateController.getState(partitionStateKey)

      if (fetchPolicy.canFetch(localState, storedState)) {
        logger.debug(s"Fetching messages from Kafka for partition: $partition from offset ${localState.offset}")

        val messageBlocks = retrieveMessages(partition,
          partitionStateKey,
          localState.offset,
          consumer.get,
          receiver.receiverConfigs
        )

        // Write to stream only if you get any data
        if (messageBlocks.nonEmpty) {
          messageBlocks.foreach(block => {
            receiver.store(block.iterator)
            logger.debug(s"Successful write to stream. " +
              s"${block.size} WrappedMessage objects for partition $partition written to stream.")
          })

          val state = State(messageBlocks.head.head.batchEndOffset, System.currentTimeMillis())
          stateCache(partition) = state

          // We should set the state here if consumption type is blocking
          if (!receiver.receiverConfigs.isBlocking) {
            stateController.setState(partitionStateKey, state)
          }

          logger.debug(s"Local state updated for $partition with value $state")

          outcome = Outcome.FetchSuccess
        } else {
          outcome = Outcome.NothingToFetch
        }

      } else {
        logger.debug(s"Fetch policy not met for partition: $partition")
        outcome = Outcome.NothingToFetch
      }
    }
    outcome
  }

  /**
    * Retry till a  non-empty set of partitions are obtained from Kafka
    *
    * @param stateCache Cache for Partition -> State mapping
    * @param stateKeyCache Cache for Partition -> PartitionStateKey mapping
    * @param receiverConfigs Global receiver configs
    * @param receiverId ID to uniquely identify the current Receiver
    * @param totalReceivers Total receivers being used by the application
    * @param partitionListCache Cache for all topic Partitions
    *
    * @return [[Partition]] -> [[Leader]] mappings
    */
  def blockingRefresh(stateCache: Cache[Partition, State],
                      stateKeyCache: Cache[Partition, String],
                      receiverConfigs: ReceiverConfigs,
                      receiverId: Int,
                      totalReceivers: Int,
                      partitionListCache: ListBuffer[Partition])
  : Map[Partition, Leader] = {
    val partitionLeaderMap = refresh(
      stateCache,
      stateKeyCache,
      receiverConfigs,
      receiverId,
      totalReceivers,
      partitionListCache).orNull

    if (partitionLeaderMap == null) {
      logger.warn(s"No partitions found yet. Will retry in ${receiverConfigs.partitionWarmUpRefreshIntervalMs} ms")
      Thread.sleep(receiverConfigs.partitionWarmUpRefreshIntervalMs)

      blockingRefresh(
        stateCache,
        stateKeyCache,
        receiverConfigs,
        receiverId,
        totalReceivers,
        partitionListCache)
    } else {
      logger.info("Partitions discovered for consuming. Will initiate fetch.")
      partitionLeaderMap
    }
  }

  /**
    * Rest after attempting fetch from each partition in the list
    *
    * @param receiverConfigs Global receiver configs
    * @param hasValidFetchOccurred True, if at least one valid fetch from the partition list occurred
    */
  def completePartitionFetchCycle(receiverConfigs: ReceiverConfigs,
                                  hasValidFetchOccurred: Boolean)
  : Unit = {
    if (hasValidFetchOccurred) {
      logger.warn(s"Iterated entire partition list for performing Kafka fetch. " +
        s"Will resume in ${receiverConfigs.receiverRestIntervalOnSuccess} ms")

      Thread.sleep(receiverConfigs.receiverRestIntervalOnSuccess)
    } else {
      logger.warn(s"No data fetched from any partitions in the list. " +
        s"Will resume in ${receiverConfigs.receiverRestIntervalOnFail} ms")

      Thread.sleep(receiverConfigs.receiverRestIntervalOnFail)
    }
  }

  /**
    * Connect to Kafka and obtain the partition->leader mapping
    *
    * @param stateCache Cache for Partition -> State mapping
    * @param stateKeyCache Cache for Partition -> PartitionStateKey mapping
    * @param receiverConfigs Global receiver configs
    * @param receiverId ID to uniquely identify the current Receiver
    * @param totalReceivers Total receivers being used by the application
    * @param partitionListCache Cache for all topic Partitions
    *
    * @return [[Partition]]->[[Leader]] mapping
    */
  def refresh(stateCache: Cache[Partition, State],
              stateKeyCache: Cache[Partition, String],
              receiverConfigs: ReceiverConfigs,
              receiverId: Int,
              totalReceivers: Int,
              partitionListCache: ListBuffer[Partition])
  : Option[Map[Partition, Leader]] = {

    try {
      val partitionsList = PartitionUtils.partitionsForAllReceivers(receiverConfigs)

      // We should only add the new partitions(topics) to the partitionListCache ie partitions which we have not seen before
      // This also ensures that we continue using the cached partition list in case of any exceptions when trying to fetch the topics.
      val newPartitions = partitionsList.filterNot(partitionListCache.contains(_))

      partitionListCache ++= newPartitions

      val partitionLeaderMap = PartitionUtils.partitionLeaderMapForReceiver(receiverConfigs,
        receiverId,
        totalReceivers,
        partitionListCache.toList)

      refreshLocalCaches(
        stateCache,
        stateKeyCache,
        partitionLeaderMap,
        receiverConfigs)

      logger.info(s"Refreshing partition list for receiver(id=$receiverId). " +
        s"Partitions to fetch: ${partitionLeaderMap.keys.mkString(",")}")

      Some(partitionLeaderMap)
    } catch {
      case sc: StateControllerConnectionException => {
        logger.error("Partition refresh failed due to StateController connection problems")
        stateControllerConnectionExceptionMeter.mark()
        Thread.sleep(receiverConfigs.receiverRestIntervalOnFail)
        None
      }
      case zk: ZkException => {
        logger.error("Partition refresh failed due to Kafka Zookeeper connection problems")
        kafkaZkExceptionMeter.mark()
        Thread.sleep(receiverConfigs.receiverRestIntervalOnFail)
        None
      }
    }
  }

  /**
    * Initialize the local caches used by the Receiver
    * Local state will be set to State(-1, timestamp) if a previous state was not found
    *
    * @param stateCache Cache for Partition -> State mapping
    * @param stateKeyCache Cache for Partition -> PartitionStateKey mapping
    * @param partitionLeaderMap Mapping between [[Partition]] and their [[Leader]]
    * @param receiverConfigs Global receiver configs
    */
  def refreshLocalCaches(stateCache: Cache[Partition, State],
                         stateKeyCache: Cache[Partition, String],
                         partitionLeaderMap: Map[Partition, Leader],
                         receiverConfigs: ReceiverConfigs)
  : Unit = {
    logger.info("Refreshing local caches")
    val newPartitions = partitionLeaderMap.keySet.diff(stateCache.keys.toSet)
    val newPartitionsLeaderMap = partitionLeaderMap.filterKeys(newPartitions)

    stateCache ++= PartitionUtils.initializePartitionStates(
      newPartitionsLeaderMap,
      receiverConfigs,
      name)

    stateKeyCache ++= PartitionUtils.partitionStateKeys(
      newPartitionsLeaderMap.keys.toSeq,
      receiverConfigs.stateController)
  }

  /**
    * Fetch messages from Kafka and a list of [[WrappedMessage]] blocks to store in the receiver.
    *
    * This groups the fetched messages into multiple blocks, where each block is at most `receiverConfigs.maxBlockSize`
    * bytes large. This is useful primarily in blocking mode where a single fetch from a Kafka partition in each batch
    * interval may not give us the parallelism (i.e. # of RDD partitions) that we want.
    *
    * @param partition [[Partition]] to consume messages from
    * @param currentState Current [[State]] of the [[Partition]]
    * @param receiverConfigs Configs for the Receiver
    * @param partitionStateKey Partition key that needs to be associated with the message set
    *
    * @return Iterable of iterables of [[WrappedMessage]] objects, where the size of the payloads in each
    *         `Iterable[WrappedMessage]` is at most `receiverConfigs.maxBlockSize` bytes
    */
  def retrieveMessages(partition: Partition,
                       partitionStateKey: String,
                       currentState: Long,
                       consumer: SimpleConsumer,
                       receiverConfigs: ReceiverConfigs)
  : Iterable[Iterable[WrappedMessage]] = {
    val messages = partition.fetchMessages(currentState,
      receiverConfigs.fetchPolicy.fetchSize,
      partitionStateKey,
      consumer)
    // `ceil` operation here is to prevent having a partition that's very small relative to all the others when the
    // number of messages isn't divisible by the repartition factor
    // e.g. if `topicRepartitionFactor` is 2, any time `messages` is of size 2N + 1 we would end up with partitions of
    // size (N, N, 1) if we just perform a `grouped()` with just the `topicRepartitionFactor`
    // `max` handles the case when we did not fetch any messages
    val numPartitions = math.max(math.ceil(messages.size / receiverConfigs.topicRepartitionFactor.toDouble).toInt, 1)
    messages.grouped(numPartitions).toIterable
  }
}
