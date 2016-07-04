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

import com.groupon.dse.configs.ReceiverConfigs
import com.groupon.dse.kafka.common.KafkaException._
import com.groupon.dse.kafka.common.Outcome._
import com.groupon.dse.kafka.partition.Partition
import com.groupon.dse.spark.dstreams.KafkaLowLevelReceiver
import com.groupon.dse.util.Utils
import org.slf4j.LoggerFactory

object KafkaExceptionHandler {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Handle specific Kafka exceptions
    *
    * @param kafkaException Instance of [[KafkaException]]
    * @param partition Current [[Partition]]
    * @param localState Local [[State]] for the [[Partition]]
    * @param receiver Underlying [[KafkaLowLevelReceiver]]
    * @param clientName Receiver name
    *
    * @return Type of [[Outcome]]
    */
  def handleException(kafkaException: KafkaException,
                      partition: Partition,
                      localState: State,
                      consumerClientCache: ConsumerClientCache,
                      receiver: KafkaLowLevelReceiver,
                      clientName: String)
  : Outcome = {
    val stateController = receiver.receiverConfigs.stateController

    val partitionKey = stateController.generateStateKey(partition)

    kafkaException match {
      // Set to next best available offset when required
      case ims: InvalidMessageSizeException => {
        val state = State(localState.offset + 1, localState.timestamp)
        blockingSetState(partitionKey, state, receiver.receiverConfigs)
      }

      case im: InvalidMessageException => {
        val state = State(localState.offset + 1, localState.timestamp)
        blockingSetState(partitionKey, state, receiver.receiverConfigs)
      }

      // Ensure the requested offsets are within the range of stored Kafka offsets
      case oor: OffsetOutOfRangeException => {
        val consumer = consumerClientCache.getWithLoad(partition).get
        val offsets = partition.offsets(consumer)
        val safeOffset = if (localState.offset < offsets.min) offsets.min else offsets.max
        val state = State(safeOffset, localState.timestamp)
        blockingSetState(partitionKey, state, receiver.receiverConfigs)
      }

      // Wait for the error state to resolve
      case utp: UnknownTopicOrPartitionException => {
        logger.error("Wait for error to resolve.", utp)
        Thread.sleep(receiver.receiverConfigs.receiverRestIntervalOnFail)
      }

      // Update the partition->leader mapping
      case lna: LeaderNotAvailableException => {
        logger.error(s"Leader not available for partition: $partition. " +
          s"Will attempt fetch during next iteration", lna)

        // As part of handling this error, we are just removing the SimpleConsumer client associated
        // with the Partition. During the next fetch cycle, as part of re-populating the cache, the
        // Leader for the Partition will also be computed.
        consumerClientCache.remove(partition)
      }

      // Restart receiver, for any other exception
      case ke: KafkaException => receiver.restart(s"$ke. Restarting receiver")
    }

    Outcome.FetchFailure
  }

  /**
    * Continuously attempt to set the state
    *
    * @param key Key of the [[Partition]] which needs state update
    * @param state New [[State]] to set
    * @param receiverConfigs Configs for the receiver
    */
  def blockingSetState(key: String,
                       state: State,
                       receiverConfigs: ReceiverConfigs)
  : Unit = {
    Utils.doActionWithRetry[(String, State)](
      {
        receiverConfigs.stateController.setState(key, state)
      }, (f: Throwable) => {
        logger.error("StateController could not connect to state storage system. Will retry.", f)
        Thread.sleep(receiverConfigs.receiverRestIntervalOnFail)
      },
      -1
    )
  }

}
