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
import com.groupon.dse.kafka.common._
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory

/**
  * DStream for consuming from Kafka
  *
  * @param ssc Current [[StreamingContext]]
  * @param receiverConfigs Configs for the receiver
  * @param receiverId Id of the current receiver
  * @param totalReceivers Total number of receivers
  * @param storageLevel [[StorageLevel]] for RDD's
  */
class KafkaLowLevelDStream(@transient ssc: StreamingContext,
                           receiverConfigs: ReceiverConfigs,
                           receiverId: Int,
                           totalReceivers: Int,
                           storageLevel: StorageLevel)
  extends ReceiverInputDStream[WrappedMessage](ssc) {

  override def getReceiver(): Receiver[WrappedMessage] = new KafkaLowLevelReceiver(ssc.sparkContext,
    receiverConfigs,
    receiverId,
    totalReceivers,
    storageLevel).asInstanceOf[Receiver[WrappedMessage]]
}

/**
  * Receiver for consuming from Kafka
  *
  * @param sc Current [[SparkContext]]
  * @param receiverConfigs Configs for the receiver
  * @param receiverId Id of the current receiver
  * @param totalReceivers Total number of receivers
  * @param storageLevel [[StorageLevel]] for RDD's
  */
class KafkaLowLevelReceiver(sc: SparkContext,
                            val receiverConfigs: ReceiverConfigs,
                            val receiverId: Int,
                            val totalReceivers: Int,
                            storageLevel: StorageLevel)
  extends Receiver[WrappedMessage](storageLevel) {

  private val logger = LoggerFactory.getLogger(classOf[KafkaLowLevelReceiver])

  override def onStart(): Unit = {
    val workerThread = new KafkaLowLevelConsumer(this)
    workerThread.start()
    logger.info(s"Receiver with id $receiverId started")
  }

  override def onStop(): Unit = {
    logger.info(s"Shutting down receiver with id $receiverId")
    receiverConfigs.stateController.close
  }
}
