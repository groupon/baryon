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

package com.groupon.dse.configs

import java.util.Properties

import com.groupon.dse.configs.AppConfigs.InvalidConfigException
import com.groupon.dse.kafka.controllers.{StateController, StateControllerBuilder}
import com.groupon.dse.kafka.policy.{FetchPolicy, FetchPolicyBuilder}
import com.groupon.dse.kafka.topic.{TopicFetcher, TopicFetcherBuilder}
import org.slf4j.LoggerFactory

/**
  * Configs that will be sent to each [[com.groupon.dse.spark.dstreams.KafkaLowLevelDStream]]
  */
case class ReceiverConfigs(isBlocking: Boolean,
                           fetchPolicy: FetchPolicy,
                           kafkaServerConfigs: KafkaServerConfig,
                           stateController: StateController,
                           partitionRefreshIntervalMs: Int,
                           partitionWarmUpRefreshIntervalMs: Int,
                           receiverRestIntervalOnFail: Int,
                           receiverRestIntervalOnSuccess: Int,
                           topicRepartitionFactor: Int,
                           topicFetcher: TopicFetcher
                          )
  extends Serializable {

  override def toString: String = s"isBlocking: $isBlocking\n" +
    s"fetchPolicy: $fetchPolicy\n" +
    s"kafkaServerConfigs: $kafkaServerConfigs\n" +
    s"stateController: $stateController\n" +
    s"topicFetcher: $topicFetcher\n" +
    s"partitionRefreshIntervalMs: $partitionRefreshIntervalMs\n" +
    s"partitionWarmUpRefreshIntervalMs: $partitionWarmUpRefreshIntervalMs\n" +
    s"receiverRestIntervalOnFail: $receiverRestIntervalOnFail\n" +
    s"receiverRestIntervalOnSuccess: $receiverRestIntervalOnSuccess\n" +
    s"topicRepartitionFactor: $topicRepartitionFactor]"
}

/**
  * Parse a Properties object to construct a ReceiverConfigs object
  */
object ReceiverConfigBuilder {

  val logger = LoggerFactory.getLogger(getClass)

  def apply(properties: Properties,
            providedStateController: Option[StateController] = None,
            providedTopicFetcher: Option[TopicFetcher] = None
           )
  : ReceiverConfigs = {
    val isBlocking = AppConfigs.validatedBooleanConfig(
      properties,
      AppConfigs.TopicsEnableBlockingConsumption._1,
      AppConfigs.TopicsEnableBlockingConsumption._2)

    val partitionRefreshInterval = properties.getProperty(AppConfigs.PartitionRefreshIntervalMs._1,
      AppConfigs.PartitionRefreshIntervalMs._2).toInt

    val partitionWarmUpInterval = properties.getProperty(AppConfigs.PartitionWarmUpRefreshIntervalMs._1,
      AppConfigs.PartitionWarmUpRefreshIntervalMs._2).toInt

    val receiverRestIntervalOnFail = properties.getProperty(AppConfigs.ReceiverRestIntervalOnFailMs._1,
      AppConfigs.ReceiverRestIntervalOnFailMs._2).toInt

    val receiverRestIntervalOnSuccess = properties.getProperty(AppConfigs.ReceiverRestIntervalOnSuccessMs._1,
      AppConfigs.ReceiverRestIntervalOnSuccessMs._2).toInt

    val kafkaServerConfigs = KafkaServerConfigBuilder(properties)

    val fetchPolicy = FetchPolicyBuilder(properties)

    val stateController = providedStateController match {
      case _: Some[StateController] => providedStateController.get
      case None => StateControllerBuilder(properties)
    }

    val topicRepartitionFactor = properties.getProperty(AppConfigs.TopicRepartitionFactor._1,
      AppConfigs.TopicRepartitionFactor._2).toInt
    if (topicRepartitionFactor < 1) {
      throw InvalidConfigException(s"${AppConfigs.TopicRepartitionFactor._1} must be >= 1")
    }

    val topicFetcher = providedTopicFetcher match {
      case _: Some[TopicFetcher] => providedTopicFetcher.get
      case None => TopicFetcherBuilder(properties)
    }

    val receiverConfigs = ReceiverConfigs(
      isBlocking,
      fetchPolicy,
      kafkaServerConfigs,
      stateController,
      partitionRefreshInterval,
      partitionWarmUpInterval,
      receiverRestIntervalOnFail,
      receiverRestIntervalOnSuccess,
      topicRepartitionFactor,
      topicFetcher
    )

    logger.info(s"New ReceiverConfigs created with properties: $receiverConfigs")

    receiverConfigs
  }


}