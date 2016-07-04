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
import com.groupon.dse.kafka.controllers.{InMemoryStateController, StateController, ZookeeperStateController}
import com.groupon.dse.kafka.policy._
import com.groupon.dse.kafka.topic.LocalTopicFetcher
import org.scalatest.{BeforeAndAfter, FlatSpec}

class ReceiverConfigTest extends FlatSpec with BeforeAndAfter {
  val zkConnTimeout = 10000
  val zkSessionTimeout = 10000
  var properties: Properties = _

  before {
    properties = new Properties()
    properties.setProperty("topics", "sometopic")
    properties.setProperty("kafka.broker.zk.connect", "localhost:2181")
  }

  "ReceiverConfig" should "throw exception if required configs missing" in {
    intercept[AppConfigs.MissingConfigException] {
      val properties = new Properties()
      ReceiverConfigBuilder(properties)
    }
  }

  "ReceiverConfig default values" should "be" in {
    val receiverConfig = ReceiverConfigBuilder(properties)
    assert(receiverConfig.partitionRefreshIntervalMs == 30000)
    assert(receiverConfig.partitionWarmUpRefreshIntervalMs == 10000)
    assert(receiverConfig.receiverRestIntervalOnFail == 2500)
    assert(receiverConfig.receiverRestIntervalOnSuccess == 100)
    assert(receiverConfig.isBlocking == false)
    assert(receiverConfig.stateController.getClass == classOf[InMemoryStateController])
    assert(receiverConfig.fetchPolicy.getClass == classOf[OffsetBasedFetchPolicy])
    assert(receiverConfig.topicRepartitionFactor == 1)
    assert(receiverConfig.topicFetcher.getClass == classOf[LocalTopicFetcher])
  }

  "ReceiverConfig" should "accept the custom StateController" in {
    var stateController: StateController = new InMemoryStateController
    var receiverConfig = ReceiverConfigBuilder(properties, Some(stateController))
    assert(receiverConfig.stateController.getClass == classOf[InMemoryStateController])

    stateController = new ZookeeperStateController("someendpoint", "/somepath", zkConnTimeout, zkSessionTimeout)
    receiverConfig = ReceiverConfigBuilder(properties, Some(stateController))
    assert(receiverConfig.stateController.getClass == classOf[ZookeeperStateController])
  }

  "ReceiverConfig" should "throw exception when blocking enabled for default stateController" in
    intercept[InvalidConfigException] {
      val currentProperties = properties.clone().asInstanceOf[Properties]
      currentProperties.setProperty("topic.consumption.blocking", "true")
      ReceiverConfigBuilder(currentProperties)
    }

  "ReceiverConfig" should "throw exception when invalid value provided for blocking config" in
    intercept[InvalidConfigException] {
      val currentProperties = properties.clone().asInstanceOf[Properties]
      currentProperties.setProperty("topic.consumption.blocking", "random")
      ReceiverConfigBuilder(currentProperties)
    }

  "ReceiverConfig" should "throw exception when invalid value provided for repartition hint config" in
    intercept[InvalidConfigException] {
      val currentProperties = properties.clone().asInstanceOf[Properties]
      currentProperties.setProperty("topic.repartition.factor", "-1")
      ReceiverConfigBuilder(currentProperties)
    }

  "ReceiverConfig" should "return appropriate configs when blocking enabled" in {
    val currentProperties = properties.clone().asInstanceOf[Properties]
    currentProperties.setProperty("topic.consumption.blocking", "true")
    currentProperties.setProperty("statecontroller.type", "ZOOKEEPER")
    currentProperties.setProperty("statecontroller.zk.connect", "localhost:2181")
    currentProperties.setProperty("statecontroller.zk.root", "/test")
    currentProperties.setProperty("topic.consumption.policy", "TIME") //This should get ignored
    val receiverConfig = ReceiverConfigBuilder(currentProperties)
    assert(receiverConfig.isBlocking == true)
    assert(receiverConfig.fetchPolicy.getClass == classOf[OffsetBasedFetchPolicy])
    assert(receiverConfig.stateController.getClass == classOf[ZookeeperStateController])
    assert(receiverConfig.partitionRefreshIntervalMs == 30000)
    assert(receiverConfig.partitionWarmUpRefreshIntervalMs == 10000)
    assert(receiverConfig.receiverRestIntervalOnFail == 2500)
    assert(receiverConfig.receiverRestIntervalOnSuccess == 100)
    assert(receiverConfig.topicRepartitionFactor == 1)
  }
}