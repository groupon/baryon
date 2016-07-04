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

package com.groupon.dse.kafka.controllers

import java.util.Properties

import com.groupon.dse.configs.AppConfigs
import com.groupon.dse.configs.AppConfigs.InvalidConfigException

object StateControllerBuilder {

  def apply(properties: Properties): StateController = {
    val stateControllerType = properties.getProperty(
      AppConfigs.StateControllerType._1,
      AppConfigs.StateControllerType._2)

    stateControllerType match {
      case "ZOOKEEPER" => {
        AppConfigs.validate(properties,
          Array(AppConfigs.ZookeeperStateControllerConnect._1, AppConfigs.ZookeeperStateControllerRoot._1))

        val connectionString = properties.getProperty(AppConfigs.ZookeeperStateControllerConnect._1)

        val rootPath = properties.getProperty(AppConfigs.ZookeeperStateControllerRoot._1,
          AppConfigs.ZookeeperStateControllerRoot._2)

        val connTimeout = properties.getProperty(AppConfigs.ZookeeperStateControllerConnTimeoutMs._1,
          AppConfigs.ZookeeperStateControllerConnTimeoutMs._2).toInt

        val sessionTimeout = properties.getProperty(AppConfigs.ZookeeperStateControllerSessionTimeoutMs._1,
          AppConfigs.ZookeeperStateControllerSessionTimeoutMs._2).toInt

        new ZookeeperStateController(connectionString, rootPath, connTimeout, sessionTimeout)
      }

      case "MEMORY" => {
        val startOffset = properties.getProperty(
          AppConfigs.TopicStartOffset._1,
          AppConfigs.TopicStartOffset._2).toInt

        val isBlocking = AppConfigs.validatedBooleanConfig(
          properties,
          AppConfigs.TopicsEnableBlockingConsumption._1,
          AppConfigs.TopicsEnableBlockingConsumption._2)

        if (startOffset != -1) {
          throw InvalidConfigException("In-Memory StateController requires topic.start.offset to be set to -1")
        }

        if (isBlocking) {
          throw InvalidConfigException("Blocking consumption mode not supported for In-Memory StateController. " +
            "Set 'topic.consumption.blocking' to false.")
        }
        new InMemoryStateController
      }

      case _ => throw UnSupportedStateControllerException("Could not find StateController implementation")
    }
  }

  case class UnSupportedStateControllerException(message: String) extends Exception(message)

}
