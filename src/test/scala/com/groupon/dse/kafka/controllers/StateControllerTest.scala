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
import org.scalatest.FlatSpec

class StateControllerTest extends FlatSpec {

  "StateControllerBuilder for ZOOKEEPER" should " return appropriate class object" in {
    val properties = new Properties()
    properties.setProperty("statecontroller.type", "ZOOKEEPER")
    properties.setProperty("statecontroller.zk.connect", "localhost:2181")
    properties.setProperty("statecontroller.zk.root", "/test")
    assert(StateControllerBuilder(properties).getClass == classOf[ZookeeperStateController])
  }

  "StateControllerBuilder" should "return InMemory controller if stateController type is missing " in {
    val properties = new Properties()
    assert(StateControllerBuilder(properties).getClass == classOf[InMemoryStateController])
  }

  "StateControllerBuilder" should " throw exception if required zkstatecontroller connect config missing" in {
    intercept[AppConfigs.MissingConfigException] {
      val properties = new Properties()
      properties.setProperty("statecontroller.type", "ZOOKEEPER")
      StateControllerBuilder(properties)
    }
  }
  "StateControllerBuilder" should " throw exception if state controller is in-memory and blocking fetch policy set " in
    intercept[InvalidConfigException] {
      val properties = new Properties()
      properties.setProperty("statecontroller.type", "MEMORY")
      properties.setProperty("topic.consumption.blocking", "true")
      StateControllerBuilder(properties)
    }

  "StateControllerBuilder" should " throw exception if state controller is in-memory and start offset is not -1 " in
    intercept[InvalidConfigException] {
      val properties = new Properties()
      properties.setProperty("statecontroller.type", "MEMORY")
      properties.setProperty("topic.start.offset", "-2")
      StateControllerBuilder(properties)
    }
}
