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

package com.groupon.dse.policy

import java.util.Properties

import com.groupon.dse.kafka.common.State
import com.groupon.dse.kafka.policy.FetchPolicyBuilder.InvalidFetchPolicyException
import com.groupon.dse.kafka.policy.{FetchPolicyBuilder, OffsetBasedFetchPolicy, TimeAndOffsetBasedFetchPolicy, TimeBasedFetchPolicy}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class FetchPolicyTest extends FlatSpec with BeforeAndAfter {
  var local: State = _
  var global: State = _

  before {
    local = State(1, 9000)
    global = State(1, 9500)
  }

  "Offset fetch policy " should " return true " in {
    val policy = new OffsetBasedFetchPolicy(1, 1024, -1)
    assert(policy.canFetch(local, global))
  }

  "Time fetch policy " should " return true " in {
    val policy = new TimeBasedFetchPolicy(100, 1024, -1)
    assert(policy.canFetch(local, global))
  }

  "Time and offset fetch policy " should " return true " in {
    val policy = new TimeAndOffsetBasedFetchPolicy(1, 100, 1024, -1)
    assert(policy.canFetch(local, global))
  }

  "FetchPolicyBuilder for TIME_AND_OFFSET" should " return appropriate class object" in {
    val properties = new Properties()
    properties.setProperty("topic.consumption.policy", "TIME_AND_OFFSET")
    val clazz = FetchPolicyBuilder(properties).getClass
    assert(clazz == classOf[TimeAndOffsetBasedFetchPolicy])
  }

  "FetchPolicyBuilder for OFFSET" should " return appropriate class object" in {
    val properties = new Properties()
    properties.setProperty("topic.consumption.policy", "OFFSET")
    val clazz = FetchPolicyBuilder(properties).getClass
    assert(clazz == classOf[OffsetBasedFetchPolicy])
  }

  "FetchPolicyBuilder for TIME" should " return appropriate class object" in {
    val properties = new Properties()
    properties.setProperty("topic.consumption.policy", "TIME")
    val clazz = FetchPolicyBuilder(properties).getClass
    assert(clazz == classOf[TimeBasedFetchPolicy])
  }

  "FetchPolicyBuilder default config values" should "be" in {
    val properties = new Properties()
    val fetchPolicy = FetchPolicyBuilder(properties)
    assert(fetchPolicy.getClass == classOf[OffsetBasedFetchPolicy] &&
      fetchPolicy.fetchSize == 1048576 &&
      fetchPolicy.startOffset == -1)
  }

  it should " throw exception for invalid fetch policy" in {
    intercept[InvalidFetchPolicyException] {
      val properties = new Properties()
      properties.setProperty("topic.consumption.policy", "UNKNOWN")
      FetchPolicyBuilder(properties)
    }
  }

  "FetchPolicyBuilder with blocking consumption" should " return OffsetBasedFetchPolicy object" in {
    val properties = new Properties()
    properties.setProperty("topic.consumption.blocking", "true")
    val clazz = FetchPolicyBuilder(properties).getClass
    assert(clazz == classOf[OffsetBasedFetchPolicy])
  }

  "FetchPolicyBuilder with blocking consumption and fetch policy" should " return OffsetBasedFetchPolicy object" in {
    val properties = new Properties()
    properties.setProperty("topic.consumption.blocking", "true")
    properties.setProperty("topic.consumption.policy", "TIME")
    val clazz = FetchPolicyBuilder(properties).getClass
    assert(clazz == classOf[OffsetBasedFetchPolicy])
  }
}
