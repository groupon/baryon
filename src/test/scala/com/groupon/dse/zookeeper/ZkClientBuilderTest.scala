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

package com.groupon.dse.zookeeper

import com.groupon.dse.testutils.EmbeddedKafka
import org.scalatest.{BeforeAndAfter, FlatSpec}

class ZkClientBuilderTest extends FlatSpec with BeforeAndAfter {

  val zkConnTimeout = 10000
  val zkSessionTimeout = 10000
  var embeddedKafka: EmbeddedKafka = _
  var zkConnect: String = _

  before {
    embeddedKafka = new EmbeddedKafka
    embeddedKafka.startCluster()
    zkConnect = s"127.0.0.1:${embeddedKafka.zkPort}"
  }

  after {
    embeddedKafka.stopCluster()
  }

  "ZkClient" should "successfully create a new node " in {
    val zkClient = ZkClientBuilder(zkConnect, zkConnTimeout, zkSessionTimeout)
    try {
      zkClient.createPersistent("/testpath")
      assert(zkClient.exists("/testpath"))
    }
    finally {
      zkClient.close()
    }
  }

  "ZkClient" should "successfully create and delete a new node " in {
    val zkClient = ZkClientBuilder(zkConnect, zkConnTimeout, zkSessionTimeout)
    try {
      zkClient.createPersistent("/testpath")
      zkClient.delete("/testpath")
      assert(!zkClient.exists("/testpath"))
    }
    finally {
      zkClient.close()
    }
  }
}
