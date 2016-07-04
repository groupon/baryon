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

package com.groupon.dse.testutils

import java.io.File
import java.net.InetSocketAddress

import kafka.utils.Utils
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

/**
  * Embedded zookeeper to run locally
  * @param zkPort The port to start the local zookeeper on
  */
class EmbeddedZookeeper(zkPort: Int, zkSnapshotDir: File, zkLogDir: File, autoStart: Boolean) {

  val connectString = s"127.0.0.1:${zkPort}"
  val tickTime = 500
  val factory = new NIOServerCnxnFactory()
  val maxZkConnections = 100
  factory.configure(new InetSocketAddress("127.0.0.1", zkPort), maxZkConnections)

  var zookeeper: ZooKeeperServer = null

  def this(port: Int, autoStart: Boolean = true) = this(port, TestUtils.tempDir, TestUtils.tempDir, autoStart)

  if (autoStart) {
    start()
  }

  def start(): Unit = {
    // With Zookeeper 3.4, the startup logic of the ZookeeperServer has changed where a sequence of:
    // zookeeper.start() -> zookeeper.shutdown() -> zookeeper.start()
    // will fail to restart the ZookeeperServer. Because of this, a new ZookeeperServer needs to be instantiated if
    // we want to simulate Zookeeper unavailability during tests
    zookeeper = new ZooKeeperServer(zkSnapshotDir, zkLogDir, tickTime)
    factory.startup(zookeeper)
  }

  def stop(): Unit = {
    zookeeper.shutdown()
    zookeeper = null
  }

  def snapshotDir: File = zkSnapshotDir

  def logDir: File = zkLogDir

  def cleanShutdown(): Unit = {
    shutdown()
    Utils.rm(zkLogDir)
    Utils.rm(zkSnapshotDir)
  }

  def shutdown(): Unit = {
    Utils.swallow(zookeeper.shutdown())
    Utils.swallow(factory.shutdown())
    zookeeper = null
  }
}

