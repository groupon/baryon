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
import java.net.ServerSocket
import java.util.Random

import kafka.utils.Utils

/**
  * Provides a set of utilities for the embedded zookeeper and kafka broker
  */
object TestUtils {
  private[this] val seededRandom = new Random(192348092834L)
  private[this] val random = new Random

  /**
    * Creates a temp directory required by the zk. It deleted the dir on shutdown
    * Returns a java File object
    */
  def tempDir: File = {
    val ioDir = System.getProperty("java.io.tmpdir")
    val dir = new File(ioDir, "kafka-" + random.nextInt(10000000))
    dir.mkdirs
    println(s"tmp file : ${dir.getCanonicalPath}")
    dir.deleteOnExit()
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        Utils.rm(dir)
      }
    })
    dir
  }

  /**
    * Returns n random ports for the zookeeper and broker
    * @param count indicates the number of random ports to return
    * @return Array of n random ports
    */
  def choosePorts(count: Int) = {
    val sockets =
      for (i <- 0 until count)
        yield new ServerSocket(0)
    val socketList = sockets.toList
    val ports = socketList.map(_.getLocalPort)
    socketList map (_.close())
    ports
  }
}
