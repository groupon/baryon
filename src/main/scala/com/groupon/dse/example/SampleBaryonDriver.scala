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

package com.groupon.dse.example

import java.util.Properties

import com.groupon.dse.example.plugins.EventLoggerPlugin
import com.groupon.dse.spark.plugins.{PluginExecutor, ReceiverPlugin}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Sample application that uses PluginExecutor to start consuming data
  * from Kafka. The PluginExecutor executes a single Plugin that prints the
  * very first message in an RDD fetched from the underlying receiver.
  */
object SampleBaryonDriver {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SampleBaryonDriverApp")
    val batchInterval = Seconds(2)

    // Set the wait time for NODE_LOCAL tasks to 0. This is to prevent executors from waiting a set amount of time until
    // it tries to read data from a Kafka receiver on a different worker node.
    sparkConf.set("spark.locality.wait.node", "0s")

    val streamingContext = new StreamingContext(sparkConf, batchInterval)

    // Create a new plugin executor and start the streaming process
    val executor = new PluginExecutor(streamingContext)
      .forTopics("test-topic")
      .withKafkaZKEndpoint("localhost:2181")
      .usingLocalTopicFetcher
      .usingZKStateController
      .withZKStateControllerEndpoint("localhost:2181")
      .withZKStateControllerPath("/test-app")
      .withProperties(appConfigs)

    // Plugin initializations
    val printHead: ReceiverPlugin = new EventLoggerPlugin
    val plugins = Seq(printHead)

    // Plugin execution
    executor.execute(plugins)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /**
    * Properties required to instantiate Baryon receivers
    *
    * @return Properties object
    */
  def appConfigs: Properties = {
    /**
      * Note:
      * 1. Core properties (topics, kafkaEndpoint, zkEndpoint, zkPath) can also be just added to this properties
      * object. In such scenarios, instantiate the PluginExecutor without calling the Fluent api's. For eg:
      * val pluginExecutor = new PluginExecutor(streamingContext).withProperties(properties)
      *
      * 2. Properties can also be loaded from a .properties file using the AppConfigs.loadFromFile(fileName) API.
      * While submitting a job, make sure the property file is included in the
      * classpath using the '--files' option.
      */

    val properties = new Properties()
    properties.setProperty("spark.num.receivers", "2")
    properties.setProperty("topic.start.offset", "-1")
    properties.setProperty("spark.storage.level", "MEMORY_AND_DISK_SER_2")
    properties.setProperty("partition.refresh.interval.ms", "10000")
    properties
  }
}

