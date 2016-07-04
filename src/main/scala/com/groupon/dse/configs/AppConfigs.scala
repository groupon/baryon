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

import org.apache.spark.storage.StorageLevel

object AppConfigs {
  val SparkReceivers = ("spark.num.receivers", "1")
  val SparkStorageLevel = ("spark.storage.level", "MEMORY_AND_DISK_SER_2")

  val Topics = ("topics", "")
  val TopicsBlackList = ("topics.blacklist", "")
  val TopicsEnableBlockingConsumption = ("topic.consumption.blocking", "false")
  val TopicConsumptionPolicy = ("topic.consumption.policy", "OFFSET")
  val TopicConsumptionOffsetThreshold = ("topic.consumption.offset.threshold", "0")
  val TopicConsumptionTimeThresholdMs = ("topic.consumption.time.threshold.ms", "1000")
  val TopicFetchSizeBytes = ("topic.fetch.size.bytes", "1048576")
  val TopicRepartitionFactor = ("topic.repartition.factor", "1")
  val TopicStartOffset = ("topic.start.offset", "-1") //-1: Max, -2: Min, Other: Actual offset value

  val PartitionRefreshIntervalMs = ("partition.refresh.interval.ms", "30000")
  val PartitionWarmUpRefreshIntervalMs = ("partition.warmup.refresh.interval.ms", "10000")
  val ReceiverRestIntervalOnFailMs = ("receiver.rest.interval.fail.ms", "2500")
  val ReceiverRestIntervalOnSuccessMs = ("receiver.rest.interval.success.ms", "100")

  val KafkaBrokerConnect = ("kafka.broker.zk.connect", "")
  val KafkaSocketTimeoutMs = ("kafka.socket.timeout", "10000")
  val KafkaSocketBufferSizeBytes = ("kafka.socket.buffer.size", "1048576")
  val KafkaZkSessionTimeoutMs = ("kafka.zk.session.timeout.ms", "10000")
  val KafkaZkConnectionTimeoutMs = ("kafka.zk.connection.timeout.ms", "10000")

  val StateControllerType = ("statecontroller.type", "MEMORY")
  val ZookeeperStateControllerConnect = ("statecontroller.zk.connect", "")
  val ZookeeperStateControllerRoot = ("statecontroller.zk.root", "/baryon")
  val ZookeeperStateControllerConnTimeoutMs = ("statecontroller.zk.conn.timeout.ms", "120000")
  val ZookeeperStateControllerSessionTimeoutMs = ("statecontroller.zk.session.timeout.ms", "60000")

  val TopicFetcherType = ("topics.fetcher.type", "LOCAL")
  val HDFSTopicSource = ("topics.fetcher.hdfs.source", "")
  val HTTPTopicSource = ("topics.fetcher.http.source", "")

  /**
    * Read the contents of a .properties file into a [[Properties]] object. The .properties file
    * should be present in the classpath of the Spark driver class
    *
    * @param fileName File containing the properties
    * @return [[Properties]] object with the user provided properties
    */
  def loadFromFile(fileName: String): Properties = {
    val properties = new Properties()
    val content = scala.io.Source.fromFile(fileName).bufferedReader()
    properties.load(content)
    properties
  }

  /**
    * Fetch the appropriate [[StorageLevel]]
    *
    * @param level StorageLevel as string
    * @return [[StorageLevel]] instance
    */
  def storageLevel(level: String): StorageLevel = {
    level match {
      case "NONE" => StorageLevel.NONE
      case "DISK_ONLY" => StorageLevel.DISK_ONLY
      case "DISK_ONLY_2" => StorageLevel.DISK_ONLY_2
      case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
      case "MEMORY_ONLY_2" => StorageLevel.MEMORY_ONLY_2
      case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_ONLY_SER_2" => StorageLevel.MEMORY_ONLY_SER_2
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case "MEMORY_AND_DISK_2" => StorageLevel.MEMORY_AND_DISK_2
      case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
      case "MEMORY_AND_DISK_SER_2" => StorageLevel.MEMORY_AND_DISK_SER_2
      case "OFF_HEAP" => StorageLevel.OFF_HEAP
    }
  }

  /**
    * Check if the provided [[Properties]] object has the required keys
    *
    * @param properties Provided properties
    * @param requiredProperties Required properties
    */
  @throws[MissingConfigException]
  def validate(properties: Properties, requiredProperties: Seq[String]): Unit = {
    requiredProperties.foreach(c => {
      if (!properties.containsKey(c))
        throw MissingConfigException("Configs missing. Required configs: " + requiredProperties.mkString(", "))
    })
  }

  /**
    * Validate and return a Boolean config value
    *
    * @param properties Provided properties
    * @param propertyName Property to validate
    * @param propertyDefault Default property value
    * @return
    */
  def validatedBooleanConfig(
                              properties: Properties,
                              propertyName: String,
                              propertyDefault: String)
  : Boolean = {
    properties.getProperty(propertyName, propertyDefault) match {
      case "true" => true
      case "false" => false
      case _ => throw InvalidConfigException(s"$propertyName should be set to true or false")
    }
  }

  case class MissingConfigException(message: String) extends Exception(message)

  case class InvalidConfigException(message: String) extends Exception(message)

}
