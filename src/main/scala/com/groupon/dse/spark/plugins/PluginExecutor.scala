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

package com.groupon.dse.spark.plugins

import java.util.Properties

import com.groupon.dse.configs.{AppConfigs, ReceiverConfigBuilder}
import com.groupon.dse.kafka.controllers.{StateController, StateControllerBuilder}
import com.groupon.dse.kafka.topic.TopicFetcher
import com.groupon.dse.spark.dstreams.KafkaLowLevelDStream
import com.groupon.dse.spark.listeners.{MetricsListener, StreamingMetricsListener}
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

/**
  * The PluginExecutor class is responsible for serially executing [[ReceiverPlugin]] objects
  *
  * @param streamingContext StreamingContext to begin Spark Receivers
  */
class PluginExecutor(streamingContext: StreamingContext) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val appProperties = new Properties()

  private val TopicsConfig = AppConfigs.Topics._1
  private val KafkaBrokerConfig = AppConfigs.KafkaBrokerConnect._1
  private val StateControllerTypeConfig = AppConfigs.StateControllerType._1
  private val ZKEndpointConfig = AppConfigs.ZookeeperStateControllerConnect._1
  private val ZKRootPathConfig = AppConfigs.ZookeeperStateControllerRoot._1
  private val TopicFetcherTypeConfig = AppConfigs.TopicFetcherType._1
  private val HdfsTopicSource = AppConfigs.HDFSTopicSource._1
  private val HttpTopicSource = AppConfigs.HTTPTopicSource._1
  private[this] var stateController: Option[StateController] = None
  private[this] var topicFetcher: Option[TopicFetcher] = None

  UserMetricsSystem.initialize(streamingContext.sparkContext)
  streamingContext.addStreamingListener(new StreamingMetricsListener)
  streamingContext.sparkContext.addSparkListener(new MetricsListener)

  /**
    * Set the list of Kafka topics to fetch data from
    *
    * @param topic Topic list (could be list of regular expressions)
    * @return Current [[PluginExecutor]] instance
    */
  def forTopics(topic: String): this.type = {
    appProperties.setProperty(TopicsConfig, topic)
    this
  }

  /**
    * Set the Kafka Zookeeper endpoint
    *
    * @param brokerZk Kafka Zookeeper endpoint
    * @return Current [[PluginExecutor]] instance
    */
  def withKafkaZKEndpoint(brokerZk: String): this.type = {
    appProperties.setProperty(KafkaBrokerConfig, brokerZk)
    this
  }

  /**
    * Set StateController to a custom implementation
    *
    * @param customStateController Custom user provided StateController implementation
    * @return Current [[PluginExecutor]] instance
    */
  def usingStateController(customStateController: StateController): this.type = {
    stateController = Some(customStateController)
    this
  }

  /**
    * Set StateController to InMemoryStateController
    *
    * @return Current [[PluginExecutor]] instance
    */
  def usingInMemoryStateController: this.type = {
    appProperties.setProperty(StateControllerTypeConfig, "MEMORY")
    this
  }

  /**
    * Set StateController to ZookeeperStateController
    *
    * @return Current [[PluginExecutor]] instance
    */
  def usingZKStateController: this.type = {
    appProperties.setProperty(StateControllerTypeConfig, "ZOOKEEPER")
    this
  }

  /**
    * Set the Zookeeper StateController connection endpoint
    *
    * @param zkEndpoint Zookeeper StateController endpoint
    * @return Current [[PluginExecutor]] instance
    */
  def withZKStateControllerEndpoint(zkEndpoint: String): this.type = {
    appProperties.setProperty(ZKEndpointConfig, zkEndpoint)
    this
  }

  /**
    * Set the Zookeeper StateController root path
    *
    * @param zkPath Zookeeper StateController root path
    * @return Current [[PluginExecutor]] instance
    */
  def withZKStateControllerPath(zkPath: String): this.type = {
    appProperties.setProperty(ZKRootPathConfig, zkPath)
    this
  }

  /**
    * Set the TopicFetcher to a custom implementation
    * @param customTopicFetcher Custom user provided TopicFetcher implementation
    * @return Current [[PluginExecutor]] instance
    */
  def usingTopicFetcher(customTopicFetcher: TopicFetcher): this.type = {
    topicFetcher = Some(customTopicFetcher)
    this
  }

  /**
    * Set the TopicFetcher to LocalTopicFetcher
    * @return Current [[PluginExecutor]] instance
    */
  def usingLocalTopicFetcher: this.type = {
    appProperties.setProperty(TopicFetcherTypeConfig, "LOCAL")
    this
  }

  /**
    * Set the TopicFetcher to HdfsJsonTopicFetcher
    * @param hdfsPath Hdfs location of the topics Json
    * @return Current [[PluginExecutor]] instance
    */
  def usingHdfsJsonTopicFetcher(hdfsPath: String): this.type = {
    appProperties.setProperty(TopicFetcherTypeConfig, "HDFS")
    appProperties.setProperty(HdfsTopicSource, hdfsPath)
    this
  }

  /**
    * Set the TopicFetcher to HttpJsonTopicFetcher
    * @param httpEndpoint Http endpoint of the topics Json
    * @return Current [[PluginExecutor]] instance
    */
  def usingHttpJsonTopicFetcher(httpEndpoint: String): this.type = {
    appProperties.setProperty(TopicFetcherTypeConfig, "HTTP")
    appProperties.setProperty(HttpTopicSource, httpEndpoint)
    this
  }

  /**
    * Set the properties required to instantiate [[KafkaLowLevelDStream]]
    * Note: Explicitly set properties will not get overridden
    *
    * @param userProperties Properties required to instantiate [[KafkaLowLevelDStream]]
    * @return Current [[PluginExecutor]] instance
    */
  def withProperties(userProperties: Properties): this.type = {
    userProperties.keySet().toArray.foreach(k => {
      if (!appProperties.containsKey(k)) {
        appProperties.setProperty(k.toString, userProperties.getProperty(k.toString))
      }
    })
    this
  }

  /**
    * Execute [[ReceiverPlugin]] objects serially
    *
    * @param plugins List of [[ReceiverPlugin]] objects
    */
  def execute(plugins: Seq[ReceiverPlugin]): Unit = {

    // Create app specific configs
    if (stateController.isEmpty) {
      stateController = Some(StateControllerBuilder(appProperties))
    }

    val receiverConfigs = ReceiverConfigBuilder(appProperties, stateController, topicFetcher)

    val numReceivers = appProperties.getProperty(
      AppConfigs.SparkReceivers._1,
      AppConfigs.SparkReceivers._2).toInt

    val storageLevel = AppConfigs.storageLevel(
      appProperties.getProperty(
        AppConfigs.SparkStorageLevel._1,
        AppConfigs.SparkStorageLevel._2))

    // Start receivers and execute plugins for each RDD received
    val receivers = for (i <- 0 until numReceivers) yield {
      val receiverId = i
      new KafkaLowLevelDStream(
        streamingContext,
        receiverConfigs,
        receiverId,
        numReceivers,
        storageLevel
      )
    }

    val unionOfStreams = streamingContext.union(receivers)

    unionOfStreams.cache().foreachRDD(rdd => {
      val timestamp = System.currentTimeMillis()
      logger.debug(s"Will attempt plugin execution for time window starting at:  $timestamp")
      if (!rdd.isEmpty()) {
        // Execute each receiver plugin
        plugins.foreach(p => p.execute(rdd))
        logger.debug(s"Finished executing all plugins for time window starting at: $timestamp ")

        // Update state only if consumption type is blocking
        if (receiverConfigs.isBlocking) {
          val keyAndState = stateController.get.setState(rdd)
          keyAndState.foreach(ks => logger.debug(s"State updated. [path: ${ks._1}, state: ${ks._2}]"))
        }
      }
    })
  }
}
