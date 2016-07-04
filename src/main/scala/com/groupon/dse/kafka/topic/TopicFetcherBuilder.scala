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

package com.groupon.dse.kafka.topic

import java.util.Properties

import com.groupon.dse.configs.AppConfigs
import org.slf4j.LoggerFactory

/**
  * Helper to build a [[TopicFetcher]] object based on the provided configs
  */
object TopicFetcherBuilder {

  private val logger = LoggerFactory.getLogger(getClass)

  def apply(properties: Properties): TopicFetcher = {

    val fetcherType = properties.getProperty(
      AppConfigs.TopicFetcherType._1,
      AppConfigs.TopicFetcherType._2)

    val hdfsTopicSource = properties.getProperty(
      AppConfigs.HDFSTopicSource._1,
      AppConfigs.HDFSTopicSource._2)

    val httpTopicSource = properties.getProperty(
      AppConfigs.HTTPTopicSource._1,
      AppConfigs.HTTPTopicSource._2)

    val topicBlacklist = properties.getProperty(
      AppConfigs.TopicsBlackList._1,
      AppConfigs.TopicsBlackList._2).split(",").map(_.trim)

    fetcherType match {
      case "LOCAL" => {
        AppConfigs.validate(properties, Seq(AppConfigs.Topics._1))
        val topics = properties.getProperty(AppConfigs.Topics._1).split(",").map(_.trim)
        new LocalTopicFetcher(topics, topicBlacklist)
      }

      case "HDFS" => {
        AppConfigs.validate(properties, Seq(AppConfigs.HDFSTopicSource._1))
        new HdfsJsonTopicFetcher(hdfsTopicSource.trim)
      }

      case "HTTP" => {
        AppConfigs.validate(properties, Seq(AppConfigs.HTTPTopicSource._1))
        new HttpJsonTopicFetcher(httpTopicSource)
      }
      case _ => throw InvalidTopicFetcherException("Valid topic fetchers are LOCAL, HDFS, HTTP")
    }
  }

  case class InvalidTopicFetcherException(message: String) extends Exception(message)

}
