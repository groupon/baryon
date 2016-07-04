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

/**
  * Topic fetcher that retrieves the static topic list from the Application configs provided in the driver.
  * The blacklist regex provided in the configs is applied to the topics list to filter the unwanted topics.
  * Does not support topic metadata
  * @param topics The list of topics provided in the config file
  * @param topicBlackListRegex The topic blacklist regex
  */
class LocalTopicFetcher(
                         topics: Seq[String],
                         topicBlackListRegex: Seq[String])
  extends TopicFetcher with Serializable {

  val blacklistTopics = scala.collection.mutable.ListBuffer.empty[String]
  topicBlackListRegex.foreach(tbr => blacklistTopics.appendAll(topics.filter(_.matches(tbr))))

  val validTopics = topics.filterNot(t => blacklistTopics.contains(t))

  val topicsList = validTopics.map(topic => TopicAndMetadata(topic))

  /**
    * Fetch the topics from the local properties object
    * @return The list of [[TopicAndMetadata]] from the config file
    */
  override def fetchTopics: Seq[TopicAndMetadata] = topicsList
}