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
import com.groupon.dse.kafka.topic.TopicFetcherBuilder.InvalidTopicFetcherException
import org.scalatest.{BeforeAndAfter, FlatSpec}

class TopicFetcherBuilder$Test extends FlatSpec with BeforeAndAfter {

  var properties: Properties = _
  before {
    properties = new Properties()
  }

  "The topic Fetcher builder" should "return the appropriate TopicFetcher" in {
    properties.setProperty("topics", "sometopic")
    assert(TopicFetcherBuilder(properties).getClass == classOf[LocalTopicFetcher])
    properties.setProperty("topics.fetcher.type", "HDFS")
    properties.setProperty("topics.fetcher.hdfs.source", "/path/in/hdfs")
    assert(TopicFetcherBuilder(properties).getClass == classOf[HdfsJsonTopicFetcher])
    properties.setProperty("topics.fetcher.type", "LOCAL")
    assert(TopicFetcherBuilder(properties).getClass == classOf[LocalTopicFetcher])
  }

  "the topic fetcher builder" should "throw an exception if invalid type is provided" in {
    intercept[InvalidTopicFetcherException] {
      properties.setProperty("topics.fetcher.type", "INVALID")
      TopicFetcherBuilder(properties)
    }
  }

  "the topic fetcher" should "throw an exception if there are any missing configs" in {
    intercept[AppConfigs.MissingConfigException] {
      TopicFetcherBuilder(properties)
    }

    intercept[AppConfigs.MissingConfigException] {
      properties.setProperty("topics.fetcher.type", "HDFS")
      TopicFetcherBuilder(properties)
    }
  }
}
