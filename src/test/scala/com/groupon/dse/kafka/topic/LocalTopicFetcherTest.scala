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

import org.scalatest.FlatSpec

class LocalTopicFetcherTest extends FlatSpec {
  "The config file topic fetcher" should "filter blacklisted topics regex" in {
    val topics = Seq("test_json", "test1", "test_random", "some_other_json")
    val topicBlacklistRegex = Seq(".*json.*")
    val topicFetcher = new LocalTopicFetcher(topics, topicBlacklistRegex)
    assert(topicFetcher.fetchTopics == Seq(TopicAndMetadata("test1"), TopicAndMetadata("test_random")))
  }

  "The topic fetcher" should "return all topics when no blacklist regex is provided" in {
    val topics = Seq("test_json", "test1", "test_random", "some_other_json")
    val topicBlacklistRegex = Seq("")
    val topicFetcher = new LocalTopicFetcher(topics, topicBlacklistRegex)
    assert(topicFetcher.fetchTopics == Seq(TopicAndMetadata("test_json"), TopicAndMetadata("test1"), TopicAndMetadata("test_random"), TopicAndMetadata("some_other_json")))
  }

  "The topic fetcher" should "filter the blacklisted topics list" in {
    val topics = Seq("test_json", "test1", "test_random", "some_other_json")
    val topicBlacklistRegex = Seq("some_other_json", ".*json.*")
    val topicFetcher = new LocalTopicFetcher(topics, topicBlacklistRegex)
    assert(topicFetcher.fetchTopics == Seq(TopicAndMetadata("test1"), TopicAndMetadata("test_random")))
  }
}
