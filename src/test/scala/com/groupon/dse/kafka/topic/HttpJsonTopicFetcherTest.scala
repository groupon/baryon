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

import org.mockito.Mockito
import org.scalatest.{BeforeAndAfter, FlatSpec}

class HttpJsonTopicFetcherTest extends FlatSpec with BeforeAndAfter {

  "The topic fetcher" should "return the correct list of topics " in {
    val http = Mockito.spy(new HttpJsonTopicFetcher("http://test-url"))
    val content = "{\"topics\":[{\"name\":\"topic1\",\"metadata\":{\"m1\":\"v1\",\"m2\":true}},{\"name\":\"topic2\",\"metadata\":{\"m3\":\"v3\"}}],\"extras\":\"test\"}"
    Mockito.doReturn(Some(content)).when(http).getContentFromUrl
    assert(http.fetchTopics == Seq(TopicAndMetadata("topic1", Map("m1" -> "v1", "m2" -> true.asInstanceOf[AnyRef])), TopicAndMetadata("topic2", Map("m3" -> "v3"))))
  }

  "The topic fetcher" should "return an empty list if the Json in incorrect " in {
    val http = Mockito.spy(new HttpJsonTopicFetcher("http://test-url"))
    val content = "{\"topics\":[{\"name\":\"topic1\",\"metadata\":{\"m1\":\"v1\",\"m2\":true}}{\"name\":\"topic2\",\"metadata\":{\"m3\":\"v3\"}}],\"extras\":\"test\"}"
    Mockito.doReturn(Some(content)).when(http).getContentFromUrl
    assert(http.fetchTopics == Seq())
  }

  "The topic fetcher " should "return the filtered list if the name field is missing for any topic" in {
    val http = Mockito.spy(new HttpJsonTopicFetcher("http://test-url"))
    val contents = "{\"topics\":[{\"name\":\"topic1\",\"metadata\":{\"m1\":\"v1\",\"m2\":\"v2\"}},{\"name1\":\"topic2\",\"enabled\":\"false\",\"metadata\":{\"m3\":\"v3\",\"m4\":\"v4\"}},{\"name\":\"topic3\",\"enabled\":\"true\",\"metadata\":{\"m3\":\"v3\",\"m4\":\"v4\"}}],\"extras\":\"test\"}"
    Mockito.doReturn(Some(contents)).when(http).getContentFromUrl
    assert(http.fetchTopics == Seq(TopicAndMetadata("topic1", Map("m1" -> "v1", "m2" -> "v2")), TopicAndMetadata("topic3", Map("m3" -> "v3", "m4" -> "v4"))))
  }

  "The topic fetcher" should "return an empty map if no metadata is provided" in {
    val http = Mockito.spy(new HttpJsonTopicFetcher("http://test-url"))
    val contents = "{\"topics\":[{\"name\":\"topic1\"},{\"name\":\"topic2\",\"metadata\":{\"m3\":\"v3\",\"m4\":{\"m5\":\"v5\"},\"bool\":true}}]}"
    Mockito.doReturn(Some(contents)).when(http).getContentFromUrl
    assert(http.fetchTopics == Seq(TopicAndMetadata("topic1", Map()), TopicAndMetadata("topic2", Map("m3" -> "v3", "m4" -> Map("m5" -> "v5").asInstanceOf[AnyRef], "bool" -> true.asInstanceOf[AnyRef]))))
  }

  "The topic fetcher" should "return an empty list of topics if there is any problem when fetching the Json" in {
    val http = Mockito.spy(new HttpJsonTopicFetcher("http://test-url"))
    Mockito.doReturn(None).when(http).getContentFromUrl
    assert(http.fetchTopics == Seq.empty)
  }

}