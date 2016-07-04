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


import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{Writer => SeqWriter}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class HdfsJsonTopicFetcherTest extends FlatSpec with BeforeAndAfterAll {

  val testRoot: Path = new Path(Files.createTempDirectory("topic-fetcher-test").toString)
  var conf: Configuration = _
  var fs: FileSystem = _
  var path: Path = _

  override def beforeAll(): Unit = {
    conf = new Configuration()
    fs = FileSystem.get(conf)
  }

  override def afterAll(): Unit = {
    fs.delete(testRoot, true)
    fs.close()
  }

  "The hdfs topic fetcher " should "return the list of topics specified in the json" in {
    val output = fs.create(new Path(testRoot, "test-json-file.txt"))
    val contents = "{\"topics\":[{\"name\":\"topic1\",\"metadata\":{\"m1\":\"v1\",\"m2\":\"v2\"}},{\"name\":\"topic2\",\"metadata\":{\"m3\":\"v3\",\"m4\":\"v4\"}},{\"name\":\"topic3\",\"metadata\":{\"m3\":\"v3\",\"m4\":\"v4\"}}]}"
    output.write(contents.getBytes)
    output.close()
    val hdfsTopicFetcher = new HdfsJsonTopicFetcher(testRoot.toString() + "/test-json-file.txt")
    assert(hdfsTopicFetcher.fetchTopics == Seq(TopicAndMetadata("topic1", Map("m1" -> "v1", "m2" -> "v2")), TopicAndMetadata("topic2", Map("m3" -> "v3", "m4" -> "v4")), TopicAndMetadata("topic3", Map("m3" -> "v3", "m4" -> "v4"))))
  }

  "The hdfs topic fetcher " should "return the filtered list if the name key is missing for any topic" in {
    val output = fs.create(new Path(testRoot, "test-json-file.txt"))
    val contents = "{\"topics\":[{\"name\":\"topic1\",\"metadata\":{\"m1\":\"v1\",\"m2\":\"v2\"}},{\"name1\":\"topic2\",\"enabled\":\"false\",\"metadata\":{\"m3\":\"v3\",\"m4\":\"v4\"}},{\"name\":\"topic3\",\"enabled\":\"true\",\"metadata\":{\"m3\":\"v3\",\"m4\":\"v4\"}}],\"extras\":\"test\"}"
    output.write(contents.getBytes)
    output.close()
    val hdfsTopicFetcher = new HdfsJsonTopicFetcher(testRoot.toString() + "/test-json-file.txt")
    assert(hdfsTopicFetcher.fetchTopics == Seq(TopicAndMetadata("topic1", Map("m1" -> "v1", "m2" -> "v2")), TopicAndMetadata("topic3", Map("m3" -> "v3", "m4" -> "v4"))))
  }

  "The hdfs topic fetcher " should "support nested metadata" in {
    val output = fs.create(new Path(testRoot, "test-json-file.txt"))
    val contents = "{\"topics\":[{\"name\":\"topic1\",\"metadata\":{\"m1\":\"v1\",\"m2\":\"v2\"}},{\"name\":\"topic2\",\"metadata\":{\"m3\":\"v3\",\"m4\":{\"m5\":\"v5\"},\"bool\":true}}]}"
    output.write(contents.getBytes)
    output.close()
    val hdfsTopicFetcher = new HdfsJsonTopicFetcher(testRoot.toString() + "/test-json-file.txt")
    assert(hdfsTopicFetcher.fetchTopics == Seq(TopicAndMetadata("topic1", Map("m1" -> "v1", "m2" -> "v2")), TopicAndMetadata("topic2", Map("m3" -> "v3", "m4" -> Map("m5" -> "v5").asInstanceOf[AnyRef], "bool" -> true.asInstanceOf[AnyRef]))))
  }

  "The hdfs topic fetcher " should "return an empty map if not metadata is provided" in {
    val output = fs.create(new Path(testRoot, "test-json-file.txt"))
    val contents = "{\"topics\":[{\"name\":\"topic1\"},{\"name\":\"topic2\",\"metadata\":{\"m3\":\"v3\",\"m4\":{\"m5\":\"v5\"},\"bool\":true}}]}"
    output.write(contents.getBytes)
    output.close()
    val hdfsTopicFetcher = new HdfsJsonTopicFetcher(testRoot.toString() + "/test-json-file.txt")
    assert(hdfsTopicFetcher.fetchTopics == Seq(TopicAndMetadata("topic1", Map()), TopicAndMetadata("topic2", Map("m3" -> "v3", "m4" -> Map("m5" -> "v5").asInstanceOf[AnyRef], "bool" -> true.asInstanceOf[AnyRef]))))
  }

  "The read file method" should "return the contents of the file" in {
    val output = fs.create(new Path(testRoot, "test-json-file.txt"))
    val contents = "test file contents"
    output.write(contents.getBytes)
    output.close()
    val hdfsTopicFetcher = new HdfsJsonTopicFetcher(testRoot.toString() + "/test-json-file.txt")
    assert(hdfsTopicFetcher.readTopicFile == contents)
  }
}

