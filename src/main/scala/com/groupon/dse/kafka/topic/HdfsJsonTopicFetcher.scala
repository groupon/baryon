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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * This [[TopicFetcher]] implementation reads the contents of a JSON file stored in HDFS.
This specific implementation requires the JSON file to adhere to a specific schema as indicated below:
    {
      "topics": [
        {
          "name": "topic1",
          "metadata": {
            "type": "avro"
          }
        },
        {
          "name": "topic2"
        }
      ]
    }
The JSON file should contain a REQUIRED field called "topics" that contains an array of topic entries. Each entry should
again contain a REQUIRED "name" field indicating the name of the desired topic. The "metadata" field is optional
and is not used in the current implementation. However users can create their own [[TopicFetcher]] implementations to
use the "metadata" value to determine valid topics to fetch from Kafka.
  * @param topicsLocation The location of the topic file in HDFS.
  */
class HdfsJsonTopicFetcher(topicsLocation: String)
  extends TopicFetcher with Serializable {

  lazy val path = new Path(topicsLocation)
  lazy val config = new Configuration()
  lazy val fileSystem = FileSystem.get(config)
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Fetch the topics from the JSON file in the given HDFS location.
    * @return The list of [[TopicAndMetadata]] from HDFS
    */
  override def fetchTopics: Seq[TopicAndMetadata] = {
    try {
      val contents = readTopicFile
      val topicList = TopicUtils.extractTopicListFromJson(contents)
      topicList.toSeq
    } catch {
      case e: Exception => {
        logger.error("Exception occurred while fetching topics from HDFS : ", e)
        Seq.empty
      }
    }
  }

  /**
    * Helper method to read the topic file from HDFS and return the contents as a String
    * @return The contents of the HDFS file
    */
  def readTopicFile: String = {
    val inputStream = fileSystem.open(path)
    try {
      val data = Source.fromInputStream(inputStream).getLines().mkString("\n")
      data
    } finally {
      inputStream.close()
    }
  }

}
