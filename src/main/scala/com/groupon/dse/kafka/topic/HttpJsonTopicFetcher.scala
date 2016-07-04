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

import org.slf4j.LoggerFactory
import play.api.libs.ws.ning.NingWSClient

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * This [[TopicFetcher]] implementation reads the contents of a JSON file obtained from a HTTP endpoint.
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
  * @param topicsUrlEndpoint THe HTTP endpoint to fetch the JSON from
  */
class HttpJsonTopicFetcher(topicsUrlEndpoint: String)
  extends TopicFetcher with Serializable {
  lazy val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  lazy val client = new NingWSClient(builder.build())
  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Fetch the [[TopicAndMetadata]] list from the specified URL
    * @return the list of [[TopicAndMetadata]]
    */
  override def fetchTopics: Seq[TopicAndMetadata] = {
    try {
      TopicUtils.extractTopicListFromJson(getContentFromUrl.getOrElse(""))
    } catch {
      case e: Exception => {
        logger.error("Exception occurred while fetching topics from the provided url : ", e)
        Seq.empty
      }
    }
  }

  /**
    * Fetches the content from the provided URL asynchronously. Blocks until the response is available for further processing.
    * The url response is a [[scala.concurrent.Future]] object which implies that the results would be available at some point in the future.
    * The max wait time for the response is currently set to 2 seconds but it would return earlier if the response is available.
    * @return The String representation of the JSON fetched from the URL.
    */
  def getContentFromUrl: Option[String] = {
    val response = client.url(topicsUrlEndpoint).get().map { resp =>
      resp.json.toString
    }
    val jsonString = Await.ready(response, 2.seconds).value.get //TODO: Change this duration. Should it be user provided?

    val finalResponse = jsonString match {
      case Success(str) => Some(str)
      case Failure(exc) => {
        logger.error("Error fetching json from the provided URL : ", exc)
        None
      }
    }
    finalResponse
  }
}
