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

import com.fasterxml.jackson.core.JsonParseException
import org.scalatest.FlatSpec

class TopicUtils$Test extends FlatSpec {

  "The Topic Utils " should "return the list of topics specified in the json" in {
    val contents = "{\"topics\":[{\"name\":\"topic1\",\"metadata\":{\"m1\":\"v1\",\"m2\":\"v2\"}},{\"name\":\"topic2\",\"metadata\":{\"m3\":\"v3\",\"m4\":\"v4\"}},{\"name\":\"topic3\",\"metadata\":{\"m3\":\"v3\",\"m4\":\"v4\"}}]}"
    assert(TopicUtils.extractTopicListFromJson(contents) == Seq(TopicAndMetadata("topic1", Map("m1" -> "v1", "m2" -> "v2")), TopicAndMetadata("topic2", Map("m3" -> "v3", "m4" -> "v4")), TopicAndMetadata("topic3", Map("m3" -> "v3", "m4" -> "v4"))))
  }

  "The Topic Utils " should "return the filtered list if the name key is missing for any topic" in {
    val contents = "{\"topics\":[{\"name\":\"topic1\",\"metadata\":{\"m1\":\"v1\",\"m2\":\"v2\"}},{\"name1\":\"topic2\",\"enabled\":\"false\",\"metadata\":{\"m3\":\"v3\",\"m4\":\"v4\"}},{\"name\":\"topic3\",\"enabled\":\"true\",\"metadata\":{\"m3\":\"v3\",\"m4\":\"v4\"}}],\"extras\":\"test\"}"
    assert(TopicUtils.extractTopicListFromJson(contents) == Seq(TopicAndMetadata("topic1", Map("m1" -> "v1", "m2" -> "v2")), TopicAndMetadata("topic3", Map("m3" -> "v3", "m4" -> "v4"))))
  }

  intercept[JsonParseException] {
    val contents = "invalid_json"
    TopicUtils.extractTopicListFromJson(contents)
  }
}
