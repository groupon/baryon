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

package com.groupon.dse.kafka.policy

import java.util.Properties

import com.groupon.dse.configs.AppConfigs
import org.slf4j.LoggerFactory

/**
  * Construct the appropriate [[FetchPolicy]] object based on user params
  */
object FetchPolicyBuilder {

  private val logger = LoggerFactory.getLogger(getClass)

  def apply(properties: Properties): FetchPolicy = {

    val fetchPolicyType = properties.getProperty(
      AppConfigs.TopicConsumptionPolicy._1,
      AppConfigs.TopicConsumptionPolicy._2)

    val offsetThreshold = properties.getProperty(
      AppConfigs.TopicConsumptionOffsetThreshold._1,
      AppConfigs.TopicConsumptionOffsetThreshold._2).toLong

    val timeThreshold = properties.getProperty(
      AppConfigs.TopicConsumptionTimeThresholdMs._1,
      AppConfigs.TopicConsumptionTimeThresholdMs._2).toLong

    val fetchSize = properties.getProperty(
      AppConfigs.TopicFetchSizeBytes._1,
      AppConfigs.TopicFetchSizeBytes._2).toInt

    val startOffset = properties.getProperty(
      AppConfigs.TopicStartOffset._1,
      AppConfigs.TopicStartOffset._2).toLong

    val isBlocking = AppConfigs.validatedBooleanConfig(
      properties,
      AppConfigs.TopicsEnableBlockingConsumption._1,
      AppConfigs.TopicsEnableBlockingConsumption._2)

    if (isBlocking) {
      // If user has chosen blocking consumption and has also explicitly set
      // fetch policy related configs, notify user that the later will be ignored
      if (properties.containsKey(AppConfigs.TopicConsumptionPolicy)) {
        logger.warn("Fetch policy configs will be ignored if consumption type is blocking")
      }
      return fetchPolicyForBlockingConsumption(fetchSize, startOffset)
    }

    fetchPolicyByType(
      fetchPolicyType,
      offsetThreshold,
      timeThreshold,
      fetchSize,
      startOffset
    )
  }

  /**
    * FetchPolicy that should be used by default if consumption is blocking
    *
    * @param fetchSize Data to fetch, from Kafka, in a single api call
    * @param startOffset Offset value to start consuming from (-1: Max, -2: Min, Other: Actual offset value)
    * @return [[FetchPolicy]] instance
    */
  def fetchPolicyForBlockingConsumption(fetchSize: Int, startOffset: Long)
  : FetchPolicy = new OffsetBasedFetchPolicy(
    AppConfigs.TopicConsumptionOffsetThreshold._2.toInt,
    fetchSize,
    startOffset)

  /**
    * Build appropriate [[FetchPolicy]] based on requested type
    *
    * @param fetchPolicyType Fetch policy type expressed as String
    * @param offsetThreshold Offset threshold for FetchPolicy
    * @param timeThreshold Time threshold for FetchPolicy
    * @param fetchSize Data to fetch, from Kafka, in a single api call
    * @param startOffset Offset value to start consuming from (-1: Max, -2: Min, Other: Actual offset value)
    * @return [[FetchPolicy]] instance
    */
  def fetchPolicyByType(
                         fetchPolicyType: String,
                         offsetThreshold: Long,
                         timeThreshold: Long,
                         fetchSize: Int,
                         startOffset: Long
                       )
  : FetchPolicy = fetchPolicyType match {
    case "TIME_AND_OFFSET" => new TimeAndOffsetBasedFetchPolicy(
      offsetThreshold,
      timeThreshold,
      fetchSize,
      startOffset)

    case "OFFSET" => new OffsetBasedFetchPolicy(
      offsetThreshold,
      fetchSize,
      startOffset)

    case "TIME" => new TimeBasedFetchPolicy(
      timeThreshold,
      fetchSize,
      startOffset)

    case _ => throw InvalidFetchPolicyException("Valid policies are: TIME_AND_OFFSET, OFFSET, TIME")
  }

  case class InvalidFetchPolicyException(message: String) extends Exception(message)

}
