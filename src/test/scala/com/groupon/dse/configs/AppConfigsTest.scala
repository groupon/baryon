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

package com.groupon.dse.configs

import java.util.Properties

import com.groupon.dse.configs.AppConfigs.{InvalidConfigException, MissingConfigException}
import org.scalatest.FlatSpec

class AppConfigsTest extends FlatSpec {

  "AppConfigs" should "have proper number of fields" in {
    val nonSyntheticFields = AppConfigs.getClass.getDeclaredFields.filter(f => !f.isSynthetic)
    assert(nonSyntheticFields.size == 29) // Always one more than the total attributes
  }

  "AppConfigs" should "have proper default values" in {
    assert(AppConfigs.SparkReceivers._2 == "1")
    assert(AppConfigs.SparkStorageLevel._2 == "MEMORY_AND_DISK_SER_2")
    assert(AppConfigs.Topics._2 == "")
    assert(AppConfigs.TopicsBlackList._2 == "")
    assert(AppConfigs.TopicsEnableBlockingConsumption._2 == "false")
    assert(AppConfigs.TopicConsumptionPolicy._2 == "OFFSET")
    assert(AppConfigs.TopicConsumptionOffsetThreshold._2 == "0")
    assert(AppConfigs.TopicConsumptionTimeThresholdMs._2 == "1000")
    assert(AppConfigs.TopicFetchSizeBytes._2 == "1048576")
    assert(AppConfigs.TopicRepartitionFactor._2 == "1")
    assert(AppConfigs.TopicStartOffset._2 == "-1")
    assert(AppConfigs.PartitionRefreshIntervalMs._2 == "30000")
    assert(AppConfigs.PartitionWarmUpRefreshIntervalMs._2 == "10000")
    assert(AppConfigs.ReceiverRestIntervalOnFailMs._2 == "2500")
    assert(AppConfigs.ReceiverRestIntervalOnSuccessMs._2 == "100")
    assert(AppConfigs.KafkaBrokerConnect._2 == "")
    assert(AppConfigs.KafkaSocketTimeoutMs._2 == "10000")
    assert(AppConfigs.KafkaSocketBufferSizeBytes._2 == "1048576")
    assert(AppConfigs.KafkaZkSessionTimeoutMs._2 == "10000")
    assert(AppConfigs.KafkaZkConnectionTimeoutMs._2 == "10000")
    assert(AppConfigs.StateControllerType._2 == "MEMORY")
    assert(AppConfigs.ZookeeperStateControllerConnect._2 == "")
    assert(AppConfigs.ZookeeperStateControllerRoot._2 == "/baryon")
    assert(AppConfigs.ZookeeperStateControllerConnTimeoutMs._2 == "120000")
    assert(AppConfigs.ZookeeperStateControllerSessionTimeoutMs._2 == "60000")
    assert(AppConfigs.TopicFetcherType._2 == "LOCAL")
    assert(AppConfigs.HDFSTopicSource._2 == "")
    assert(AppConfigs.HTTPTopicSource._2 == "")
  }

  "validate" should "not throw exception if all configs present" in {
    val properties = new Properties()
    properties.setProperty("key1", "value1")
    properties.setProperty("key2", "value2")
    properties.setProperty("key3", "value3")

    val requiredProps = Seq("key1", "key2")

    AppConfigs.validate(properties, requiredProps)
  }

  it should "throw MissingConfigException if required config missing" in
    intercept[MissingConfigException] {
      val properties = new Properties()
      properties.setProperty("key1", "value1")
      properties.setProperty("key2", "value2")
      properties.setProperty("key3", "value3")

      val requiredProps = Seq("key4")

      AppConfigs.validate(properties, requiredProps)
    }

  "validatedBooleanConfig" should "return true for a boolean config with value true" in {
    val properties = new Properties()
    properties.setProperty("key1", "true")
    properties.setProperty("key2", "false")

    assert(AppConfigs.validatedBooleanConfig(properties, "key1", "true") == true)
  }

  "validatedBooleanConfig" should "return false for a boolean config with value false" in {
    val properties = new Properties()
    properties.setProperty("key1", "true")
    properties.setProperty("key2", "false")

    assert(AppConfigs.validatedBooleanConfig(properties, "key2", "false") == false)
  }

  it should "throw InvalidConfigException if boolean config has value other than true/false" in
    intercept[InvalidConfigException] {
      val properties = new Properties()
      properties.setProperty("key1", "nonboolean")
      properties.setProperty("key2", "false")

      AppConfigs.validatedBooleanConfig(properties, "key1", "true")
    }
}
