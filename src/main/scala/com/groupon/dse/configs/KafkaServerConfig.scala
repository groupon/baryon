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

/**
  * Kafka server specific configs
  */
case class KafkaServerConfig(brokerZk: String,
                             socketTimeout: Int,
                             socketBufferSize: Int,
                             zkConnectionTimeout: Int,
                             zkSessionTimeout: Int
                            )
  extends Serializable {

  override def toString: String = s"KafkaServerConfig = [brokerZk: $brokerZk, socketTimeout: $socketTimeout, " +
    s"socketBufferSize: $socketBufferSize, zkConnTimeOut: $zkConnectionTimeout, " +
    s"zkSessionTimeOut: $zkSessionTimeout]"
}

/**
  * Create an instance of [[KafkaServerConfig]] based on user params
  */
object KafkaServerConfigBuilder {
  val requiredProperties = Array(AppConfigs.KafkaBrokerConnect._1)

  def apply(properties: Properties): KafkaServerConfig = {
    AppConfigs.validate(properties, requiredProperties)

    val brokerConnect = properties.getProperty(AppConfigs.KafkaBrokerConnect._1)

    val socketTimeOut = properties.getProperty(AppConfigs.KafkaSocketTimeoutMs._1,
      AppConfigs.KafkaSocketTimeoutMs._2).toInt

    val socketBufferSize = properties.getProperty(AppConfigs.KafkaSocketBufferSizeBytes._1,
      AppConfigs.KafkaSocketBufferSizeBytes._2).toInt

    val zkConnectionTimeout = properties.getProperty(AppConfigs.KafkaZkConnectionTimeoutMs._1,
      AppConfigs.KafkaZkConnectionTimeoutMs._2).toInt

    val zkSessionTimeout = properties.getProperty(AppConfigs.KafkaZkSessionTimeoutMs._1,
      AppConfigs.KafkaZkSessionTimeoutMs._2).toInt

    KafkaServerConfig(
      brokerConnect,
      socketTimeOut,
      socketBufferSize,
      zkConnectionTimeout,
      zkSessionTimeout)
  }

}
