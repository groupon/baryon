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

package com.groupon.dse.kafka.cluster

import com.groupon.dse.configs.KafkaServerConfig
import com.groupon.dse.kafka.partition.Partition
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient

/**
  * Class to interact with Kafka cluster
  *
  * @param serverConfigs [[KafkaServerConfig]] instance with server related configs
  */
abstract class Cluster(val serverConfigs: KafkaServerConfig)
  extends Serializable {

  /**
    * Obtain list of all topics in the Kafka cluster
    * @param zkClient Client to interact with the Kafka Zookeeper
    * @return Seq of Kafka topics
    */
  def topics(zkClient: ZkClient): Seq[String]

  /**
    * Obtain seq of [[Partition]] objects for the provided set of topics
    *
    * @param topics Seq of topics for which [[Partition]] object should be created
    * @param zkClient Client to interact with the Kafka Zookeeper
    * @return Seq of [[Partition]] objects
    */
  def partitions(topics: Seq[String], zkClient: ZkClient): Seq[Partition]

  /**
    * Zookeeper path which stores the list of id's for connected brokers
    *
    * @return Zookeeper path string
    */
  def brokerIdPath: String = ZkUtils.BrokerIdsPath

  /**
    * Zookeeper path which stores the list of topics stored in the cluster
    *
    * @return Zookeeper path string
    */
  def brokerTopicsPath: String = ZkUtils.BrokerTopicsPath
}
