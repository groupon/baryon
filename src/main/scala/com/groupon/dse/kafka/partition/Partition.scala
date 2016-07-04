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

package com.groupon.dse.kafka.partition

import com.groupon.dse.kafka.common.KafkaException.LeaderNotAvailableException
import com.groupon.dse.kafka.common.{KafkaException, WrappedMessage}
import kafka.consumer.SimpleConsumer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkException

/**
  * Representation of a single Kafka partition
  *
  * @param topic Name of the topic in the partition
  * @param partitionNum Partition identifier
  */
abstract class Partition(val topic: String, val partitionNum: Int)
  extends Serializable {

  /**
    * Obtain the min and max offset for a [[Partition]]
    *
    * @param consumer Kafka consumer client
    * @return [[Offsets]] object with min and max offsets
    */
  @throws[KafkaException]
  def offsets(consumer: SimpleConsumer): Offsets

  /**
    * Provide the N latest valid offset values
    *
    * @param count Maximum valid offset values to be fetched
    * @param consumer Kafka consumer client
    * @return Sequence of the N latest valid offsets
    */
  @throws[KafkaException]
  def offsetRange(count: Int, consumer: SimpleConsumer): Seq[Long]

  /**
    * Fetch messages for the particular [[Partition]]
    *
    * @param startOffset Offset to start fetching from
    * @param fetchSize Data to fetch in a single api call
    * @param key Partition key associated with the message set
    * @param consumer Kafka consumer client
    * @return Iterable list of [[WrappedMessage]]
    */
  @throws[KafkaException]
  def fetchMessages(startOffset: Long,
                    fetchSize: Int,
                    key: String,
                    consumer: SimpleConsumer)
  : Iterable[WrappedMessage]

  /**
    * Obtain the [[Leader]] for the current partition
    *
    * @param zkClient Client to interact with the Kafka Zookeeper
    * @return [[Leader]] for the current partition
    */
  @throws[LeaderNotAvailableException]
  @throws[ZkException]
  def leader(zkClient: ZkClient): Leader

  /**
    * Unique [[Partition]] identifier
    *
    * @return [[Partition]] identifier
    */
  def partitionIdentifier: String

  /**
    * String representation of [[Partition]] class
    *
    * @return String representing the [[Partition]] class
    */
  override def toString: String = s"[$topic:$partitionNum]"

  /**
    * Check equality of two objects
    *
    * @param that Object whose equality with current [[Partition]] object needs to be checked
    * @return Result of object comparison
    */
  override def equals(that: Any): Boolean = that match {
    case that: Partition => that.canEqual(this) && that.hashCode == this.hashCode
    case _ => false
  }

  /**
    * HashCode for the [[Partition]] object
    *
    * @return HashCode value
    */
  override def hashCode: Int = partitionIdentifier.hashCode

  /**
    * Check if given object is of type [[Partition]]
    *
    * @param o Other object
    * @return  Result of instance type check
    */
  def canEqual(o: Any): Boolean = o.isInstanceOf[Partition]
}

/**
  * Wrapper class for storing the min and max offset for a [[Partition]]
  *
  * @param min Min available offset
  * @param max Max available offset
  */
case class Offsets(
                    min: Long,
                    max: Long)
  extends Serializable {

  override def toString: String = s"[$min, $max]"
}

/**
  * Broker currently serving the [[Partition]]
  *
  * @param host
  * @param port
  */
case class Leader(
                   host: String,
                   port: Int)
  extends Serializable {

  override def toString: String = s"[$host:$port]"
}