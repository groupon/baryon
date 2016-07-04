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

package com.groupon.dse.kafka.common

/**
  * Generic Kafka exception class
  *
  * @param message Error description
  */
abstract class KafkaException(message: String) extends Exception(message)

/**
  * Singleton to resolve Kafka error codes
  */
object KafkaException {

  // Error codes
  val Unknown = -1
  val OffsetOutOfRange = 1
  val InvalidMessage = 2
  val UnknownTopicOrPartition = 3
  val InvalidMessageSize = 4
  val LeaderNotAvailable = 5
  val OffsetMetadataTooLarge = 12
  val OffsetLoadInProgress = 14

  /**
    * Raise appropriate exception based on error code returned by Kafka server
    *
    * @param code Kafka server error code
    * @return Instance of [[KafkaException]]
    */
  def apply(code: Int): KafkaException = code match {
    case Unknown => new UnknownException("Unexpected error")
    case OffsetOutOfRange => new OffsetOutOfRangeException("Offset out of range")
    case InvalidMessage => new InvalidMessageException("Invalid message")
    case UnknownTopicOrPartition => new UnknownTopicOrPartitionException("Unknown topic or partition")
    case InvalidMessageSize => new InvalidMessageSizeException("Invalid message size")
    case LeaderNotAvailable => new LeaderNotAvailableException("Leader not available")
    case OffsetMetadataTooLarge => new OffsetMetadataTooLargeException("Offset metadata too large")
    case OffsetLoadInProgress => new OffsetsLoadInProgressException("Offset load in progress")
  }

  /**
    * An unexpected server error
    *
    * @param message Error description
    */
  case class UnknownException(message: String) extends KafkaException(message)

  /**
    * The requested offset is outside the range of offsets maintained by the server for the given topic/partition
    *
    * @param message Error description
    */
  case class OffsetOutOfRangeException(message: String) extends KafkaException(message)

  /**
    * Message contents does not match its CRC
    *
    * @param message Error description
    */
  case class InvalidMessageException(message: String) extends KafkaException(message)

  /**
    * This request is for a topic or partition that does not exist on this broker
    *
    * @param message Error description
    */
  case class UnknownTopicOrPartitionException(message: String) extends KafkaException(message)

  /**
    * The message has a negative size
    *
    * @param message Error description
    */
  case class InvalidMessageSizeException(message: String) extends KafkaException(message)

  /**
    * Leader not available
    *
    * @param message Error description
    */
  case class LeaderNotAvailableException(message: String) extends KafkaException(message)

  /**
    * If you specify a string larger than configured maximum for offset metadata
    *
    * @param message Error description
    */
  case class OffsetMetadataTooLargeException(message: String) extends KafkaException(message)

  /**
    * The broker returns this error code for an offset fetch request if it is still loading offsets
    *
    * @param message Error description
    */
  case class OffsetsLoadInProgressException(message: String) extends KafkaException(message)

}
