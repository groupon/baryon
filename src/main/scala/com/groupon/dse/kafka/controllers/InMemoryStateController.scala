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

package com.groupon.dse.kafka.controllers

import com.groupon.dse.kafka.common.State
import com.groupon.dse.kafka.partition.Partition

class InMemoryStateController extends StateController {

  private lazy val stateMap = scala.collection.mutable.HashMap.empty[String, State]

  /**
    * Generate the key based on the provided seed
    *
    * @return Key to store state information
    */
  override def generateStateKey(seed: Partition): String = seed.partitionIdentifier

  /**
    * Always return true since there is no outbound connection to outside systems
    *
    * @return true
    */
  override def isConnected: Boolean = true

  /**
    * Given a key, obtain the state information
    *
    * @param key Key to fetch state
    * @return State information
    */
  @throws[KeyDoesNotExistException]
  override def getState(key: String): State = {
    if (!isKeyPresent(key))
      throw new KeyDoesNotExistException(key + " does not exist")
    stateMap(key)
  }

  /**
    * Check if the state key is already present
    *
    * @param key Key to store state
    * @return Boolean to indicate whether the key is already present or not
    */
  override def isKeyPresent(key: String): Boolean = stateMap.contains(key)

  /**
    * Given the key, store appropriate state in HashMap
    *
    * @param key Key to store state
    * @param value State information
    * @return Tuple containing the key and updated [[State]]
    */
  override def setState(key: String, value: State): (String, State) = {
    stateMap += (key -> value)
    (key, value)
  }

  /**
    * Remove all the elements from HashMap
    */
  override def close(): Unit = stateMap.clear()
}
