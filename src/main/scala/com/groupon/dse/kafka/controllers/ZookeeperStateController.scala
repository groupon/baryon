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

import com.groupon.dse.configs.AppConfigs
import com.groupon.dse.kafka.common.State
import com.groupon.dse.kafka.partition.Partition
import com.groupon.dse.zookeeper.ZkClientBuilder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkException
import org.slf4j.LoggerFactory

/**
  * Controller for Zookeeper based state storage
  *
  * @param zkEndpoint Zookeeper connection endpoint
  * @param pathPrefix Base path in zookeeper to store state information
  */
class ZookeeperStateController(
                                zkEndpoint: String,
                                pathPrefix: String,
                                connectionTimeout: Int,
                                sessionTimeout: Int)
  extends StateController {

  private val logger = LoggerFactory.getLogger(getClass)
  private var connected: Boolean = true
  @transient private var zkClient: ZkClient = null

  /**
    * Use default values for retryInterval and maxRetries when none provide
    *
    * @return [[ZookeeperStateController]] instance
    */
  def this(zkEndpoint: String, pathPrefix: String) =
    this(
      zkEndpoint,
      pathPrefix,
      AppConfigs.ZookeeperStateControllerConnTimeoutMs._2.toInt,
      AppConfigs.ZookeeperStateControllerSessionTimeoutMs._2.toInt)

  /**
    * String representation of the ZookeeperStateController object
    *
    * @return String representation
    */
  override def toString: String = s"ZookeeperStateController = [zkEndpoint: $zkEndpoint, zkRoot: $pathPrefix]"

  /**
    * Given a key, obtain the state information
    *
    * @param key Key to fetch state
    * @return State information
    */
  @throws[KeyDoesNotExistException]
  @throws[StateControllerConnectionException]
  override def getState(key: String): State = {
    if (!isKeyPresent(key))
      throw new KeyDoesNotExistException(key + " does not exist")
    try {
      State(ZkUtils.readData(getZkClient, key)._1)
    } catch {
      case zk: ZkException => throwControllerException("Zookeeper connection exception while getting state", zk)
    }
  }

  /**
    * Check if the state key is already present
    *
    * @param key Key to store state
    * @return Boolean to indicate whether the key is already present or not
    */
  @throws[StateControllerConnectionException]
  override def isKeyPresent(key: String): Boolean = {
    try {
      ZkUtils.pathExists(getZkClient, key)
    } catch {
      case zk: ZkException => throwControllerException("Zookeeper connection exception while checking for key", zk)
    }
  }

  /**
    * Given the key, store appropriate state in zookeeper
    *
    * @param key Key to store state
    * @param value State information
    * @return Tuple containing the key and updated [[State]]
    */
  @throws[StateControllerConnectionException]
  override def setState(key: String, value: State): (String, State) = {
    try {
      ZkUtils.updatePersistentPath(getZkClient, key, value.toString())
      (key, value)
    } catch {
      case zk: ZkException => throwControllerException("Zookeeper connection exception while setting state", zk)
    }
  }

  /**
    * Throw a new [[StateControllerConnectionException]]
    *
    * @param msg Message to log
    * @param zk ZkException object
    */
  private def throwControllerException(msg: String, zk: ZkException) = {
    logger.error(msg, zk)
    close
    throw StateControllerConnectionException(zk.getMessage)
  }

  /**
    * Close any resources opened for the controller
    */
  override def close = {
    try {
      connected = false
      zkClient.close()
    } catch {
      case e: Exception => logger.error("Failed to close the ZookeeperStateController zkClient", e)
    }
  }

  /**
    * Check if the zkClient valid else create a new one
    *
    * @return ZkClient instance
    */
  private def getZkClient: ZkClient = {
    if (zkClient == null || !connected) {
      logger.debug("Creating new zkClient.")
      zkClient = ZkClientBuilder(zkEndpoint, connectionTimeout, sessionTimeout)
      connected = true
    }
    zkClient
  }

  /**
    * Generate the key based on the provided seed
    *
    * @param seed Seed to generate key
    * @return Key to store state information
    */
  override def generateStateKey(seed: Partition): String = pathPrefix + "/" + seed.partitionIdentifier

  /**
    * Indicate if the controller is still connected to Zookeeper
    *
    * @return Return true if connected
    */
  override def isConnected: Boolean = connected

}