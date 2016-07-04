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

package com.groupon.dse.spark.listeners

import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorRemoved, SparkListenerStageCompleted, SparkListenerTaskEnd}

/**
  * [[SparkListener]] implementation that provides metrics about failures in Spark
  */
class MetricsListener extends SparkListener {
  private lazy val executorRemovedMeter = UserMetricsSystem.meter("baryon.executorRemoved.rate")
  private lazy val failedStagesMeter = UserMetricsSystem.meter("baryon.failedStages.rate")
  private lazy val failedTasksMeter = UserMetricsSystem.meter("baryon.failedTasks.rate")

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    executorRemovedMeter.mark()
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    if (stageCompleted.stageInfo.failureReason.isDefined) {
      failedStagesMeter.mark()
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (!taskEnd.taskInfo.successful) {
      failedTasksMeter.mark()
    }
  }
}
