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

package com.groupon.dse.util

import org.scalatest.FlatSpec

class UtilTest extends FlatSpec {

  "doActionWithRetry" should "execute action successfully if no errors" in {
    val action = (i: Int) => {
      i * 2
    }
    val failureHandler = (t: Throwable) => {
      // do nothing
    }
    val maxRetries = 1

    val ret = Utils.doActionWithRetry[Int](
      {
        action(2)
      }, {
        failureHandler
      },
      maxRetries)
    assert(ret == 4)
  }

  "doActionWithRetry" should "get an exception if error exists after retries" in
    intercept[RuntimeException] {
      val action = (i: Int) => {
        throw new RuntimeException
      }
      val failureHandler = (t: Throwable) => {
        // do nothing
      }
      val maxRetries = 1

      Utils.doActionWithRetry[Int](
        {
          action(2)
        }, {
          failureHandler
        },
        maxRetries)
    }

  "doActionWithRetry" should "eventually succeed after 1 retry" in {
    val action = (time: Long) => {
      val entryTime = System.currentTimeMillis()
      if (entryTime - time < 1000) {
        throw new RuntimeException
      }
    }

    val failureHandler = (t: Throwable) => {
      Thread.sleep(1000)
    }

    val maxRetries = 1

    val inputTime = System.currentTimeMillis()
    Utils.doActionWithRetry[Unit](
      {
        action(inputTime)
      }, {
        failureHandler
      },
      maxRetries)
  }

  "doActionWithRetry" should "eventually succeed after 2 retry" in {
    val action = (time: Long) => {
      val entryTime = System.currentTimeMillis()
      if (entryTime - time < 1000) {
        throw new RuntimeException
      }
    }

    val failureHandler = (t: Throwable) => {
      Thread.sleep(500)
    }

    val maxRetries = 2

    val inputTime = System.currentTimeMillis()
    Utils.doActionWithRetry[Unit](
      {
        action(inputTime)
      }, {
        failureHandler
      },
      maxRetries)
  }
}
