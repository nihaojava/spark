/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import org.apache.spark.TaskState
import org.apache.spark.TaskState.TaskState
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Information about a running task attempt inside a TaskSet.
 * 关于任务集中正在运行的任务尝试的信息。
 */
@DeveloperApi
class TaskInfo(
    val taskId: Long,
    /**
     * The index of this task within its task set. Not necessarily the same as the ID of the RDD
     * partition that the task is computing.
     */
    val index: Int,
    val attemptNumber: Int,
    val launchTime: Long, /*启动时间戳ms*/
    val executorId: String,
    val host: String,
    val taskLocality: TaskLocality.TaskLocality,
    val speculative: Boolean) {

  /**
   * The time when the task started remotely getting the result. Will not be set if the
   * task result was sent immediately when the task finished (as opposed to sending an
   * IndirectTaskResult and later fetching the result from the block manager).
   */
  /*记录获取到该Task结果的时间（如果是执行完直接发送过来的DirectTaskResult将不会设置）*/
  var gettingResultTime: Long = 0

  /**
   * Intermediate updates to accumulables during this task. Note that it is valid for the same
   * accumulable to be updated multiple times in a single task or for two accumulables with the
   * same name but different IDs to exist in a task.
   */
  def accumulables: Seq[AccumulableInfo] = _accumulables

  /*累加器相关*/
  private[this] var _accumulables: Seq[AccumulableInfo] = Nil

  private[spark] def setAccumulables(newAccumulables: Seq[AccumulableInfo]): Unit = {
    _accumulables = newAccumulables
  }

  /**
   * The time when the task has completed successfully (including the time to remotely fetch
   * results, if necessary).
   */
  /*完成时间戳ms*/
  var finishTime: Long = 0

  var failed = false

  var killed = false
  /*设置获取到结果的时间为当前时间*/
  private[spark] def markGettingResult(time: Long = System.currentTimeMillis) {
    gettingResultTime = time
  }

  private[spark] def markFinished(state: TaskState, time: Long = System.currentTimeMillis) {
    finishTime = time
    if (state == TaskState.FAILED) {
      failed = true
    } else if (state == TaskState.KILLED) {
      killed = true
    }
  }

  def gettingResult: Boolean = gettingResultTime != 0

  def finished: Boolean = finishTime != 0

  def successful: Boolean = finished && !failed && !killed

  def running: Boolean = !finished

  def status: String = {
    if (running) {
      if (gettingResult) {
        "GET RESULT"
      } else {
        "RUNNING"
      }
    } else if (failed) {
      "FAILED"
    } else if (killed) {
      "KILLED"
    } else if (successful) {
      "SUCCESS"
    } else {
      "UNKNOWN"
    }
  }

  def id: String = s"$index.$attemptNumber"

  /*task执行时长，单位为ms*/
  def duration: Long = {
    if (!finished) {
      /*此任务还没完成，抛出异常*/
      throw new UnsupportedOperationException("duration() called on unfinished task")
    } else {
      /*完成时间点 - 启动时间点，单位为ms */
      finishTime - launchTime
    }
  }

  private[spark] def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
