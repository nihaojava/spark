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

package org.apache.spark

import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.util._

private[spark] class TaskContextImpl(
    val stageId: Int, // task所属stage的ID
    val partitionId: Int, // 分区ID
    override val taskAttemptId: Long, // 任务尝试ID
    override val attemptNumber: Int, // 任务尝试号
    override val taskMemoryManager: TaskMemoryManager, // task内存管理器
    localProperties: Properties,
    @transient private val metricsSystem: MetricsSystem, // 度量系统
    // The default value is only used in tests.
    override val taskMetrics: TaskMetrics = TaskMetrics.empty) //
  extends TaskContext
  with Logging {

  /** List of callback functions to execute when the task completes. */
  // 保存任务执行完成后需要回调的TaskCompletionListener的数组。
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  /** List of callback functions to execute when the task fails. */
  // 保存任务执行失败后需要回调的TaskFailureListener的数组。
  @transient private val onFailureCallbacks = new ArrayBuffer[TaskFailureListener]

  // Whether the corresponding task has been killed.
  // 对应的task是否已经被kill。
  @volatile private var interrupted: Boolean = false

  // Whether the task has completed.
  // 对应的task是否已经完成。
  @volatile private var completed: Boolean = false

  // Whether the task has failed.
  //  对应的task是否已经失败。
  @volatile private var failed: Boolean = false

  // 用于向onCompleteCallbacks中添加TaskCompletionListener
  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    onCompleteCallbacks += listener
    this
  }

  // 用于向onFailureCallbacks中添加TaskFailureListener
  override def addTaskFailureListener(listener: TaskFailureListener): this.type = {
    onFailureCallbacks += listener
    this
  }

  /** Marks the task as failed and triggers the failure listeners. */
  // 标记任务失败 并且 触发失败的监听器
  private[spark] def markTaskFailed(error: Throwable): Unit = {
    // failure callbacks should only be called once
    // 失败的回调应该只回调一次
    if (failed) return  // 判断task是否已经标记为失败。如果是，则返回，否则进入一下步。
    failed = true // 将task标记为失败
    val errorMsgs = new ArrayBuffer[String](2)
    // Process failure callbacks in the reverse order of registration
    // 处理失败回调 按照注册的顺序反序
    // 对onFailureCallbacks进行反向排序后，对onFailureCallbacks中的每一个TaskFailureListener，
    // 调用其onTaskFailure方法。如果调用onTaskFailure方法的过程中发生了异常，这些异常将被收集到errorMsgs中。
    // 如果errorMsgs不为空，则对外抛出携带errorMsgs的TaskCompletionListenerException。
    onFailureCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskFailure(this, error)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskFailureListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs, Option(error))
    }
  }

  /** Marks the task as completed and triggers the completion listeners. */
  // 标记task执行完成 并且 触发完成的监听器
  private[spark] def markTaskCompleted(): Unit = {
    completed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process complete callbacks in the reverse order of registration
    onCompleteCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskCompletion(this)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskCompletionListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs)
    }
  }

  /** Marks the task for interruption, i.e. cancellation. */
  // 标记task已经被kill，例如 取消执行
  private[spark] def markInterrupted(): Unit = {
    interrupted = true
  }

  override def isCompleted(): Boolean = completed

  override def isRunningLocally(): Boolean = false

  override def isInterrupted(): Boolean = interrupted

  // 获取指定key对应的本地属性值
  override def getLocalProperty(key: String): String = localProperties.getProperty(key)

  // 通过source名称从metricsSystem中获取Source序列
  override def getMetricsSources(sourceName: String): Seq[Source] =
    metricsSystem.getSourcesByName(sourceName)

  // 向TaskMetrics注册累加器
  private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    taskMetrics.registerAccumulator(a)
  }

}
