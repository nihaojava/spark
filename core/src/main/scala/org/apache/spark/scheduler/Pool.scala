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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * A Schedulable entity that represents collection of Pools or TaskSetManagers
 */
private[spark] class Pool(
    val poolName: String, /*pool的名称*/
    val schedulingMode: SchedulingMode, /*调度模式，FAIR、FIFO、NONE*/
    initMinShare: Int,  /*MinShare的初始值*/
    initWeight: Int)  /*weight的初始值*/
  extends Schedulable with Logging {

  /*用于存储Schedulable*/
  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  /*调度池名称 和 Schedulable的对应关系*/
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  var weight = initWeight /*用于公平调度算法的权重*/
  var minShare = initMinShare /*用于公平调度算法的参考值*/
  var runningTasks = 0  /*当前正在运行的任务数量*/
  var priority = 0  /*优先级别*/

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1  /*调度池 或 TaskSetManagers所属的stage*/
  var name = poolName
  var parent: Pool = null /*当前Pool的父Pool*/

  /*任务集合的调度算法，默认为FIFO*/
  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case _ =>
        val msg = "Unsupported scheduling mode: $schedulingMode. Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
  }
  /*添加schedulable*/
  override def addSchedulable(schedulable: Schedulable) {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    /*设置schedulable的父亲为当前pool*/
    schedulable.parent = this
  }
  /*移除schedulable*/
  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }
  /*根据名称查找Schedulable*/
  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }
  /*当某个executor丢失后，调用当前Pool的schedulableQueue中的各个schedulable的executorLost方法。
  * 最终的目的是层层调用，最后执行此Pool下所有TaskSetManager的executorLost方法*/
  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason) {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  /*检查当前Pool中是否有需要推断执行的任务。
  * 实际是通过迭代调用schedulableQueue中各个子schedulable的checkSpeculatableTasks方法，
  * 最终目的执行此Pool下所有TaskSetManager的heckSpeculatableTasks*/
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    shouldRevive
  }

  /*将当前Pool中的所有TaskSetManager按照调度算法进行排序，并返回排序后的TaskSetManager数组*/
  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }
  /*增加当前pool及其父pool中记录的当前正在运行的任务数量*/
  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }
  /*减少当前pool及其父pool中记录的当前正在运行的任务数量*/
  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
