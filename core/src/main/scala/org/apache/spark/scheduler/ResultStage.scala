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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * ResultStages apply a function on some partitions of an RDD to compute the result of an action.
 * resultstage在RDD的某些分区上应用一个函数来计算操作的结果。
 * The ResultStage object captures the function to execute, `func`, which will be applied to each
 * partition, and the set of partition IDs, `partitions`. Some stages may not run on all partitions
 * of the RDD, for actions like first() and lookup().
 * ResultStage对象捕获要执行的函数‘func’，它将应用于每个分区，以及分区id集合‘partitions’。
 * 对于first()和lookup()等操作，有些阶段可能不会在RDD的所有分区上运行。
 */
private[spark] class ResultStage(
    id: Int,
    rdd: RDD[_],
    val func: (TaskContext, Iterator[_]) => _,  /*对rdd的分区进行计算的函数*/
    val partitions: Array[Int], /*要计算的分区数组*/
    parents: List[Stage], /*父stage列表*/
    firstJobId: Int,  /*第一个提交当前stage的job*/
    callSite: CallSite)
  extends Stage(id, rdd, partitions.length, parents, firstJobId, callSite) {

  /**
   * The active job for this result stage. Will be empty if the job has already finished
   * (e.g., because the job was cancelled).
   */
  /*result stage处理的activeJob*/
  private[this] var _activeJob: Option[ActiveJob] = None

  def activeJob: Option[ActiveJob] = _activeJob

  def setActiveJob(job: ActiveJob): Unit = {
    _activeJob = Option(job)
  }

  def removeActiveJob(): Unit = {
    _activeJob = None
  }

  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
   *
   * This can only be called when there is an active job.
   */
  /*找出当前job的所有分区中还没有完成的*/
  override def findMissingPartitions(): Seq[Int] = {
    val job = activeJob.get
    /*根据job中的finished数组来判断是否完成*/
    (0 until job.numPartitions).filter(id => !job.finished(id))
  }

  override def toString: String = "ResultStage " + id
}
