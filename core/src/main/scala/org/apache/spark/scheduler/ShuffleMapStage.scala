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

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStages are intermediate stages in the execution DAG that produce data for a shuffle.
 * shufflemapstage是执行DAG的中间阶段，它为shuffle生成数据。
 * They occur right before each shuffle operation, and might contain multiple pipelined operations
 * before that (e.g. map and filter). When executed, they save map output files that can later be
 * fetched by reduce tasks. The `shuffleDep` field describes the shuffle each stage is part of,
 * and variables like `outputLocs` and `numAvailableOutputs` track how many map outputs are ready.
 *
 * ShuffleMapStages can also be submitted independently as jobs with DAGScheduler.submitMapStage.
 * shufflemapstage也可以作为作业独立提交，使用DAGScheduler.submitMapStage。
 * For such stages, the ActiveJobs that submitted them are tracked in `mapStageJobs`. Note that
 * there can be multiple ActiveJobs trying to compute the same shuffle map stage.
 * 对于这些阶段，提交它们的ActiveJobs将在“mapStageJobs”中进行跟踪。
 * 注意：可以有多个活动作业尝试计算相同shuffle map stage。
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _]) /**/
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite) {

  /*与当前ShuffleMapStage相关联的ActiveJob列表*/
  private[this] var _mapStageJobs: List[ActiveJob] = Nil

  /*可用的map任务的输出数量，也代表了执行成功的map任务数*/
  private[this] var _numAvailableOutputs: Int = 0

  /**
   * List of [[MapStatus]] for each partition. The index of the array is the map partition id,
   * and each value in the array is the list of possible [[MapStatus]] for a partition
   * (a single task might run multiple times).
   * 每个partition的MapStatus列表。数组索引是map partitino的id，
   * 数组中每个值是一个分区可能的MapStatus列表。（一个单独的task可能运行多次）
   */
  /*各个partition与其对应的MapStatus，【也表示每个maptask和它的执行结果MapStatus】*/
  /*数据结构是Arry[List[MapStatus]],元素为List的数组*/
  private[this] val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)

  override def toString: String = "ShuffleMapStage " + id

  /**
   * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  /*与当前ShuffleMapStage相关联的ActiveJob列表*/
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs

  /** Adds the job to the active job list. */
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs
  }

  /** Removes the job from the active job list. */
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)
  }

  /**
   * Number of partitions that have shuffle outputs.
   * 已经shuffle输出的分区个数
   * When this reaches [[numPartitions]], this map stage is ready.
   * 当它等于numPartitions时，这个map stage已经完成了。
   * This should be kept consistent as `outputLocs.filter(!_.isEmpty).size`.
   */
  /*已经shuffle输出的分区个数*/
  def numAvailableOutputs: Int = _numAvailableOutputs

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   * This should be the same as `outputLocs.contains(Nil)`.
   */
  /*也就是说，ShuffleMapStage的所有分区的map任务都执行成功后，ShuffleMapStage才是可用*/
  def isAvailable: Boolean = _numAvailableOutputs == numPartitions

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  /*找到所有还未执行成功而需要计算的分区*/
  override def findMissingPartitions(): Seq[Int] = {
    /*也就是outputLocs中partitionid 对应的MapStatus列表为空，就认为缺失*/
    val missing = (0 until numPartitions).filter(id => outputLocs(id).isEmpty)
    assert(missing.size == numPartitions - _numAvailableOutputs,
      s"${missing.size} missing, expected ${numPartitions - _numAvailableOutputs}")
    missing
  }
  /*当某一分区的任务执行完成后，首先*/
  def addOutputLoc(partition: Int, status: MapStatus): Unit = {
    /*取出partition关联的List*/
    val prevList = outputLocs(partition)
    /*将status添加到List前缀，然后将list再赋值给outputLocs(partition)*/
    outputLocs(partition) = status :: prevList
    /*如果prevList为空，可用分区数加1【说明以前此分区没有输出数据(不可用)】*/
    if (prevList == Nil) {
      _numAvailableOutputs += 1
    }
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId): Unit = {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      _numAvailableOutputs -= 1
    }
  }

  /**
   * Returns an array of [[MapStatus]] (index by partition id). For each partition, the returned
   * value contains only one (i.e. the first) [[MapStatus]]. If there is no entry for the partition,
   * that position is filled with null.
   */
  def outputLocInMapOutputTrackerFormat(): Array[MapStatus] = {
    outputLocs.map(_.headOption.orNull)
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    var becameUnavailable = false
    for (partition <- 0 until numPartitions) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        _numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, _numAvailableOutputs, numPartitions, isAvailable))
    }
  }
}
