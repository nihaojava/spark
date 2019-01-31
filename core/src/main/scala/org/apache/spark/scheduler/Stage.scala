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

import scala.collection.mutable.HashSet

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * A stage is a set of parallel tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 * stage是一组并行任务，它们都计算作为Spark作业一部分运行的相同函数，其中所有任务具有相同的shuffle依赖项。
 * scheduler运行的每一个tasks的DAG在发生shuffle的边界上被划分为多个stages，
 * 然后DAGScheduler按照拓扑顺序运行这些stages。
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * other stage(s), or a result stage, in which case its tasks directly compute a Spark action
 * (e.g. count(), save(), etc) by running a function on an RDD. For shuffle map stages, we also
 * track the nodes that each output partition is on.
 * 每个stage可能是shuffle map stage，在这种情况下它的task的结果是其他stages的输入；
 * 也可能是result stage，在这种情况下它的task：通过在RDD上运行函数，直接计算一个spark action(例如：count(), save(),等等)
 * 对于shuffle map stages，我们还跟踪每个输出分区所在的节点。
 *
 * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 * 每个阶段也有一个firstJobId，标识第一个提交stage的作业。当使用FIFO调度时，这允许先计算早期作业的stage，
 * 或者在出现故障时更快地恢复。
 *
 * Finally, a single stage can be re-executed in multiple attempts due to fault recovery. In that
 * case, the Stage object will track multiple StageInfo objects to pass to listeners or the web UI.
 * The latest one will be accessible through latestInfo.
 * 最后，由于故障恢复，可以在多次尝试中重新执行单个阶段。
 * 在这种情况下，Stage对象将跟踪要传递给侦听器或web UI的多个StageInfo对象。
 * 最新的一个将通过latestInfo访问。
 *
 * @param id Unique stage ID
 * @param rdd RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
 *   on, while for a result stage, it's the target RDD that we ran an action on
 *  这个阶段运行的RDD：对于shuffle map stage，它是我们运行map task的RDD；
 *  对于result stage，它是我们运行action的目标RDD。
 * @param numTasks Total number of tasks in stage; result stages in particular may not need to
 *   compute all partitions, e.g. for first(), lookup(), and take().
 *   stage的task数量，result stages可能不需要计算所有分区，例如first(), lookup(),take()
 * @param parents List of stages that this stage depends on (through shuffle dependencies).
 *                当前stage依赖的父stage列表（通过shuffle依赖）
 * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
 * @param callSite Location in the user program associated with this stage: either where the target
 *   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
 */
private[scheduler] abstract class Stage(
    val id: Int,  /*唯一的stageID*/
    val rdd: RDD[_],  /*当前stage运行的RDD*/
    val numTasks: Int,  /*当前stage的task数量*/
    val parents: List[Stage], /*父stage列表，说明一个stage可以有一到多个父stages*/
    val firstJobId: Int,  /*第一个提交当前stage的job*/
    val callSite: CallSite) /**/
  extends Logging {

  /*当前stage的分区数量。实际为rdd的分区数量*/
  val numPartitions = rdd.partitions.length

  /** Set of jobs that this stage belongs to. */
  /*当前stage所属的job集合，说明一个stage可以属于多个job*/
  val jobIds = new HashSet[Int]

  /*存放等待分区，【分区的计算任务已经提交，等待结果】*/
  val pendingPartitions = new HashSet[Int]

  /** The ID to use for the next new attempt for this stage. */
  /*用于生成此stage下一次尝试的id*/
  private var nextAttemptId: Int = 0

  val name: String = callSite.shortForm
  val details: String = callSite.longForm

  /**
   * Pointer to the [StageInfo] object for the most recent attempt. This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
   * 指向最近一次尝试的[StageInfo]对象。在实际创建任何尝试之前，需要在这里初始化它，
   * 因为DAGScheduler使用这个StageInfo告诉sparklistener作业何时启动(它在创建任何stage尝试之前发生)。
   */
  /*Stage最近一次尝试的信息，StageInfo*/
  private var _latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId)

  /**
   * Set of stage attempt IDs that have failed with a FetchFailure. We keep track of these
   * failures in order to avoid endless retries if a stage keeps failing with a FetchFailure.
   * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
   * multiple tasks from the same stage attempt fail (SPARK-5945).
   */
  /*发生过fetchFailed的Stage尝试的身份标识的集合。*/
  private val fetchFailedAttemptIds = new HashSet[Int]

  /*清空fetchFailedAttemptIds*/
  private[scheduler] def clearFailures() : Unit = {
    fetchFailedAttemptIds.clear()
  }

  /**
   * Check whether we should abort the failedStage due to multiple consecutive fetch failures.
   *
   * This method updates the running set of failed stage attempts and returns
   * true if the number of failures exceeds the allowable number of failures.
   */
  /*用于将发生了fetchFailed的stageAttemptId添加到fetchFailedAttemptIds*/
  private[scheduler] def failedOnFetchAndShouldAbort(stageAttemptId: Int): Boolean = {
    fetchFailedAttemptIds.add(stageAttemptId)
    fetchFailedAttemptIds.size >= Stage.MAX_CONSECUTIVE_FETCH_FAILURES
  }

  /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID. */
  /*创建一个新的StageAttempt*/
  def makeNewStageAttempt(
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    val metrics = new TaskMetrics
    metrics.register(rdd.sparkContext)
    /*创建新的StageInfo*/
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences)
    nextAttemptId += 1
  }

  /** Returns the StageInfo for the most recent attempt for this stage. */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */
  /*找到缺失的分区，需要子类实现。*/
  def findMissingPartitions(): Seq[Int]
}

private[scheduler] object Stage {
  // The number of consecutive failures allowed before a stage is aborted
  /*在一个stage中止之前允许的最大连续失败次数*/
  val MAX_CONSECUTIVE_FETCH_FAILURES = 4
}
