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

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.RDDInfo

/**
 * :: DeveloperApi ::
 * Stores information about a stage to pass from the scheduler to SparkListeners.
 * 存储有关从scheduler传递到sparklistener的stage信息。
 */
@DeveloperApi
class StageInfo(
    val stageId: Int,
    val attemptId: Int,
    val name: String,
    val numTasks: Int,
    val rddInfos: Seq[RDDInfo], /*和stage当前rdd形成窄依赖链条（或piple）的所有祖先RDD的RDDInfos（包含stage当前rdd）*/
    val parentIds: Seq[Int],
    val details: String,  /*详细的线程栈信息*/
    val taskMetrics: TaskMetrics = null,
    /*任务的本地性偏好*/
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty) {
  /** When this stage was submitted from the DAGScheduler to a TaskScheduler. */
  /*stage从DAGScheduler提交到TaskScheduler的时间*/
  var submissionTime: Option[Long] = None
  /** Time when all tasks in the stage completed or when the stage was cancelled. */
  /*stage上的所有task计算完成的时间 或 stage被取消的时间*/
  var completionTime: Option[Long] = None
  /** If the stage failed, the reason why. */
  /*如果stage失败了，用于记录失败的原因*/
  var failureReason: Option[String] = None

  /**
   * Terminal values of accumulables updated during this stage, including all the user-defined
   * accumulators.
   * 这一阶段累计更新的终端值，包括所有用户定义的累加器。
   */
  /*存储了所有聚合器计算的最终值*/
  val accumulables = HashMap[Long, AccumulableInfo]()

  /*保存stage失败的原因*/
  def stageFailed(reason: String) {
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)
  }

  /*获取stage状态*/
  private[spark] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }
}

private[spark] object StageInfo {
  /**
   * Construct a StageInfo from a Stage.
   * 构造一个Stage的StageInfo。
   *
   * Each Stage is associated with one or many RDDs, with the boundary of a Stage marked by
   * shuffle dependencies. Therefore, all ancestor RDDs related to this Stage's RDD through a
   * sequence of narrow dependencies should also be associated with this Stage.
   * 每个Stage都与一个或多个rdd相关，以shuffle dependencies标志为stage边界。
   * 因此，所有通过一系列窄依赖关系与此阶段的RDD相关的祖先RDD也应该与此阶段相关联。
   */
  def fromStage(
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,
      taskMetrics: TaskMetrics = null,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty
    ): StageInfo = {
    /*获取能和当前rdd形成窄依赖链条（或piple）的所有祖先RDD，然后转换为RDDInfo*/
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    /*当前stage的RDD创建对应的RDDInfo，然后将上一步的结果一起放入rddInfos*/
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos
    /*new一个StageInfo*/
    new StageInfo(
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos, /*和stage当前rdd形成窄依赖链条（或piple）的所有祖先RDD的RDDInfos（包含stage当前rdd）*/
      stage.parents.map(_.id),
      stage.details,
      taskMetrics,
      taskLocalityPreferences)
  }
}
