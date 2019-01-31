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

import java.util.Properties

import org.apache.spark.util.CallSite

/**
 * A running job in the DAGScheduler. Jobs can be of two types: a result job, which computes a
 * ResultStage to execute an action, or a map-stage job, which computes the map outputs for a
 * ShuffleMapStage before any downstream stages are submitted. The latter is used for adaptive
 * query planning, to look at map output statistics before submitting later stages. We distinguish
 * between these two types of jobs using the finalStage field of this class.
 * DAGScheduler中正在运行的作业。
 *
 * Jobs are only tracked for "leaf" stages that clients directly submitted, through DAGScheduler's
 * submitJob or submitMapStage methods. However, either type of job may cause the execution of
 * other earlier stages (for RDDs in the DAG it depends on), and multiple jobs may share some of
 * these previous stages. These dependencies are managed inside DAGScheduler.
 * 作业只跟踪客户直接提交的“叶子”stage，通过DAGScheduler的submitJob和submitMapStage方法。然而，
 * 无论哪种类型的job都可能引起其他更早stages的执行（dag中依赖的rdd），多个job可能会共享一些前面的stage。
 * 这些依赖由DAGScheduler来管理。
 *
 * @param jobId A unique ID for this job.
 * @param finalStage The stage that this job computes (either a ResultStage for an action or a
 *   ShuffleMapStage for submitMapStage).
 * @param callSite Where this job was initiated in the user's program (shown on UI).
 * @param listener A listener to notify if tasks in this job finish or the job fails.
 * @param properties Scheduling properties attached to the job, such as fair scheduler pool name.
 */
/*用来表示已激活的job，即被DAGScheduler接收处理的Job*/
private[spark] class ActiveJob(
    val jobId: Int,
    val finalStage: Stage,  /*job的最下游Stage*/
    val callSite: CallSite,
    val listener: JobListener,  /*jobWaiter*/
    val properties: Properties) {

  /**
   * Number of partitions we need to compute for this job. Note that result stages may not need
   * to compute all partitions in their target RDD, for actions like first() and lookup().
   * 注意，对于first()和lookup()等操作，result stages可能不需要计算其目标RDD中的所有分区。
   * 这个作业需要计算的分区数。
   */
  /*当前job的分区数量*/
  val numPartitions = finalStage match {
      /*ResultStage： 可能只需要计算对应rdd的其中一些分区*/
    case r: ResultStage => r.partitions.length
      /*ShuffleMapStage：对应rdd的所有分区都需要计算*/
    case m: ShuffleMapStage => m.rdd.partitions.length
  }

  /** Which partitions of the stage have finished */
  /*使用一个boolean数组来记录那些分区已经计算完成，以数组下标为分区id，
  finished[2] = false, 表示id为2的分区 已经计算完成*/
  val finished = Array.fill[Boolean](numPartitions)(false)

  /*已完成的分区个数*/
  var numFinished = 0
}
