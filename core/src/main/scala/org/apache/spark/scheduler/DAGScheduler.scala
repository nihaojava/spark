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

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable.{HashMap, HashSet, Stack}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
 * tasks that can run right away based on the data that's already on the cluster (e.g. map output
 * files from previous stages), though it may fail if this data becomes unavailable.
 * 高级调度层实现面向stage的调度。它为每个作业计算一个stage的DAG，跟踪哪些rdd和阶段输出是具体化的，
 * 并找到运行作业的最小调度。然后，它将阶段作为任务集提交给在集群上运行它们的底层TaskScheduler实现。
 * 任务集包含完全独立的任务，这些任务可以基于集群中已经存在的数据(例如，前一阶段map的输出文件)立即运行，
 * 但是如果这些数据不可用，任务集可能会失败。
 *
 * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
 * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
 * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
 * set of map output files, and another to read those files after a barrier). In the end, every
 * stage will have only shuffle dependencies on other stages, and may compute multiple operations
 * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
 * various RDDs (MappedRDD, FilteredRDD, etc).
 *
 * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * When looking through this code, there are several key concepts:
 * 在浏览这段代码时，有几个关键的概念:
 *
 *  - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.
 *    For example, when the user calls an action, like count(), a job will be submitted through
 *    submitJob. Each Job may require the execution of multiple stages to build intermediate data.
 *   作业(由[[ActiveJob]]表示)是提交给scheduler的顶级工作项。例如，当用户调用count()这样的操作时，
 *   作业将通过submitJob提交。每个作业可能需要执行多个阶段来构建中间数据。
 *
 *  - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each
 *    task computes the same function on partitions of the same RDD. Stages are separated at shuffle
 *    boundaries, which introduce a barrier (where we must wait for the previous stage to finish to
 *    fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that
 *    executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.
 *    Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
 *    Stage ([[Stage]])是计算作业中间结果的任务集，其中每个任务在相同RDD的不同分区上计算相同的函数。
 *    Stages在shuffle边界处分开，这就引入了一个障碍(我们必须等待上一阶段完成才能获取输出)。
 *    有两种阶段:[[ResultStage]]，用于执行操作的最后阶段;[[ShuffleMapStage]]，用于为洗牌写入映射输出文件。
 *    如果这些作业重用相同的rdd，那么阶段通常在多个作业之间共享。
 *
 *  - Tasks are individual units of work, each sent to one machine.
 *    任务是单个的工作单元，每个任务被发送到一台机器。
 *
 *  - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
 *    and likewise remembers which shuffle map stages have already produced output files to avoid
 *    redoing the map side of a shuffle.
 *    缓存跟踪:DAGScheduler找出缓存哪些rdd以避免重新计算它们，并同样记住哪些洗牌映射阶段已经生成了输出文件，
 *    以避免重新执行洗牌的映射端。
 *
 *  - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
 *    on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
 *    首选位置:DAGScheduler还根据底层rdd的首选位置或缓存或洗牌数据的位置计算在某个阶段中运行每个任务的位置。
 *
 *  - Cleanup: all data structures are cleared when the running jobs that depend on them finish,
 *    to prevent memory leaks in a long-running application.
 *    清理:当依赖于它们的运行作业完成时，将清除所有数据结构，以防止长时间运行的应用程序中的内存泄漏。
 *
 * To recover from failures, the same stage might need to run multiple times, which are called
 * "attempts". If the TaskScheduler reports that a task failed because a map output file from a
 * previous stage was lost, the DAGScheduler resubmits that lost stage. This is detected through a
 * CompletionEvent with FetchFailed, or an ExecutorLost event. The DAGScheduler will wait a small
 * amount of time to see whether other nodes or tasks fail, then resubmit TaskSets for any lost
 * stage(s) that compute the missing tasks. As part of this process, we might also have to create
 * Stage objects for old (finished) stages where we previously cleaned up the Stage object. Since
 * tasks from the old attempt of a stage could still be running, care must be taken to map any
 * events received in the correct Stage object.
 * 要从失败中恢复，同一阶段可能需要运行多次，这将被称作“尝试”。如果TaskScheduler报告某个任务失败，
 * 原因是来自上一阶段的map输出文件丢失，DAGScheduler重新提交丢失的阶段。
 * 这是通过CompletionEvent的FetchFailed事件或CompletionEvent的ExecutorLost事件检查到的。
 * DAGScheduler将等待一小段时间查看其他节点或任务是否失败，然后为丢失的stage重新提交任务集。
 * 作为这个过程的一部分，我们可能还需要创建旧(已完成)阶段的阶段对象，我们以前清理了的阶段对象。
 *
 * Here's a checklist to use when making or reviewing changes to this class:
 * 下面是对这个类进行更改或评审时使用的检查列表:
 *
 *  - All data structures should be cleared when the jobs involving them end to avoid indefinite
 *    accumulation of state in long-running programs.
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
 */
private[spark]
class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock()) /*时钟对象*/
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  /*有关DAGScheduler的度量源*/
  private[spark] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)
  /*生成下一个jobId*/
  private[scheduler] val nextJobId = new AtomicInteger(0)
  /*总共提交的作业数量*/
  private[scheduler] def numTotalJobs: Int = nextJobId.get()
  /*用于生成下一个StageId*/
  private val nextStageId = new AtomicInteger(0)

  /*jobId 和 stageId 的映射关系，1对多*/
  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  /*stageId 和 stage 之间的映射关系，1对1*/
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  /**
   * Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for
   * that dependency. Only includes stages that are part of currently running job (when the job(s)
   * that require the shuffle stage complete, the mapping will be removed, and the only record of
   * the shuffle data will be in the MapOutputTracker).
   *
   */
  /*shuffleId 和 shuffleMapStage之间的映射关系，1对1，会为每个shuffleDependency生成一个shuffleMapStage*/
  private[scheduler] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]
  /*jobId 和 ActiveJob之间的关系，1对1*/
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done
  /*处于等待状态的Stage集合【一般是等待它的父Stage计算完再提交】*/
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  /*处于运行状态的Stage集合*/
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  /*处于失败状态的Stage集合*/
  private[scheduler] val failedStages = new HashSet[Stage]
  /*所有激活的job集合*/
  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
   *
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
   *
   */
  /*缓存每个RDD的所有分区的位置信息Seq[TaskLocation]
  * 【
  * 在提交每个stage前会将此清空】*/
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  /*当检测到一个节点出现故障时，会将执行失败的Executor和MapOutputTracker当前的纪元添加到failedEpoch。*/
  private val failedEpoch = new HashMap[String, Long]
  /*sparkEnv中的组件*/
  private [scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  /*sparkEnv中的组件*/
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  /*在测试过程中，当发生FetchFailed时，是否禁止重试*/
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)
  /*只有一个线程的Scheduled线程池。
  * 对失败的Stage进行重试*/
  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  /*事件循环处理器【一个】*/
  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)

  /**
   * Called by the TaskSetManager to report task's starting.
   */
  /*由TaskSetManager调用，报告task开始执行*/
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  /*TaskSetManager报告task已经计算完成并且已经获取到计算结果*/
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
   * Called by the TaskSetManager to report task completions or failures.
   * 由TaskSetManager调用以报告任务完成或失败。
   */
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Seq[AccumulatorV2[_, _]],
      taskInfo: TaskInfo): Unit = {
    /*向eventProcessLoop投递CompletionEvent事件*/
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   * 更新正在执行的任务的度量标准，并让管理员知道BlockManager仍然有效。
   * 如果driver知道给定的block manager，则返回true。否则返回false，指示block manager应当重新注册。
   */
  def executorHeartbeatReceived(
      execId: String,
      // (taskId, stageId, stageAttemptId, accumUpdates)
      accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
      blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates))
    blockManagerMaster.driverEndpoint.askWithRetry[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.
   * 当一个executor失败的时候，由TaskScheduler来实现调度。
   */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
   * 当添加主机时，由TaskScheduler的实现调用此方法。
   */
  /*向eventProcessLoop投递ExecutorAdded事件*/
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  /*用于获取RDD各个分区的TaskLocation（TaskLocation(bm.host, bm.executorId)）序列。*/
  /*TaskLocation 记录了该分区对应的task在哪个host和executor 完成了计算。*/
  /*概括就是获取RDD各个分区缓存位置【在这个过程中会如果rdd已经持久化过了，获取持久化的位置host，executorId】*/
  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    /*如果cacheLocs中不包含此rdd*/
    if (!cacheLocs.contains(rdd.id)) {
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      /*如果rdd的StorageLevel为None，构造一个空的*/
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)
      } else {
        /*如果rdd的StorageLevel不为NONE，首先构造RDD各个分区的RDDBlockId数组*/
        val blockIds =
          rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        /*然后调用blockManagerMaster的getLocations方法获取数组中每个RDDBlockId所存储的位置信息（即blockManageId）序列，
        * 并封装为TaskLocation*/
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      /*先存，将rdd.id 和 locs 之间的映射关系存入cacheLocs*/
      cacheLocs(rdd.id) = locs
    }
    /*然后再取*/
    cacheLocs(rdd.id)
  }

  /*清空，cacheLocs中缓存的每个RDD的所有分区的位置信息，提交job前应该清空*/
  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
   * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
   * addition to any missing ancestor shuffle map stages.
   */
  /*获取或创建ShuffleMapStage。
  * 如果此shuffleDep对应的shuffleMapStage已经存在shuffleIdToMapStage【表示已经创建了shuffleMapStage】中则直接获取；
  * 否则不仅会创建此shuffleDep的ShuffleMapStage还会为此shuffleDep的祖先中的没有创建shuffleMapStage的shuffleDep创建
  * shuffleMapStage*/
  private def getOrCreateShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    /*如果已经创建过stage就直接返回该stage*/
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) =>
        stage

      case None =>
        // Create stages for all missing ancestor shuffle dependencies.
        /*找到所有未创建过ShuffleMapStage的祖先shuffleDependencies，遍历为每个创建ShuffleMapStage*/
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
          // that were not already in shuffleIdToMapStage, it's possible that by the time we
          // get to a particular dependency in the foreach loop, it's been added to
          // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
          // SPARK-13902 for more information.
          /*再次确认shuffleDependencies未注册过ShuffleMapStage
          【dep.shuffleId会new一个shuffleId肯定不存在啊？val只会再创建的时候执行赋值依次，类似java中的final】*/
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            /*创建ShuffleMapStage*/
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        /*最后，为给定的shuffleDep创建ShuffleMapStage */
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }

  /**
   * Creates a ShuffleMapStage that generates the given shuffle dependency's partitions. If a
   * previously run stage generated the same shuffle data, this function will copy the output
   * locations that are still available from the previous shuffle to avoid unnecessarily
   * regenerating data.
   *
   */
  /*根据ShuffleDependency和jobId 创建ShuffleMapStage*/
  def createShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): ShuffleMapStage = {
    /*获取shuffleDep的rdd属性，作为将要创建的ShuffleMapStage的rdd*/
    /*【特别注意：这里的rdd是shuffleDep里的rdd】*/
    val rdd = shuffleDep.rdd
    /*rdd分区个数 即 为task数*/
    val numTasks = rdd.partitions.length
    /*获取父stage*/
    val parents = getOrCreateParentStages(rdd, jobId)
    /*获取当前stageId 并 nextStageId=+1*/
    val id = nextStageId.getAndIncrement()
    /*【特别注意：这里的rdd是shuffleDep里的rdd】*/
    val stage = new ShuffleMapStage(id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep)
    /*更新 stageid 和 stage之间的映射*/
    stageIdToStage(id) = stage
    /*更新shuffleId 和 mapStage之间的映射*/
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    /*更新stage及其祖先stage的所属的jobs（jobIds）和当前job包含的stages（jobIdToStageIds）*/
    updateJobIdStageIdMaps(jobId, stage)

    /*查看是否已经存在shuffleId对应的MapStatus*/
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      /*如果存在，说明此stage已经计算过，可能以前计算的部分分区可用，也可能所有分区可用*/
      // A previously run stage generated partitions for this shuffle, so for each output
      // that's still available, copy information about that output location to the new stage
      // (so we don't unnecessarily re-compute that data).
      /*获取对MapStatus序列化后的字节数组*/
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      /*对上一步获得的字节数组进行反序列化，得到MapStatus数组*/
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      /*将MapStatus数组中不为null的 添加到 stage的outputLocs中【按照index一样添加】*/
      (0 until locs.length).foreach { i =>
        if (locs(i) ne null) {
          // locs(i) will be null if missing
          stage.addOutputLoc(i, locs(i))
        }
      }
    } else {
      /*如果不存在，则注册shuffleId与对应的MapStatus的映射关系*/
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
   * Create a ResultStage associated with the provided jobId.
   */
  /*创建与提供的jobId关联的ResultStage。*/
  private def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    /*获取或创建所有父stage的列表*/
    /*这步会为当前rdd的所有没有创建过ShuffleMapStage的shuffle依赖创建ShuffleMapStage，
     * 返回的结果是rdd的邻近ShuffleMapStage，用于创建ResultStage */
    val parents = getOrCreateParentStages(rdd, jobId)
    /*生成下一个stageId*/
    val id = nextStageId.getAndIncrement()
    /*new 一个ResultStage*/
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId, callSite)
    /*stageid 和 stage 映射关系 存入stageIdToStage*/
    stageIdToStage(id) = stage
    /*用来更新stage及其祖先stage的
      所属的jobs（jobIds）和 当前job包含的stages（jobIdToStageIds）*/
    updateJobIdStageIdMaps(jobId, stage)
    /*返回ResultStage*/
    stage
  }

  /**
   * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
   * the provided firstJobId.
   */
  /*获取或者创建给定RDD的所有父Stage，这些Stage将被分配给jobId对应的job*/
  private def getOrCreateParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
      /*【或者这样理解：1.获取给定RDD的依赖链中，最邻近此RDD的shuffle依赖序列（可能有多个）
      2.然后遍历上一步得到的依赖序列，为其创建ShuffleMapStage
      (在创建过程中如果它的祖先中的shuffle依赖也没有创建Stage，也会为其创建)】
  * 【结合stage划分的图更容易理解】*/
     getShuffleDependencies(rdd).map { shuffleDep =>
      /*为其创建ShuffleMapStage(在创建过程中如果它的祖先中的shuffle依赖也没有创建Stage，也会为其创建)*/
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
  }

  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  /*找到所有未创建过ShuffleMapStage的祖先shuffleDependencies*/
  private def getMissingAncestorShuffleDependencies(
      rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val ancestors = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        getShuffleDependencies(toVisit).foreach { shuffleDep =>
          /*shuffleDep为注册过*/
          /*【shuffleDep.shuffleId会new一个shuffleId肯定不存在啊？
          val只会再创建的时候执行赋值依次，类似java中的final】*/
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            /*将shuffleDep存入ancestors*/
            ancestors.push(shuffleDep)
            /*将shuffleDep.rdd放入waitingForVisit，while的时候会继续*/
            waitingForVisit.push(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
          /*否则，依赖以及其祖先已经注册过*/
        }
      }
    }
    ancestors
  }

  /**
   * Returns shuffle dependencies that are immediate parents of the given RDD.
   * 返回给定RDD的直接父RDD的shuffle依赖
   * This function will not return more distant ancestors.  For example, if C has a shuffle
   * dependency on B which has a shuffle dependency on A:
   *
   * A <-- B <-- C
   *
   * calling this function with rdd C will only return the B <-- C dependency.
   *
   * This function is scheduler-visible for the purpose of unit testing.
   */
  /*获取给定RDD的依赖链中，最邻近此RDD的shuffle依赖（可能有多个）
  也就是获取给定RDD的最直接的ShuffleDependencies（可能有多个）
  * 【结合stage划分的图来理解】*/
  private[scheduler] def getShuffleDependencies(
      rdd: RDD[_]): HashSet[ShuffleDependency[_, _, _]] = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new Stack[RDD[_]]
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.pop()
      if (!visited(toVisit)) {
        visited += toVisit
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.push(dependency.rdd)
        }
      }
    }
    parents
  }
  /*获取stage缺失数据的父stage*/
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        /*如果Stage的rdd的分区中存在没有对应TaskLocation序列的分区，
        * 说明当前stage的某个上游stage的某个分区任务未执行*/
        /*查看该rdd是否有缓存【是否每个partition的缓存都可用】*/
         val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        /*没有缓存或者有的分区缓存不可用*/
        /*【当有部分缓存的分区不可用的时候，缓存的还可以继续用，但是没有缓存的需要前面shuffle】*/
        if (rddHasUncachedPartitions) {
          /*dependencies*/
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                /*此是已经创建过stage了，直接获取*/
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                /*stage的上游ShuffleMapStage不可用，说明有分区没有计算完成*/
                if (!mapStage.isAvailable) {
                  /*加入到missing*/
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                /*加入waitingForVisit，继续往上找*/
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }//如果有缓存，这个链上停止向前找
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  /*用来更新stage及其祖先stage的
  所属的jobs（jobIds）和当前job包含的stages（jobIdToStageIds）*/
  /*每次createShuffleMapStage都会调用此方法*/
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        /*【一个stage可能属于多个job，一个job有多个stage】*/
        /*将jobId添加到每个Stage的jobids，即更新stage所属的job集合*/
        s.jobIds += jobId
        /*将jobId 和 StageId之间的映射关系更新到 jobIdToStageIds，即一个job包含多个stage*/
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        /*获取父stage中的jobIds没有包含此jobId的stage，即需要 更新所属job的父stage*/
        val parentsWithoutThisJobId = s.parents.filter { ! _.jobIds.contains(jobId) }
        /*将parentsWithoutThisJobId加入stage链表头，
        再调用updateJobIdStageIdMapsList，递归了，依次更新完所有祖先stage*/
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
   * 删除作业的状态和其他作业不需要的任何阶段。
   * @param job The job whose state to cleanup.
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleIdToMapStage.find(_._2 == stage)) {
                  shuffleIdToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
  }

  /**
   * Submit an action job to the scheduler.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
   */
  /*向taskScheduler提交一个action Job*/
  def submitJob[T, U](
      rdd: RDD[T],  /*执行action操作的rdd*/
      func: (TaskContext, Iterator[T]) => U,  /*每个分区task执行的函数*/
      partitions: Seq[Int], /*分区id序列*/
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,  /*对task执行结果进行处理的函数*/
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    /*检查以确保我们没有在不存在的分区上启动任务。*/
    /*rdd的分区个数，也是最大的分区ID*/
    val maxPartitions = rdd.partitions.length
    /*如果partitions中的分区ID大于 最大分区ID 或小于0，则抛出异常*/
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    /*生成一个新的jobId*/
    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      /*如果job的分区数量等于0，则创建一个totalTask属性为0的JobWaiter并返回。
      * 根据JobWaiter的实现，JobWaiter的jobPromise将设置为Success。*/
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    /*如果分区数大于0，则创建真正等待Job执行完成的JobWaiter*/
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    /*向eventProcessLoop发送JobSubmitted事件*/
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    /*返回Jobwaiter*/
    waiter
  }

  /**
   * Run an action job on the given RDD and pass all the results to the resultHandler function as
   * they arrive.
   *
   * @param rdd target RDD to run tasks on [目标RDD，也就是触发Action的RDD]
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to 每个分区结果回调此函数
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   * 要附加到此作业的调度程序属性，例如公平调度程序池名称
   * @throws Exception when the job fails
   */
  def runJob[T, U](
      rdd: RDD[T],  /*触发Action的RDD*/
      func: (TaskContext, Iterator[T]) => U,  /*对action提交的func进行了包装*/
      partitions: Seq[Int],/*触发Action的RDD的所有分区ID的集合*/
      callSite: CallSite,
      resultHandler: (Int, U) => Unit, /*对各个分区计算结果的处理函数*/
      properties: Properties): Unit = {
    /*生成运行job的启动时间start*/
    val start = System.nanoTime
    /*调用submitJob方法提交job。由于执行job的过程是异步的，因此submitJob方法将立即返回JobWaiter对象P326，
    * 当*/
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    // Note: Do not call Await.ready(future) because that calls `scala.concurrent.blocking`,
    // which causes concurrent SQL executions to fail if a fork-join pool is used. Note that
    // due to idiosyncrasies in Scala, `awaitPermission` is not actually used anywhere so it's
    // safe to pass in null here. For more detail, see SPARK-13747.
    val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
    /*利用JobWaiter等待job处理完毕【无限等待，直到有结果为止】。
      如果job执行成功，根据处理结果打印相应的日志。
    * 如果job执行失败，除打印日志外，还将抛出引起job失败的异常信息*/
    waiter.completionFuture.ready(Duration.Inf)(awaitPermission)
    waiter.completionFuture.value.get match {
        /*成功*/
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        /*失败*/
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  /**
   * Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
   * as they arrive. Returns a partial result object from the evaluator.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param evaluator [[ApproximateEvaluator]] to receive the partial results
   * @param callSite where in the user program this job was called
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.length).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, callSite, listener, SerializationUtils.clone(properties)))
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Submit a shuffle map stage to run independently and get a JobWaiter object back. The waiter
   * can be used to block until the job finishes executing or can be used to cancel the job.
   * This method is used for adaptive query planning, to run map stages and look at statistics
   * about their outputs before submitting downstream stages.
   *
   * @param dependency the ShuffleDependency to run a map stage for
   * @param callback function called with the result of the job, which in this case will be a
   *   single MapOutputStatistics object showing how much data was produced for each partition
   * @param callSite where in the user program this job was submitted
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def submitMapStage[K, V, C](
      dependency: ShuffleDependency[K, V, C],
      callback: MapOutputStatistics => Unit,
      callSite: CallSite,
      properties: Properties): JobWaiter[MapOutputStatistics] = {

    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw new SparkException("Can't run submitMapStage on RDD with 0 partitions")
    }

    // We create a JobWaiter with only one "task", which will be marked as complete when the whole
    // map stage has completed, and will be passed the MapOutputStatistics for that stage.
    // This makes it easier to avoid race conditions between the user code and the map output
    // tracker that might result if we told the user the stage had finished, but then they queries
    // the map output tracker and some node failures had caused the output statistics to be lost.
    val waiter = new JobWaiter(this, jobId, 1, (i: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, SerializationUtils.clone(properties)))
    waiter
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   * 取消一个正在执行或者在等待队列中的job
   */
  def cancelJob(jobId: Int): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId))
  }

  /**
   * Cancel all jobs in the given job group ID.
   * 取消指定grop中的所有job
   */
  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   * 取消所有正在执行或者在等待队列中的job
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int) {
    eventProcessLoop.post(StageCancelled(stageId))
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  /*重新提交失败的stage*/
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
  }

  /**
   * Check for waiting stages which are now eligible for resubmission.
   * Submits stages that depend on the given parent stage. Called when the parent stage completes
   * successfully.
   */
  /*检查现在有资格重新提交的waiting stages。提交依赖于parent stage的stages。
  * 当父stage成功完成时调用。*/
  private def submitWaitingChildStages(parent: Stage) {
    logTrace(s"Checking if any dependencies of $parent are now runnable")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    /*从waitingStages中帅选出父stages包含parent的*/
    val childStages = waitingStages.filter(_.parents.contains(parent)).toArray
    waitingStages --= childStages
    /*将childStages按jobid升序排列，执行submitStage*/
    for (stage <- childStages.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  /*找到Stage所属jobs中已经激活的job*/
  private def activeJobForStage(stage: Stage): Option[Int] = {
    /*使用此stage的job，sorted按id从小到大排序*/
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    /*过滤出上一步中处于激活状态的第一个job（id最小的）*/
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId)))
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
  }

  private[scheduler] def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
  }
  /*处理作业job提交*/
  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],     /*执行action的rdd*/
      func: (TaskContext, Iterator[_]) => _,  /*action算子对应的函数，对finalRDD的各个分区执行此函数*/
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,    /*jobWaiter*/
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      /*创建ResultStage*/
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    /*创建ActiveJob*/
    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    /*清空缓存的RDD各个分区计算结果位置cacheLocs*/
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    /*生成Job提交时间*/
    val jobSubmissionTime = clock.getTimeMillis()
    /*将jobId和刚创建的ActiveJob之间的对应关系放入jobIdToActiveJob*/
    jobIdToActiveJob(jobId) = job
    /*将刚创建的ActiveJob放入activeJobs中*/
    activeJobs += job
    /*finalStage的activeJob设置为刚创建的ActiveJob*/
    finalStage.setActiveJob(job)
    /*获取当前Job的所有Stage*/
    val stageIds = jobIdToStageIds(jobId).toArray
    /*获取当前Job的所有Stage对应的StageInfo（即数组stageInfos）*/
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    /*向LiveListenerBus投递SparkListenerJobStart事件，进而引发所有关注此事件的监听器执行相应的操作*/
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    /*调用submitStage方法提交finalStage*/
    submitStage(finalStage)
  }

  private[scheduler] def handleMapStageSubmitted(jobId: Int,
      dependency: ShuffleDependency[_, _, _],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    // Submitting this map stage might still require the creation of some parent stages, so make
    // sure that happens.
    var finalStage: ShuffleMapStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = getOrCreateShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got map stage job %s (%s) with %d output partitions".format(
      jobId, callSite.shortForm, dependency.rdd.partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)

    // If the whole stage has already finished, tell the listener and remove it
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }
  }

  /** Submits stage, but first recursively submits any missing parents. */
  /*提交stage，但首先递归地提交任何缺失的父stage。*/
  private def submitStage(stage: Stage) {
    /*获取当前stage对应的jobId（可能对应多个，取id最小的）*/
    val jobId = activeJobForStage(stage)
    /*检查jobId是否已定义*/
    if (jobId.isDefined) {
      /*已定义，*/
      logDebug("submitStage(" + stage + ")")
      /*检查此stage是否已经提交，即waitingStages和runningStages和failedStages都不包含此stage;
      * 如果已经提交，不执行任何动作*/
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        /*未提交*/
        /*获取当前stage的所有缺失数据的父Stage，按id从小到大【按id从小到大提交stage】*/
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        /*检查是否存在未提交的父Stage*/
        if (missing.isEmpty) {/*可能回溯到第一个shuffleMapStage（如果这些stage以前都没计算过）*/
          /*不存在，则提交当前stage所有未提交的Task*/
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          /*提交task的入口，在stage没有不可用的父stage时，提交当前stage还未提交的任务*/
          submitMissingTasks(stage, jobId.get)
        } else {
          /*存在,遍历还是调用submitStage将未提交的父stage提交，并将当前stage加入waitingStages中，
          * 表示当前stage必须等待所有父stage执行完成*/
          for (parent <- missing) {
            submitStage(parent)
          }
          /*添加到等待集合，等待所依赖的stage完成后再提交*/
          waitingStages += stage
        }
      }
    } else {
      /*未定义，则终止所有依赖failedStage的job*/
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  /** Called when stage's parents are available and we can now do its task. */
  /*提交task的入口，在stage没有不可用的父stage时，提交当前stage还未提交的任务*/
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    /*清空当前stage的待处理分区*/
    stage.pendingPartitions.clear()

    // First figure out the indexes of partition ids to compute.
    /*首先找到此stage中缺失的分区，也就是需要计算的分区*/
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    /*使用调度池、作业组、描述等。从与此stage相关的ActiveJob中*/
    /*获取ActiveJob的properties*/
    val properties = jobIdToActiveJob(jobId).properties
    /*将stage添加到正在运行的stages集合*/
    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    /*匹配提交的stage类型，启动对当前stage的输出提交到hdfs的协调*/
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    /*获取要计算的每一个分区的偏好位置*/
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }
    /*开始stage的执行尝试，并向listenerBus投递SparkListenerStageSubmitted事件*/
    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      /*对于ShuffleMapTask，那么对 rdd和shuffleDep进行序列化*/
      /*对于ResultTask，那么对 rdd和func进行序列化*/
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          JavaUtils.bufferToArray(/*stage.rdd 为 stage.shuffleDep里的rdd*/
            closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
        case stage: ResultStage =>
          JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
      }
      /*广播任务的序列化对象taskBinaryBytes*/
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    val tasks: Seq[Task[_]] = try {
      val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
      stage match {
          /*为ShuffleMapStage中要计算的每一个分区创建一个ShuffleMapTask*/
        case stage: ShuffleMapStage =>
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
              Option(sc.applicationId), sc.applicationAttemptId)
          }
        /*为ResultStage中要计算的每一个分区创建一个ResultTask*/
        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, properties, serializedTaskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }
    /*调用TaskScheduler的submitTask方法提交此批Task*/
    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      /*将提交的tasks的partitionIds 加入到 当前stage的pendingPartitions*/
      stage.pendingPartitions ++= tasks.map(_.partitionId)
      logDebug("New pending partitions: " + stage.pendingPartitions)
      /*为这批tasks创建TaskSet，调用taskScheduler.submitTasks提交*/
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      /*没有创建任何task，将当前Stage标记为完成*/
      markStageAsFinished(stage, None)

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)
      /*提交当前stage的子Stage【也就是在等待此stage完成的stage】*/
      submitWaitingChildStages(stage)
    }
  }

  /**
   * Merge local values from a task into the corresponding accumulators previously registered
   * here on the driver.
   * 将任务的本地值合并到先前在驱动程序上注册的相应累加器中。
   * Although accumulators themselves are not thread-safe, this method is called only from one
   * thread, the one that runs the scheduling loop. This means we only handle one task
   * completion event at a time so we don't need to worry about locking the accumulators.
   * This still doesn't stop the caller from updating the accumulator outside the scheduler,
   * but that's not our problem since there's nothing we can do about that.
   */
  /*更新累加器*/
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    try {
      event.accumUpdates.foreach { updates =>
        val id = updates.id
        // Find the corresponding accumulator on the driver and update it
        /*在driver上找到相应的累加器并更新它*/
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw new SparkException(s"attempted to access non-existent accumulator $id")
        }
        /*更新*/
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // To avoid UI cruft, ignore cases where value wasn't updated
        /*为了避免UI cruft，忽略没有更新值的情况*/
        if (acc.name.isDefined && !updates.isZero) {
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          event.taskInfo.setAccumulables(
            acc.toInfo(Some(updates.value), Some(acc.value)) +: event.taskInfo.accumulables)
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Failed to update accumulators for task ${task.partitionId}", e)
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   * 响应task完成。这在事件循环中调用，因此它假设可以修改调度程序的内部状态。
   * 使用taskEnded()从外部发布任务结束事件。
   */
  /*处理task完成事件【可能成功，可能失败】*/
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val taskId = event.taskInfo.id
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    /**/
    outputCommitCoordinator.taskCompleted(
      stageId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // Reconstruct task metrics. Note: this may be null if the task has failed.
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
            null
        }
      } else {
        null
      }

    // The stage may have already finished when we get this event -- eg. maybe it was a
    // speculative task. It is important that we send the TaskEnd event in any case, so listeners
    // are properly notified and can chose to handle it. For instance, some listeners are
    // doing their own accounting and if they don't get the task end event they think
    // tasks are still running when they really aren't.
    /*向事件总线投递SparkListenerTaskEnd事件*/
    listenerBus.post(SparkListenerTaskEnd(
       stageId, task.stageAttemptId, taskType, event.reason, event.taskInfo, taskMetrics))

    /*如果此stage已经取消了跳过后面的动作*/
    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }
    /*取出对应的Stage*/
    val stage = stageIdToStage(task.stageId)
    event.reason match {
        /*task执行成功*/
      case Success =>
        stage.pendingPartitions -= task.partitionId
        task match {
          /*对ResultTask的处理*/
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                  /*更新job里该分区状态为true*/
                  job.finished(rt.outputId) = true
                  /*job完成的分区数+1*/
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  /*如果整个job已经完成*/
                  if (job.numFinished == job.numPartitions) {
                    /*将Stage标记为完成*/
                    markStageAsFinished(resultStage)
                    /*清理工作*/
                    cleanupStateForJobAndIndependentStages(job)
                    /*向事件总线投递JobEnd事件，对应的监听器jobProgressListener会处理*/
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    /*调用job的listener【也就是jobWaiter】的taskSucceeded，
                    进而调用JobWaiter的resultHandler函数来处理该ResultTask的结果*/
                    /*P326*/
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }
          /*ShuffleMapTask*/
          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            /*更新累加器相关*/
            updateAccumulators(event)
            /*完成结果（状态）*/
            val status = event.result.asInstanceOf[MapStatus]
            /*结果所在executorId*/
            val execId = status.location.executorId
            /*记录日志ShuffleMapTask在那个executor上计算完成*/
            logDebug("ShuffleMapTask finished on " + execId)

            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              /*如果缓存的故障信息failedEpoch中包含此executor 并且
              failedEpoch中记录的executor的时间 大于等于 task中记录的故障时间*/
              /*则本次计算可能是虚假的，记录日志*/
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              /*正常情况*/
              /*记录partitionId 和 对应task的计算结果MapStatus 的映射*/
              shuffleStage.addOutputLoc(smt.partitionId, status)
            }

            /*如果shuffleStage在runningStages中 并且 shuffleStage的pendingPartitions为空*/
            /*说明此shuffleStage已经计算完成了（所有的partition都计算完了）*/
            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              /*标记当前stage为完成状态*/
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.
              /*当这个Stage完成的时候将 shuffleId和每个分区的结果MapStatus 记录到mapOutputTracker*/
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocInMapOutputTrackerFormat(),
                changeEpoch = true)

              /*一个shuffleMapStage计算完成就要清理cacheLocs*/
              clearCacheLocs()

              if (!shuffleStage.isAvailable) {
                /*返回true，说明有的任务失败了*/
                // Some tasks had failed; let's resubmit this shuffleStage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.findMissingPartitions().mkString(", "))
                /*重新提交stage*/
                submitStage(shuffleStage)
              } else {
                // Mark any map-stage jobs waiting on this stage as finished
                /*遍历与当前ShuffleMapStage相关联的ActiveJob列表，标记为为完成状态*/
                if (shuffleStage.mapStageJobs.nonEmpty) {
                  val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
                  for (job <- shuffleStage.mapStageJobs) {
                    markMapStageJobAsFinished(job, stats)
                  }
                }
                /*提交子stage*/
                submitWaitingChildStages(shuffleStage)
              }
            }
        }
        /*重新提交，将partitionId 加入pendingPartitions */
      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage.pendingPartitions += task.partitionId

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleIdToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
        } else {
          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, Some(failureMessage))
          } else {
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }

          val shouldAbortStage =
            failedStage.failedOnFetchAndShouldAbort(task.stageAttemptId) ||
            disallowStageRetryForTest

          if (shouldAbortStage) {
            val abortMessage = if (disallowStageRetryForTest) {
              "Fetch failure will not retry stage due to testing config"
            } else {
              s"""$failedStage (${failedStage.name})
                 |has failed the maximum allowable number of
                 |times: ${Stage.MAX_CONSECUTIVE_FETCH_FAILURES}.
                 |Most recent failure reason: $failureMessage""".stripMargin.replaceAll("\n", " ")
            }
            abortStage(failedStage, abortMessage, None)
          } else { // update failedStages and make sure a ResubmitFailedStages event is enqueued
            // TODO: Cancel running tasks in the failed stage -- cf. SPARK-17064
            val noResubmitEnqueued = !failedStages.contains(failedStage)
            failedStages += failedStage
            failedStages += mapStage
            if (noResubmitEnqueued) {
              // We expect one executor failure to trigger many FetchFailures in rapid succession,
              // but all of those task failures can typically be handled by a single resubmission of
              // the failed stage.  We avoid flooding the scheduler's event queue with resubmit
              // messages by checking whether a resubmit is already in the event queue for the
              // failed stage.  If there is already a resubmit enqueued for a different failed
              // stage, that event would also be sufficient to handle the current failed stage, but
              // producing a resubmit for each failed stage makes debugging and logging a little
              // simpler while not producing an overwhelming number of scheduler events.
              logInfo(
                s"Resubmitting $mapStage (${mapStage.name}) and " +
                s"$failedStage (${failedStage.name}) due to fetch failure"
              )
              messageScheduler.schedule(
                new Runnable {
                  override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
                },
                DAGScheduler.RESUBMIT_TIMEOUT,
                TimeUnit.MILLISECONDS
              )
            }
          }
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            mapStage.removeOutputLoc(mapId, bmAddress)
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            handleExecutorLost(bmAddress.executorId, filesLost = true, Some(task.epoch))
          }
        }

      case commitDenied: TaskCommitDenied =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case exceptionFailure: ExceptionFailure =>
        // Tasks failed with exceptions might still have accumulator updates.
        updateAccumulators(event)

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case _: ExecutorLostFailure | TaskKilled | UnknownReason =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using external shuffle), the entire slave
   * is lost (likely including the shuffle service), or a FetchFailed occurred, in which case we
   * presume all shuffle data related to this executor to be lost.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private[scheduler] def handleExecutorLost(
      execId: String,
      filesLost: Boolean,
      maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (filesLost || !env.blockManager.externalShuffleServiceEnabled) {
        logInfo("Shuffle files lost for executor: %s (epoch %d)".format(execId, currentEpoch))
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleIdToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          mapOutputTracker.registerMapOutputs(
            shuffleId,
            stage.outputLocInMapOutputTrackerFormat(),
            changeEpoch = true)
        }
        if (shuffleIdToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
               "(epoch " + currentEpoch + ")")
    }
  }

  /*将Executor的身份标识从failedEpoch移除*/
  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      /*添加的host在早先遗失【失联】名单中*/
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
  }

  private[scheduler] def handleStageCancellation(stageId: Int) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          handleJobCancellation(jobId, s"because Stage $stageId was cancelled")
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason))
    }
  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   */
  /*将Stage标记为完成，将此stage从runningStages移除*/
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    /*计算stage的执行时间*/
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      /*如果stage执行过程中没有发生错误，那么设置stage的latestInfo的完成时间为当前系统时间*/
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      /*清空fetchFailedAttemptIds*/
      stage.clearFailures()
    } else {
      /*如果执行Stage的过程中发生了错误，那么调用stage.latestInfo.stageFailed保存失败原因和Stage完成时间*/
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo(s"$stage (${stage.name}) failed in $serviceTime s due to ${errorMessage.get}")
    }
    /*调用stageEnd停止对当前Stage的输出提交到HDFS的协调*/
    outputCommitCoordinator.stageEnd(stage.id)
    /*向listenerBus投递SparkListenerStageCompleted事件*/
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    /*将当前stage从正在运行的stage中移除*/
    runningStages -= stage
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  /*终止所有依赖failedStage的job*/
  private[scheduler] def abortStage(
      failedStage: Stage,
      reason: String,
      exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      /*如果已删除该stage，则跳过所有操作。*/
      return
    }
    /*找到依赖failedStage的activeJob*/
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    /*更新failedStage的完成时间*/
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      /*失败一个作业和仅用于该作业的所有阶段，并清理相关状态。*/
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  /*失败一个作业和仅用于该作业的所有stage，并清理相关状态。*/
  private def failJobAndIndependentStages(
      job: ActiveJob,
      failureReason: String,
      exception: Option[Throwable] = None): Unit = {
    val error = new SparkException(failureReason, exception.getOrElse(null))
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              markStageAsFinished(stage, Some(failureReason))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
              ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      // SPARK-15783 important to cleanup state first, just for tests where we have some asserts
      // against the state.  Otherwise we have a *little* bit of flakiness in the tests.
      cleanupStateForJobAndIndependentStages(job)
      job.listener.jobFailed(error)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  /*寻找targetStage所包含的rdd 是否 在stage依赖链中并且还没有计算完成*/
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    /*如果stage就是目标stage的，返回true*/
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              /*获取或创建shufDep的ShuffleMapStage*/
              val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
              /*如果ShuffleMapStage不可以用（还未计算完），加入waitingForVisit*/
              if (!mapStage.isAvailable) {
                waitingForVisit.push(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
              /*否则就没有必要跟踪依赖项了*/
            case narrowDep: NarrowDependency[_] =>
              /*如果是窄依赖，也计入waitingForVisit*/
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    /*返回visitedRdds中是否包含 目标stage的rdd*/
    visitedRdds.contains(target.rdd)
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  /*用于获取RDD的指定分区的偏好位置*/
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   * getPreferredLocs的递归实现。
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   */
  /*/*用于获取RDD的指定分区的偏好位置*/*/
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    /*避免对RDD指定分区的重复访问*/
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      /*之前访问的分区已经返回Nil。【意思就是以前获取过此RDD的该分区的偏好位置，没有找到返回了Nil，
      这次就不用再找了，直接返回Nil】*/
      return Nil
    }
    // If the partition is cached, return the cache locations
    /*如果分区被缓存，则返回缓存位置*/
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    /*如果RDD有一些位置首选项(输入RDDs就是这种情况)，则获取它们*/
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    /*遍历RDD的窄依赖，获取窄依赖中RDD的同一分区的偏好位置，以首先找到的偏好位置作为当前RDD的偏好位置返回*/
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }

      case _ =>
    }
    /*如果以上步骤都未能获取到RDD指定分区的偏好位置，则返回Nil*/
    Nil
  }

  /** Mark a map stage job as finished with the given output stats, and report to its listener.
    * 标记map stage job 为完成状态 */
  def markMapStageJobAsFinished(job: ActiveJob, stats: MapOutputStatistics): Unit = {
    // In map stage jobs, we only create a single "task", which is to finish all of the stage
    // (including reusing any previous map outputs, etc); so we just mark task 0 as done
    job.finished(0) = true
    job.numFinished += 1
    job.listener.taskSucceeded(0, stats)
    cleanupStateForJobAndIndependentStages(job)
    listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
  }

  def stop() {
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  /*【启动事件循环处理器线程】*/
  eventProcessLoop.start()
}

/*事件循环处理器*/
private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  /*event loop 线程run会执行此方法*/
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }
  /*event loop 线程会执行此方法*/
  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
      /*action触发提交job*/
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val filesLost = reason match {
        case SlaveLost(_, true) => true
        case _ => false
      }
      dagScheduler.handleExecutorLost(execId, filesLost)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stopInNewThread()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200
}
