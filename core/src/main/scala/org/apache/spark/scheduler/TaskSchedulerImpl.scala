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

import java.nio.ByteBuffer
import java.util.{Timer, TimerTask}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.Set
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, ThreadUtils, Utils}

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a [[LocalSchedulerBackend]] and setting
 * isLocal to true. It handles common logic, like determining a scheduling order across jobs, waking
 * up to launch speculative tasks, etc.
 * 通过一个SchedulerBackend为不同的clusters调度tasks。
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
 * 客户机应该首先调用initialize()和start()，然后通过runTasks方法提交任务集。
 *
 * THREADING: [[SchedulerBackend]]s and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * [[SchedulerBackend]]s synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 */
private[spark] class TaskSchedulerImpl private[scheduler](
    val sc: SparkContext,
    val maxTaskFailures: Int, /*任务失败的最大次数，分布式为4次，本地模式为1次*/
    private[scheduler] val blacklistTrackerOpt: Option[BlacklistTracker], /*BlacklistTracker黑名单跟踪器*/
    isLocal: Boolean = false) /*是否是local部署模式*/
  extends TaskScheduler with Logging
{

  /*local-cluster，standalone，yarn等完全分布式会调用此方法，
  *task最大重试次数，默认为4次 */
  def this(sc: SparkContext) = {
    this(
      sc,
      sc.conf.get(config.MAX_TASK_FAILURES),  /*task最大重试次数，默认为4次*/
      TaskSchedulerImpl.maybeCreateBlacklistTracker(sc))
  }

  /*local，local[N] 模式会调用此方法，task最大重试次数，默认为1次*/
  def this(sc: SparkContext, maxTaskFailures: Int, isLocal: Boolean) = {
    this(
      sc,
      maxTaskFailures,
      TaskSchedulerImpl.maybeCreateBlacklistTracker(sc),
      isLocal = isLocal)
  }

  val conf = sc.conf

  // How often to check for speculative tasks
  /*每个多长时间 检查一次是否有需要推断执行的tasks，默认为100s*/
  val SPECULATION_INTERVAL_MS = conf.getTimeAsMs("spark.speculation.interval", "100ms")

  // Duplicate copies of a task will only be launched if the original copy has been running for
  // at least this amount of time. This is to avoid the overhead of launching speculative copies
  // of tasks that are very short.
  /*保证原始任务最少需要执行的时间，单位为ms。
  * 只有原始任务执行时间超过此值，才能启动副本任务（为需要推断执行的task启动的task）*/
  val MIN_TIME_TO_SPECULATION = 100

  /*推断执行调度线程*/
  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved
  /*阈值以上，我们警告用户初始TaskSet可能会挨饿*/
  /*判断TaskSet饥饿的阀值，默认为15s*/
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task
  /*每个task需要分配的cpu，默认1个task1个cpu*/
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  /*StageId、Attempt、TaskSetManager 之间的缓存*/
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  // Protected by `this`
  /*taskId 和 TaskSetManager 的映射关系*/
  private[scheduler] val taskIdToTaskSetManager = new HashMap[Long, TaskSetManager]
  /*taskId 与 执行此task的 ExecutorId的映射关系*/
  val taskIdToExecutorId = new HashMap[Long, String]

  /*标记TaskSchedulerImpl是否已经接收到Task*/
  @volatile private var hasReceivedTask = false
  /*标记TaskSchedulerImpl已经接收到的Task是否有已经运行过的*/
  @volatile private var hasLaunchedTask = false
  /*处理饥饿的定时器*/
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  /*用于生成taskId*/
  val nextTaskId = new AtomicLong(0)

  // IDs of the tasks running on each executor
  /*executorId 和 运行在此executor上的task 的映射关系*/
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]

  /*executorId 和 运行在此executor上的task个数 的映射关系，
  * 根据executorIdToRunningTaskIds计算得到*/
  def runningTasksByExecutors: Map[String, Int] = synchronized {
    executorIdToRunningTaskIds.toMap.mapValues(_.size)
  }

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  /*host与运行其上的Executors映射，1对多*/
  protected val hostToExecutors = new HashMap[String, HashSet[String]]
  /*rack与hosts之间的映射，1对多*/
  protected val hostsByRack = new HashMap[String, HashSet[String]]

  /*Executor与Executor运行所在的Host之间的映射*/
  protected val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null
  /*后端调度接口*/
  var backend: SchedulerBackend = null
  /*map输出跟踪器*/
  val mapOutputTracker = SparkEnv.get.mapOutputTracker
  /*调度池构建器*/
  var schedulableBuilder: SchedulableBuilder = null
  /*根调度池*/
  var rootPool: Pool = null
  // default scheduler is FIFO
  /*调度模式，默认为FIFO，即将task排序的算法*/
  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
  val schedulingMode: SchedulingMode = try {
    SchedulingMode.withName(schedulingModeConf.toUpperCase)
  } catch {
    case e: java.util.NoSuchElementException =>
      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
  }

  // This is a var so that we can reset it for testing purposes.
  /*任务结果获取器*/
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  /*初始化方法*/
  def initialize(backend: SchedulerBackend) {
    /*将传过来的SchedulerBackend赋值给this.backend*/
    this.backend = backend
    // temporarily set rootPool name to empty
    /*new一个根调度池*/
    rootPool = new Pool("", schedulingMode, 0, 0)
    /*根据schedulingMode创建调度池构建器*/
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          /*默认为此schedulableBuilder*/
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported spark.scheduler.mode: $schedulingMode")
      }
    }
    /*构建调度池*/
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    /*启动SchedulerBachend*/
    backend.start()

    /*如果当前应用不是在Local模式下，并且设置了推断执行（默认为false），
    * 那么设置一个执行间隔为100ms的检查可推断任务的定时器*/
    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          /*在激活的job中检查可推断执行的任务*/
          checkSpeculatableTasks()
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  override def postStartHook() {
    waitBackendReady()
  }

  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      /*创建taskSetManager，默认为4此*/
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId) = manager
      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
        ts.taskSet != taskSet && !ts.isZombie
      }
      if (conflictingTaskSet) {
        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
          s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
      }
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    /*/*给调度池中的所有task分配资源*/*/
    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  /*创建taskSetManager*/
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures, blacklistTrackerOpt)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task and then abort
        //    the stage.
        // 2. The task set manager has been created but no tasks has been scheduled. In this case,
        //    simply abort the stage.
        tsm.runningTasksSet.foreach { tid =>
          val execId = taskIdToExecutorId(tid)
          backend.killTask(tid, execId, interruptThread)
        }
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    manager.parent.removeSchedulable(manager)
    logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed, from pool" +
      s" ${manager.parent.name}")
  }

  /*遍历申请执行task的各个Executor，依次为其分配task【可能有的executor没分配到】*/
  /*给单个TaskSet中的所有Task分配资源【】*/
  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,  /*TaskSetManager*/
      maxLocality: TaskLocality,  /*最大的本地性*/
      shuffledOffers: Seq[WorkerOffer], /*可以理解为申请执行task的各个Executor的信息*/
      availableCpus: Array[Int],  /*可以理解为申请执行task的各个Executor的可用cores*/
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]]) : Boolean = { /*tasks存放分配结果TaskDescription*/
    var launchedTask = false
    // nodes and executors that are blacklisted for the entire application have already been
    // filtered out by this point
    /*遍历shuffledOffers，申请执行task的各个Executor的信息*/
    for (i <- 0 until shuffledOffers.size) {
      /*获取WorkOffer的execId*/
      val execId = shuffledOffers(i).executorId
      /*获取WorkOffer的host*/
      val host = shuffledOffers(i).host
      /*如果WorkOffer的可用core数大于等于 CPUS_PER_TASK（默认为1），则继续执行*/
      if (availableCpus(i) >= CPUS_PER_TASK) {
        try {
          /*resourceOffer：用于给task按照本地性分配资源，返回Option[TaskDescription]
          * 返回的结果为TaskDescription或None，这里用for是为了处理None的情况？*/
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            /*将TaskDescription加入tasks*/
            tasks(i) += task
            /*taskId*/
            val tid = task.taskId
            /*记录taskIdToTaskSetManager、taskIdToExecutorId、executorIdToRunningTaskIds*/
            taskIdToTaskSetManager(tid) = taskSet
            taskIdToExecutorId(tid) = execId
            executorIdToRunningTaskIds(execId).add(tid)
            /*可用cores-CPUS_PER_TASK（默认为1）*/
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            /*launchedTask赋值为true，即TaskSet中的某个task分配到了资源*/
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   * 集群管理器调用以提供从服务器上的资源。我们通过询问活动任务集按照优先级排序来进行响应。
   * 我们以循环的方式将任务分配到每个节点，以便在集群中平衡任务。
   */
  /*为任务task分配资源*/
  def resourceOffers(offers: IndexedSeq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    /*将每个从属服务器标记为活动的，并记住其主机名*/
    // Also track if new executor is added
    /*还可以跟踪是否添加了新的executor*/

    /*是否添加了新executor*/
    var newExecAvail = false
    /*遍历offers序列，对每个WorkerOffer执行以下操作*/
    for (o <- offers) {
      /*如果offer中的host没在hostToExecutors中缓存，添加*/
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }
      /*如果offer中的executorId没在executorIdToRunningTaskIds中缓存，更新相关缓存*/
      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        hostToExecutors(o.host) += o.executorId
        /*向dagScheduler投递ExecutorAdded事件*/
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        /*标记添加了新的executor*/
        newExecAvail = true
      }
      /*更新host和机架之间的关系，默认机架名称为None*/
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Before making any offers, remove any nodes from the blacklist whose blacklist has expired. Do
    // this here to avoid a separate thread and added synchronization overhead, and also because
    // updating the blacklist is only relevant when task offers are being made.
    /* 在分配资源之前，从黑名单中删除黑名单已经过期的节点。*/
    blacklistTrackerOpt.foreach(_.applyBlacklistTimeout())
    /*将offers中host和executor在黑名单中的过滤掉，生成filteredOffers*/
    val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
      offers.filter { offer =>
        !blacklistTracker.isNodeBlacklisted(offer.host) &&
          !blacklistTracker.isExecutorBlacklisted(offer.executorId)
      }
    }.getOrElse(offers) /**/

    /*对所有的offers随机洗牌，保证任务分布均衡*/
    val shuffledOffers = shuffleOffers(filteredOffers)
    // Build a list of tasks to assign to each worker.
    /*创建要分配给每个workerOffers的任务列表。*/
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    /*统计每个executor的可用cores，存放在数组availableCpus中*/
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    /*对rootPool中的所有TaskSetManager按照调度算法排序，默认为FIFO*/
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    /*遍历排序好的TaskSetManager*/
    for (taskSet <- sortedTaskSets) {
      /*记录TaskSetManager的父调度池名称，TaskSetManager的名称，和TaskSetManager中正在运行的任务数*/
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      /*如果有新的executor添加，重新计算本地性*/
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    /*按照我们的调度顺序获取每个任务集，然后按照递增的局部性级别顺序为其提供每个节点，
    这样它就有机会在所有节点上启动本地任务。
    注意:首选的本地顺序:PROCESS_LOCAL、NODE_LOCAL、NO_PREF、RACK_LOCAL、ANY*/
    /**/
    /*遍历taskSetManager【提交的各个stage】，按照最大本地性原则，调用resourceOfferSingleTaskSet*/
    for (taskSet <- sortedTaskSets) {
      var launchedAnyTask = false
      var launchedTaskAtCurrentMaxLocality = false
      /*遍历taskSet允许的本地性数组，从高到低*/
      for (currentMaxLocality <- taskSet.myLocalityLevels) {
        do {
          /*resourceOfferSingleTaskSet为单个taskSet中的各个task分配资源，有分配到资源的task就返回true*/
          launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(
            taskSet, currentMaxLocality, shuffledOffers, availableCpus, tasks)
          launchedAnyTask |= launchedTaskAtCurrentMaxLocality
        } while (launchedTaskAtCurrentMaxLocality)/*这不是死循环嘛？
        【遍历直到taskSet中剩下的task分配完成或都没分配到资源】*/
      }
      /*如果没有一个taskSetManager中没有一个task获得了资源*/
      if (!launchedAnyTask) {
        taskSet.abortIfCompletelyBlacklisted(hostToExecutors)
      }
    }

    /*如果返回的TaskDescription列表【已分配到资源的task，1对1】大于0，hasLaunchedTask设置为true*/
    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    /*返回TaskDescription列表【已分配到资源的task，1对1】*/
    return tasks
  }

  /**
   * Shuffle offers around to avoid always placing tasks on the same workers.  Exposed to allow
   * overriding in tests, so it can be deterministic.
   */
  /*对offers进行洗牌（打乱排列顺序）*/
  protected def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] = {
    Random.shuffle(offers)
  }
  /*更新task状态【只有在此方法中使用了taskResultGetter】*/
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    var reason: Option[ExecutorLossReason] = None
    synchronized {
      try {
        taskIdToTaskSetManager.get(tid) match {
          case Some(taskSet) =>
            if (state == TaskState.LOST) {
              // TaskState.LOST is only used by the deprecated Mesos fine-grained scheduling mode,
              // where each executor corresponds to a single task, so mark the executor as failed.
              val execId = taskIdToExecutorId.getOrElse(tid, throw new IllegalStateException(
                "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)"))
              if (executorIdToRunningTaskIds.contains(execId)) {
                reason = Some(
                  SlaveLost(s"Task $tid was lost, so marking the executor as lost as well."))
                removeExecutor(execId, reason.get)
                failedExecutor = Some(execId)
              }
            }
            if (TaskState.isFinished(state)) {
              cleanupTaskState(tid)
              taskSet.removeRunningTask(tid)
              if (state == TaskState.FINISHED) {
                /*处理成功的task*/
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                /*处理失败的task，【重新放入待处理列表，并通知DAGScheduler重新调度】*/
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates) or its " +
                "executor has been marked as failed.")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      assert(reason.isDefined)
      dagScheduler.executorLost(failedExecutor.get, reason.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   * 更新正在执行的任务的度量标准，并让master知道BlockManager仍然有效。如果driver知道给定的块管理器，则返回true。
   * 否则，返回false，指示block manager应该重新注册。
   */
  override def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
      accumUpdates.flatMap { case (id, updates) =>
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        taskIdToTaskSetManager.get(id).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId)
  }
  /*标记此task为获取到结果，并通知dagScheduler*/
  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskFailedReason): Unit = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  override def stop() {
    speculationScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  /*在激活的job中检查可推断执行的任务*/
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    /*如果检查到有可以推断执行的任务*/
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (executorIdToRunningTaskIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)

          case None =>
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the slave while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get, reason)
      backend.reviveOffers()
    }
  }

  private def logExecutorLoss(
      executorId: String,
      hostPort: String,
      reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * Cleans up the TaskScheduler's state for tracking the given task.
   */
  private def cleanupTaskState(tid: Long): Unit = {
    taskIdToTaskSetManager.remove(tid)
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach { _.remove(tid) }
    }
  }

  /**
   * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
   */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason) {
    // The tasks on the lost executor may not send any more status updates (because the executor
    // has been lost), so they should be cleaned up here.
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      // We do not notify the TaskSetManager of the task failures because that will
      // happen below in the rootPool.executorLost() call.
      taskIds.foreach(cleanupTaskState)
    }

    val host = executorIdToHost(executorId)
    val execs = hostToExecutors.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      hostToExecutors -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }

    if (reason != LossReasonPending) {
      executorIdToHost -= executorId
      rootPool.executorLost(executorId, host, reason)
    }
    blacklistTrackerOpt.foreach(_.handleRemovedExecutor(executorId))
  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    hostToExecutors.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    hostToExecutors.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.contains(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
  }

  /**
   * Get a snapshot of the currently blacklisted nodes for the entire application.  This is
   * thread-safe -- it can be called without a lock on the TaskScheduler.
   */
  def nodeBlacklist(): scala.collection.immutable.Set[String] = {
    blacklistTrackerOpt.map(_.nodeBlacklist()).getOrElse(scala.collection.immutable.Set())
  }

  // By default, rack is unknown
  /*默认机架rack，为None*/
  def getRackForHost(value: String): Option[String] = None

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (sc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }

}


private[spark] object TaskSchedulerImpl {
  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used.  The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
   * [o1, o5, o4, 02, o6, o3]
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

  /*检查如果启用了Blacklist，则new一个BlacklistTracker；否则返回None*/
  private def maybeCreateBlacklistTracker(sc: SparkContext): Option[BlacklistTracker] = {
    if (BlacklistTracker.isBlacklistEnabled(sc.conf)) {
      Some(new BlacklistTracker(sc))
    } else {
      None
    }
  }

}
