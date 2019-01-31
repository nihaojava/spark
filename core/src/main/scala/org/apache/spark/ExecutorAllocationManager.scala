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

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{DYN_ALLOCATION_MAX_EXECUTORS, DYN_ALLOCATION_MIN_EXECUTORS}
import org.apache.spark.metrics.source.Source
import org.apache.spark.scheduler._
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * An agent that dynamically allocates and removes executors based on the workload.
 *
 * The ExecutorAllocationManager maintains a moving target number of executors which is periodically
 * synced to the cluster manager. The target starts at a configured initial value and changes with
 * the number of pending and running tasks.
 *
 * Decreasing the target number of executors happens when the current target is more than needed to
 * handle the current load. The target number of executors is always truncated to the number of
 * executors that could run all current running and pending tasks at once.
 *
 * Increasing the target number of executors happens in response to backlogged tasks waiting to be
 * scheduled. If the scheduler queue is not drained in N seconds, then new executors are added. If
 * the queue persists for another M seconds, then more executors are added and so on. The number
 * added in each round increases exponentially from the previous round until an upper bound has been
 * reached. The upper bound is based both on a configured property and on the current number of
 * running and pending tasks, as described above.
 *
 * The rationale for the exponential increase is twofold: (1) Executors should be added slowly
 * in the beginning in case the number of extra executors needed turns out to be small. Otherwise,
 * we may add more executors than we need just to remove them later. (2) Executors should be added
 * quickly over time in case the maximum number of executors is very high. Otherwise, it will take
 * a long time to ramp up under heavy workloads.
 *
 * The remove policy is simpler: If an executor has been idle for K seconds, meaning it has not
 * been scheduled to run any tasks, then it is removed.
 *
 * There is no retry logic in either case because we make the assumption that the cluster manager
 * will eventually fulfill all requests it receives asynchronously.
 *
 * The relevant Spark properties include the following:
 *
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *   spark.dynamicAllocation.initialExecutors - Number of executors to start with
 *
 *   spark.dynamicAllocation.schedulerBacklogTimeout (M) -
 *     If there are backlogged tasks for this duration, add new executors
 *
 *   spark.dynamicAllocation.sustainedSchedulerBacklogTimeout (N) -
 *     If the backlog is sustained for this duration, add more executors
 *     This is used only after the initial backlog timeout is exceeded
 *
 *   spark.dynamicAllocation.executorIdleTimeout (K) -
 *     If an executor has been idle for this duration, remove it
 */
private[spark] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    conf: SparkConf)
  extends Logging {

  allocationManager =>

  import ExecutorAllocationManager._

  // Lower and upper bounds on the number of executors.
  /*executors数量的上下边界*/
  /*下边界，可以通过spark.dynamicAllocation.minExecutors来设置，默认为0*/
  private val minNumExecutors = conf.get(DYN_ALLOCATION_MIN_EXECUTORS)
  /*上边界，可以通过spark.dynamicAllocation.maxExecutors来设置，默认为Int.MaxValue*/
  private val maxNumExecutors = conf.get(DYN_ALLOCATION_MAX_EXECUTORS)
  /*初始executors数量，如果不设置，默认为minNumExecutors*/
  private val initialNumExecutors = Utils.getDynamicAllocationInitialExecutors(conf)

  // How long there must be backlogged tasks for before an addition is triggered (seconds)
  /*在触发添加executor之前，必须有多长时间的积压任务(秒)，默认为1s*/
  private val schedulerBacklogTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.schedulerBacklogTimeout", "1s")

  // Same as above, but used only after `schedulerBacklogTimeoutS` is exceeded
  /*与上面相同，但仅在超过'schedulerBacklogTimeoutS'之后使用*/
  private val sustainedSchedulerBacklogTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", s"${schedulerBacklogTimeoutS}s")

  // How long an executor must be idle for before it is removed (seconds)
  /*executor在被删除之前必须空闲多长时间(秒)，默认为60s*/
  private val executorIdleTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.executorIdleTimeout", "60s")

  /*缓存的Executor的闲置时间*/
  private val cachedExecutorIdleTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.cachedExecutorIdleTimeout", s"${Integer.MAX_VALUE}s")

  // During testing, the methods to actually kill and add executors are mocked out
  private val testing = conf.getBoolean("spark.dynamicAllocation.testing", false)

  // TODO: The default value of 1 for spark.executor.cores works right now because dynamic
  // allocation is only supported for YARN and the default number of cores per executor in YARN is
  // 1, but it might need to be attained differently for different cluster managers
  /*分配仅支持对YARN，默认YARN中每个executor的core数为1，但对于不同的集群管理器，可能需要以不同的方式来实现它*/
  /*每个Executor的task数*/
  private val tasksPerExecutor =
    conf.getInt("spark.executor.cores", 1) / conf.getInt("spark.task.cpus", 1)

  validateSettings()

  // Number of executors to add in the next round
  /*要在下一轮中添加的executors的数量*/
  private var numExecutorsToAdd = 1

  // The desired number of executors at this moment in time. If all our executors were to die, this
  // is the number of executors we would immediately want from the cluster manager.
  /*期望目标Executors数。默认为initialNumExecutors，为0【？】*/
  private var numExecutorsTarget = initialNumExecutors

  // Executors that have been requested to be removed but have not been killed yet
  /*等待删除的Executors，已经向cluster manager发送删除请求，但是还没有删除*/
  private val executorsPendingToRemove = new mutable.HashSet[String]

  // All known executors
  /*所有已知executor*/
  private val executorIds = new mutable.HashSet[String]

  // A timestamp of when an addition should be triggered, or NOT_SET if it is not set
  // This is set when pending tasks are added but not scheduled yet
  /*触发添加executor的时间戳，如果没有触发则为NOT_SET。
  * 这是在添加等待的任务但尚未调度时设置的*/
  private var addTime: Long = NOT_SET

  // A timestamp for each executor of when the executor should be removed, indexed by the ID
  // This is set when an executor is no longer running a task, or when it first registers
  /*存放每个executorID 和 它应当被删除的时间点（有效期）*/
  private val removeTimes = new mutable.HashMap[String, Long]

  // Polling loop interval (ms)
  /*轮询时间间隔*/
  private val intervalMillis: Long = 100

  // Clock used to schedule when executors should be added and removed
  /*用于计划何时添加和删除执行器的时钟*/
  /*系统时钟*/
  private var clock: Clock = new SystemClock()

  // Listener for Spark events that impact the allocation policy
  /*监听器，用于监听影响分配策略的Spark事件*/
  private val listener = new ExecutorAllocationListener

  // Executor that handles the scheduling task.
  /*执行器，用于执行scheduling task*/
  /*executor是只有一个线程并周期执行的线程池ScheduledThreadPoolExecutor，
  * 以固定的时间间隔100ms进行调度*/
  private val executor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-dynamic-executor-allocation")

  // Metric source for ExecutorAllocationManager to expose internal status to MetricsSystem.
  /*用于ExecutorAllocationManager向MetricsSystem公开内部状态的度量源。*/
  val executorAllocationManagerSource = new ExecutorAllocationManagerSource

  // Whether we are still waiting for the initial set of executors to be allocated.
  /*是否仍然在等待初始化设置 要分配的executors*/
  // While this is true, we will not cancel outstanding executor requests. This is
  // set to false when:
  //   (1) a stage is submitted, or
  //   (2) an executor idle timeout has elapsed.
  @volatile private var initializing: Boolean = true

  // Number of locality aware tasks, used for executor placement.
  /*具有数据本地性的task数*/
  private var localityAwareTasks = 0

  // Host to possible task running on it, used for executor placement.
  /*Host 与 想要在此Host上运行的task数量之间的映射关系*/
  private var hostToLocalTaskCount: Map[String, Int] = Map.empty

  /**
   * Verify that the settings specified through the config are valid.
   * If not, throw an appropriate exception.
   * 校验config中指定的设置是否有效。
   * 如果没效，抛出相应的异常。
   */
  private def validateSettings(): Unit = {
    if (minNumExecutors < 0 || maxNumExecutors < 0) {
      throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be positive!")
    }
    if (maxNumExecutors == 0) {
      throw new SparkException("spark.dynamicAllocation.maxExecutors cannot be 0!")
    }
    if (minNumExecutors > maxNumExecutors) {
      throw new SparkException(s"spark.dynamicAllocation.minExecutors ($minNumExecutors) must " +
        s"be less than or equal to spark.dynamicAllocation.maxExecutors ($maxNumExecutors)!")
    }
    if (schedulerBacklogTimeoutS <= 0) {
      throw new SparkException("spark.dynamicAllocation.schedulerBacklogTimeout must be > 0!")
    }
    if (sustainedSchedulerBacklogTimeoutS <= 0) {
      throw new SparkException(
        "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout must be > 0!")
    }
    if (executorIdleTimeoutS <= 0) {
      throw new SparkException("spark.dynamicAllocation.executorIdleTimeout must be > 0!")
    }
    // Require external shuffle service for dynamic allocation
    // Otherwise, we may lose shuffle files when killing executors
    if (!conf.getBoolean("spark.shuffle.service.enabled", false) && !testing) {
      throw new SparkException("Dynamic allocation of executors requires the external " +
        "shuffle service. You may enable this through spark.shuffle.service.enabled.")
    }
    if (tasksPerExecutor == 0) {
      throw new SparkException("spark.executor.cores must not be less than spark.task.cpus.")
    }
  }

  /**
   * Use a different clock for this allocation manager. This is mainly used for testing.
   */
  def setClock(newClock: Clock): Unit = {
    clock = newClock
  }

  /**
   * Register for scheduler callbacks to decide when to add and remove executors, and start
   * the scheduling task.
   * 注册调度程序回调以决定何时添加和删除executors，并启动调度任务。
   */
  def start(): Unit = {
    /*向事件总线添加ExecutorAllocationListener*/
    listenerBus.addListener(listener)
    /*创建定时调度的任务scheduleTask，此任务主要调用schedule方法*/
    val scheduleTask = new Runnable() {
      override def run(): Unit = {
        try {
          /*调用schedule方法*/
          schedule()
        } catch {
          case ct: ControlThrowable =>
            throw ct
          case t: Throwable =>
            logWarning(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        }
      }
    }
    /*设置executor线程池要执行的任务，和调度时间间隔，100ms*/
    executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)
    /*根据我们的调度需求请求更新集群管理器。*/
    client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
  }

  /**
   * Stop the allocation manager.
   */
  def stop(): Unit = {
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  /**
   * Reset the allocation manager to the initial state. Currently this will only be called in
   * yarn-client mode when AM re-registers after a failure.
   */
  def reset(): Unit = synchronized {
    initializing = true
    numExecutorsTarget = initialNumExecutors
    numExecutorsToAdd = 1

    executorsPendingToRemove.clear()
    removeTimes.clear()
  }

  /**
   * The maximum number of executors we would need under the current load to satisfy all running
   * and pending tasks, rounded up.
   */
  /*计算需要的最大executors数*/
  private def maxNumExecutorsNeeded(): Int = {
    /*待执行的task数 和 正在运行的task数 之和*/
    val numRunningOrPendingTasks = listener.totalPendingTasks + listener.totalRunningTasks
    /*总的task 除以 每个executors运行的task数*/
    /* + tasksPerExecutor - 1 的目的是为了向上取整，那为什么写这么麻烦？*/
    (numRunningOrPendingTasks + tasksPerExecutor - 1) / tasksPerExecutor
  }

  /**
   * This is called at a fixed interval to regulate the number of pending executor requests
   * and number of executors running.
   *
   * First, adjust our requested executors based on the add time and our current needs.
   * Then, if the remove time for an existing executor has expired, kill the executor.
   *
   * This is factored out into its own method for testing.
   */
  private def schedule(): Unit = synchronized {
    val now = clock.getTimeMillis

    /*更新期望目标Executors数*/
    updateAndSyncNumExecutorsTarget(now)

    /*对过期的executors进行删除*/
    val executorIdsToBeRemoved = ArrayBuffer[String]()
    /*retain：只保留map中符合此断言的mapping
    * 此处是removeTimes中只留下没过期的executorIds，过期的加到executorIdsToBeRemoved中*/
    removeTimes.retain { case (executorId, expireTime) =>
      /*判断是否过期*/
      val expired = now >= expireTime
      /*如果过期*/
      if (expired) {
        initializing = false
        /*executorId*/
        executorIdsToBeRemoved += executorId
      }
      !expired
    }
    /*删除executorIdsToBeRemoved中的executor*/
    if (executorIdsToBeRemoved.nonEmpty) {
      removeExecutors(executorIdsToBeRemoved)
    }
  }

  /**
   * Updates our target number of executors and syncs the result with the cluster manager.
   *
   * Check to see whether our existing allocation and the requests we've made previously exceed our
   * current needs. If so, truncate our target and let the cluster manager know so that it can
   * cancel pending requests that are unneeded.
   *
   * If not, and the add time has expired, see if we can request new executors and refresh the add
   * time.
   *
   * @return the delta in the target number of executors.
   */
  /*同步更新目标Executors数*/
  private def updateAndSyncNumExecutorsTarget(now: Long): Int = synchronized {
    /*计算需要的最大Executors数*/
    val maxNeeded = maxNumExecutorsNeeded
    /*如果还在初始化，返回0【其实就是第一个job不执行此方法】*/
    if (initializing) {
      // Do not change our target while we are still initializing,
      // Otherwise the first job may have to ramp up unnecessarily
      /*当我们还在初始化时，不要改变我们的目标，否则，第一个job可能不得不毫无必要地增加*/
      0
    } else if (maxNeeded < numExecutorsTarget) {
      /*如果 实际需要的最大Executors数 < 期望目标Executors数 */
      // The target number exceeds the number we actually need, so stop adding new
      // executors and inform the cluster manager to cancel the extra pending requests
      /*调整期望目标Executors数 */
      val oldNumExecutorsTarget = numExecutorsTarget
      /*numExecutorsTarget不能小于minNumExecutors*/
      numExecutorsTarget = math.max(maxNeeded, minNumExecutors)
      numExecutorsToAdd = 1

      // If the new target has not changed, avoid sending a message to the cluster manager
      if (numExecutorsTarget < oldNumExecutorsTarget) {
        /*取消多余的executors,目前什么都没做*/
        client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
        logDebug(s"Lowering target number of executors to $numExecutorsTarget (previously " +
          s"$oldNumExecutorsTarget) because not all requested executors are actually needed")
      }
      /*新的期望Executors数 - 旧的期望Executors数，此时为负值*/
      numExecutorsTarget - oldNumExecutorsTarget
    } else if (addTime != NOT_SET && now >= addTime) {
      /*到此步说明 实际需要的最大Executors数 >= 期望目标Executors数 */
      /*添加Executors，参数为需要的最大Executors数*/
      val delta = addExecutors(maxNeeded)
      logDebug(s"Starting timer to add more executors (to " +
        s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
      addTime += sustainedSchedulerBacklogTimeoutS * 1000
      delta
    } else {
      0
    }
  }

  /**
   * Request a number of executors from the cluster manager.
   * If the cap on the number of executors is reached, give up and reset the
   * number of executors to add next round instead of continuing to double it.
   * 从集群管理器请求多个executors。
   *
   * @param maxNumExecutorsNeeded the maximum number of executors all currently running or pending
   *                              tasks could fill
   * @return the number of additional executors actually requested.
   */
  private def addExecutors(maxNumExecutorsNeeded: Int): Int = {
    /*当调用此方法时 maxNumExecutorsNeeded > numExecutorsTarget*/
    // Do not request more executors if it would put our target over the upper bound
    if (numExecutorsTarget >= maxNumExecutors) {
      logDebug(s"Not adding executors because our current target total " +
        s"is already $numExecutorsTarget (limit $maxNumExecutors)")
      numExecutorsToAdd = 1
      return 0
    }

    val oldNumExecutorsTarget = numExecutorsTarget
    // There's no point in wasting time ramping up to the number of executors we already have, so
    // make sure our target is at least as much as our current allocation:
    numExecutorsTarget = math.max(numExecutorsTarget, executorIds.size)
    // Boost our target with the number to add for this round:
    //当前numExecutorsTarget增加增长因子个
    numExecutorsTarget += numExecutorsToAdd
    // Ensure that our target doesn't exceed what we need at the present moment:
    /*确保我们的目标不会超出现在的需要，也就是数不能大于maxNumExecutorsNeeded*/
    numExecutorsTarget = math.min(numExecutorsTarget, maxNumExecutorsNeeded)
    // Ensure that our target fits within configured bounds:
    /*确保我们的目标符合配置范围:minNumExecutors和maxNumExecutors之间，超出minNumExecutors就是它，
    * 超出maxNumExecutors就是它*/
    numExecutorsTarget = math.max(math.min(numExecutorsTarget, maxNumExecutors), minNumExecutors)

    /*计算差值，即需要增加的Executors数，一定大于等于0*/
    val delta = numExecutorsTarget - oldNumExecutorsTarget

    // If our target has not changed, do not send a message
    // to the cluster manager and reset our exponential growth
    /*//如果我们的目标没有改变，就不要发送消息到集群管理器，并重置我们的增长指数为1*/
    if (delta == 0) {
      numExecutorsToAdd = 1
      return 0
    }

    val addRequestAcknowledged = testing ||
      client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
    /*添加请求被确认*/
    if (addRequestAcknowledged) {
      /*记录日志：申请delta个executor，因为任务积压*/
      val executorsString = "executor" + { if (delta > 1) "s" else "" }
      logInfo(s"Requesting $delta new $executorsString because tasks are backlogged" +
        s" (new desired total will be $numExecutorsTarget)")
      /*设置新的增长指数，
      如果差值和现在的增长指数一样，新的增长指数=现在的增长指数*2，否则为1*/
      numExecutorsToAdd = if (delta == numExecutorsToAdd) {
        numExecutorsToAdd * 2
      } else {
        1
      }
      delta
    } else {
      /*无法增加到numExecutorsTarget个executors，恢复numExecutorsTarget为old*/
      logWarning(
        s"Unable to reach the cluster manager to request $numExecutorsTarget total executors!")
      numExecutorsTarget = oldNumExecutorsTarget
      0
    }
  }

  /**
   * Request the cluster manager to remove the given executors.
   * Returns the list of executors which are removed.
   */
  /*删除executors列表中的executors
  * 返回已经删除的executors列表*/
  private def removeExecutors(executors: Seq[String]): Seq[String] = synchronized {
    /*确认要删的executors列表，从待删除列表executors 选取的 确认要删除的executors*/
    val executorIdsToBeRemoved = new ArrayBuffer[String]

    logInfo("Request to remove executorIds: " + executors.mkString(", "))
    /* 将存在的Executors数 = 所有已知的executor数 - 等待删除的Executors数（已发送删除请求，等待删除结果的） */
    val numExistingExecutors = allocationManager.executorIds.size - executorsPendingToRemove.size
    /*新Executor总数 = 将存在的Executors数*/
    var newExecutorTotal = numExistingExecutors
    /*遍历待删除executors列表 得到确认要删的executors列表，和newExecutorTotal*/
    executors.foreach { executorIdToBeRemoved =>
      /*如果 新Executor总数 =< minNumExecutors,剩余executors不足，不能删除  */
      if (newExecutorTotal - 1 < minNumExecutors) { /*为什么不这么写 newExecutorTotal =< minNumExecutors*/
        logDebug(s"Not removing idle executor $executorIdToBeRemoved because there are only " +
          s"$newExecutorTotal executor(s) left (limit $minNumExecutors)")
        /*记录日志，不能删除，因为现在剩的executors数 小于等于 最低executors数*/
      } else if (canBeKilled(executorIdToBeRemoved)) {
        /*加入确认要删的executors列表executorIdsToBeRemoved*/
        executorIdsToBeRemoved += executorIdToBeRemoved
        /*新Executor总数-1*/
        newExecutorTotal -= 1
      }
    }

    if (executorIdsToBeRemoved.isEmpty) {
      return Seq.empty[String]
    }

    // Send a request to the backend to kill this executor(s)
    /*向后端发送请求以杀死此executors*/
    val executorsRemoved = if (testing) {
      executorIdsToBeRemoved
    } else {
      /*调用CoarseGrainedSchedulerBackend的killExecutors，请求kill杀死executorIds列表里的executor*/
      client.killExecutors(executorIdsToBeRemoved)
    }
    // reset the newExecutorTotal to the existing number of executors
    /*重置newExecutorTotal = numExistingExecutors，然后遍历executorsRemoved，
    * 重新计算newExecutorTotal*/
    newExecutorTotal = numExistingExecutors
    if (testing || executorsRemoved.nonEmpty) {
      executorsRemoved.foreach { removedExecutorId =>
        newExecutorTotal -= 1
        logInfo(s"Removing executor $removedExecutorId because it has been idle for " +
          s"$executorIdleTimeoutS seconds (new desired total will be $newExecutorTotal)")
        executorsPendingToRemove.add(removedExecutorId)
      }
      executorsRemoved
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor/s " +
        "executorIdsToBeRemoved.mkString(\",\") or no executor eligible to kill!")
      Seq.empty[String]
    }
  }

  /**
   * Request the cluster manager to remove the given executor.
   * Return whether the request is acknowledged.
   */
  private def removeExecutor(executorId: String): Boolean = synchronized {
    val executorsRemoved = removeExecutors(Seq(executorId))
    executorsRemoved.nonEmpty && executorsRemoved(0) == executorId
  }

  /**
   * Determine if the given executor can be killed.
   */
  private def canBeKilled(executorId: String): Boolean = synchronized {
    // Do not kill the executor if we are not aware of it (should never happen)
    if (!executorIds.contains(executorId)) {
      logWarning(s"Attempted to remove unknown executor $executorId!")
      return false
    }

    // Do not kill the executor again if it is already pending to be killed (should never happen)
    if (executorsPendingToRemove.contains(executorId)) {
      logWarning(s"Attempted to remove executor $executorId " +
        s"when it is already pending to be removed!")
      return false
    }

    true
  }

  /**
   * Callback invoked when the specified executor has been added.
   * 当指定的executor被添加后调用此方法
   */
  private def onExecutorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      executorIds.add(executorId)
      // If an executor (call this executor X) is not removed because the lower bound
      // has been reached, it will no longer be marked as idle. When new executors join,
      // however, we are no longer at the lower bound, and so we must mark executor X
      // as idle again so as not to forget that it is a candidate for removal. (see SPARK-4951)
      /*过滤出当前没有任务运行的Executors，然后遍历，设置其过期时间
      * 就是更新removeTimes中的内容*/
      executorIds.filter(listener.isExecutorIdle).foreach(onExecutorIdle)
      logInfo(s"New executor $executorId has registered (new total is ${executorIds.size})")
    } else {
      logWarning(s"Duplicate executor $executorId has registered")
    }
  }

  /**
   * Callback invoked when the specified executor has been removed.
   */
  private def onExecutorRemoved(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      executorIds.remove(executorId)
      removeTimes.remove(executorId)
      logInfo(s"Existing executor $executorId has been removed (new total is ${executorIds.size})")
      if (executorsPendingToRemove.contains(executorId)) {
        executorsPendingToRemove.remove(executorId)
        logDebug(s"Executor $executorId is no longer pending to " +
          s"be removed (${executorsPendingToRemove.size} left)")
      }
    } else {
      logWarning(s"Unknown executor $executorId has been removed!")
    }
  }

  /**
   * Callback invoked when the scheduler receives new pending tasks.
   * This sets a time in the future that decides when executors should be added
   * if it is not already set.
   * scheduler接收到新的待处理任务时调用回调。
   * 如果设置addTime == NOT_SET，这将设置addTime添加时间（一个未来的时间），
   * 它将决定何时添加executors。
   */
  private def onSchedulerBacklogged(): Unit = synchronized {
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeoutS seconds)")
      addTime = clock.getTimeMillis + schedulerBacklogTimeoutS * 1000
    }
  }

  /**
   * Callback invoked when the scheduler queue is drained.
   * This resets all variables used for adding executors.
   */
  private def onSchedulerQueueEmpty(): Unit = synchronized {
    logDebug("Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    numExecutorsToAdd = 1
  }

  /**
   * Callback invoked when the specified executor is no longer running any tasks.
   * This sets a time in the future that decides when this executor should be removed if
   * the executor is not already marked as idle.
   * 当指定的executor不再运行任何task的时候调用。如果executor尚未标记为空闲idle，这将设置一个将来的时间点，
   * 该时间决定了何时删除此executor。
   *
   */
  private def onExecutorIdle(executorId: String): Unit = synchronized {
    /*判断executorId是否在executorIds列表中*/
    if (executorIds.contains(executorId)) {
      /*如果 不在removeTimes 和 executorsPendingToRemove 中*/
      if (!removeTimes.contains(executorId) && !executorsPendingToRemove.contains(executorId)) {
        // Note that it is not necessary to query the executors since all the cached
        // blocks we are concerned with are reported to the driver. Note that this
        // does not include broadcast blocks.
        /*指定的executor上是否有缓存的block*/
        val hasCachedBlocks = SparkEnv.get.blockManager.master.hasCachedBlocks(executorId)
        val now = clock.getTimeMillis()
        /*计算timeout（有效期）*/
        val timeout = {
          if (hasCachedBlocks) {
            /*如果有缓存的block*/
            /*timeout默认为 now + Integer.MAX_VALUE seconds，可以认为是一个无穷大的数，即一直有效 */
            // Use a different timeout if the executor has cached blocks.
            now + cachedExecutorIdleTimeoutS * 1000
          } else {
            /*now + 60s*/
            now + executorIdleTimeoutS * 1000
          }
        }
        val realTimeout = if (timeout <= 0) Long.MaxValue else timeout // overflow
        /*设置此executorId的过期时间realTimeout*/
        removeTimes(executorId) = realTimeout
        logDebug(s"Starting idle timer for $executorId because there are no more tasks " +
          s"scheduled to run on the executor (to expire in ${(realTimeout - now)/1000} seconds)")
      }
    } else {
      logWarning(s"Attempted to mark unknown executor $executorId idle")
    }
  }

  /**
   * Callback invoked when the specified executor is now running a task.
   * This resets all variables used for removing this executor.
   */
  private def onExecutorBusy(executorId: String): Unit = synchronized {
    logDebug(s"Clearing idle timer for $executorId because it is now running a task")
    removeTimes.remove(executorId)
  }

  /**
   * A listener that notifies the given allocation manager of when to add and remove executors.
   *
   * This class is intentionally conservative in its assumptions about the relative ordering
   * and consistency of events returned by the listener. For simplicity, it does not account
   * for speculated tasks.
   */
  private class ExecutorAllocationListener extends SparkListener {

    private val stageIdToNumTasks = new mutable.HashMap[Int, Int]
    private val stageIdToTaskIndices = new mutable.HashMap[Int, mutable.HashSet[Int]]
    private val executorIdToTaskIds = new mutable.HashMap[String, mutable.HashSet[Long]]
    // Number of tasks currently running on the cluster.  Should be 0 when no stages are active.
    private var numRunningTasks: Int = _

    // stageId to tuple (the number of task with locality preferences, a map where each pair is a
    // node and the number of tasks that would like to be scheduled on that node) map,
    // maintain the executor placement hints for each stage Id used by resource framework to better
    // place the executors.
    private val stageIdToExecutorPlacementHints = new mutable.HashMap[Int, (Int, Map[String, Int])]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      initializing = false
      val stageId = stageSubmitted.stageInfo.stageId
      val numTasks = stageSubmitted.stageInfo.numTasks
      allocationManager.synchronized {
        stageIdToNumTasks(stageId) = numTasks
        allocationManager.onSchedulerBacklogged()

        // Compute the number of tasks requested by the stage on each host
        var numTasksPending = 0
        val hostToLocalTaskCountPerStage = new mutable.HashMap[String, Int]()
        stageSubmitted.stageInfo.taskLocalityPreferences.foreach { locality =>
          if (!locality.isEmpty) {
            numTasksPending += 1
            locality.foreach { location =>
              val count = hostToLocalTaskCountPerStage.getOrElse(location.host, 0) + 1
              hostToLocalTaskCountPerStage(location.host) = count
            }
          }
        }
        stageIdToExecutorPlacementHints.put(stageId,
          (numTasksPending, hostToLocalTaskCountPerStage.toMap))

        // Update the executor placement hints
        updateExecutorPlacementHints()
      }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageId = stageCompleted.stageInfo.stageId
      allocationManager.synchronized {
        stageIdToNumTasks -= stageId
        stageIdToTaskIndices -= stageId
        stageIdToExecutorPlacementHints -= stageId

        // Update the executor placement hints
        updateExecutorPlacementHints()

        // If this is the last stage with pending tasks, mark the scheduler queue as empty
        // This is needed in case the stage is aborted for any reason
        if (stageIdToNumTasks.isEmpty) {
          allocationManager.onSchedulerQueueEmpty()
          if (numRunningTasks != 0) {
            logWarning("No stages are running, but numRunningTasks != 0")
            numRunningTasks = 0
          }
        }
      }
    }

    /*当task start的时候调用*/
    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val stageId = taskStart.stageId
      val taskId = taskStart.taskInfo.taskId
      val taskIndex = taskStart.taskInfo.index
      val executorId = taskStart.taskInfo.executorId

      allocationManager.synchronized {
        numRunningTasks += 1
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        if (!allocationManager.executorIds.contains(executorId)) {
          allocationManager.onExecutorAdded(executorId)
        }

        // If this is the last pending task, mark the scheduler queue as empty
        stageIdToTaskIndices.getOrElseUpdate(stageId, new mutable.HashSet[Int]) += taskIndex
        if (totalPendingTasks() == 0) {
          allocationManager.onSchedulerQueueEmpty()
        }

        // Mark the executor on which this task is scheduled as busy
        executorIdToTaskIds.getOrElseUpdate(executorId, new mutable.HashSet[Long]) += taskId
        allocationManager.onExecutorBusy(executorId)
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val executorId = taskEnd.taskInfo.executorId
      val taskId = taskEnd.taskInfo.taskId
      val taskIndex = taskEnd.taskInfo.index
      val stageId = taskEnd.stageId
      allocationManager.synchronized {
        numRunningTasks -= 1
        // If the executor is no longer running any scheduled tasks, mark it as idle
        if (executorIdToTaskIds.contains(executorId)) {
          executorIdToTaskIds(executorId) -= taskId
          if (executorIdToTaskIds(executorId).isEmpty) {
            executorIdToTaskIds -= executorId
            allocationManager.onExecutorIdle(executorId)
          }
        }

        // If the task failed, we expect it to be resubmitted later. To ensure we have
        // enough resources to run the resubmitted task, we need to mark the scheduler
        // as backlogged again if it's not already marked as such (SPARK-8366)
        if (taskEnd.reason != Success) {
          if (totalPendingTasks() == 0) {
            allocationManager.onSchedulerBacklogged()
          }
          stageIdToTaskIndices.get(stageId).foreach { _.remove(taskIndex) }
        }
      }
    }

    /*当添加Executor的时候调用*/
    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
      val executorId = executorAdded.executorId
      if (executorId != SparkContext.DRIVER_IDENTIFIER) {
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        if (!allocationManager.executorIds.contains(executorId)) {
          allocationManager.onExecutorAdded(executorId)
        }
      }
    }

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
      allocationManager.onExecutorRemoved(executorRemoved.executorId)
    }

    /**
     * An estimate of the total number of pending tasks remaining for currently running stages. Does
     * not account for tasks which may have failed and been resubmitted.
     * 对当前运行阶段的待处理任务总数的估计。不考虑可能失败并重新提交的任务。
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
     * 如果调用者没有拥有“allocationManager”锁，这就不是线程安全的。
     */
    def totalPendingTasks(): Int = {
      /*（各stage总的task数 - 正在运行的task数据）然后求和*/
      stageIdToNumTasks.map { case (stageId, numTasks) =>
        numTasks - stageIdToTaskIndices.get(stageId).map(_.size).getOrElse(0)
      }.sum
    }

    /**
     * The number of tasks currently running across all stages.
     */
    def totalRunningTasks(): Int = numRunningTasks

    /**
     * Return true if an executor is not currently running a task, and false otherwise.
     * 如果executor当前没有运行任务，则返回true，否则返回false。
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
     */
    /*返回当前executor是否空闲（没有在运行任务）*/
    def isExecutorIdle(executorId: String): Boolean = {
      !executorIdToTaskIds.contains(executorId)
    }

    /**
     * Update the Executor placement hints (the number of tasks with locality preferences,
     * a map where each pair is a node and the number of tasks that would like to be scheduled
     * on that node).
     * 更新Executor位置提示.(具有数据本地性的task数，node 和 想在此node上执行的task数 的map)
     * These hints are updated when stages arrive and complete, so are not up-to-date at task
     * granularity within stages.
     * 这些提示在stages激活或完成时更新，因此它在stages内的任务粒度不是最新的。
     * 也就是说在stages里的task执行过程中，这些信息不会即时更新。
     */
    def updateExecutorPlacementHints(): Unit = {
      var localityAwareTasks = 0
      val localityToCount = new mutable.HashMap[String, Int]()
      stageIdToExecutorPlacementHints.values.foreach { case (numTasksPending, localities) =>
        localityAwareTasks += numTasksPending
        localities.foreach { case (hostname, count) =>
          val updatedCount = localityToCount.getOrElse(hostname, 0) + count
          localityToCount(hostname) = updatedCount
        }
      }

      allocationManager.localityAwareTasks = localityAwareTasks
      allocationManager.hostToLocalTaskCount = localityToCount.toMap
    }
  }

  /**
   * Metric source for ExecutorAllocationManager to expose its internal executor allocation
   * status to MetricsSystem.
   * Note: These metrics heavily rely on the internal implementation of
   * ExecutorAllocationManager, metrics or value of metrics will be changed when internal
   * implementation is changed, so these metrics are not stable across Spark version.
   */
  private[spark] class ExecutorAllocationManagerSource extends Source {
    val sourceName = "ExecutorAllocationManager"
    val metricRegistry = new MetricRegistry()

    private def registerGauge[T](name: String, value: => T, defaultValue: T): Unit = {
      metricRegistry.register(MetricRegistry.name("executors", name), new Gauge[T] {
        override def getValue: T = synchronized { Option(value).getOrElse(defaultValue) }
      })
    }

    registerGauge("numberExecutorsToAdd", numExecutorsToAdd, 0)
    registerGauge("numberExecutorsPendingToRemove", executorsPendingToRemove.size, 0)
    registerGauge("numberAllExecutors", executorIds.size, 0)
    registerGauge("numberTargetExecutors", numExecutorsTarget, 0)
    registerGauge("numberMaxNeededExecutors", maxNumExecutorsNeeded(), 0)
  }
}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue
}
