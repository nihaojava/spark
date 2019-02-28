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

package org.apache.spark.deploy.yarn

import java.util.Collections
import java.util.concurrent._
import java.util.regex.Pattern

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef}
import org.apache.spark.scheduler.{ExecutorExited, ExecutorLossReason}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RemoveExecutor
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveLastAllocatedExecutorId
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * YarnAllocator is charged with requesting containers from the YARN ResourceManager and deciding
 * what to do with containers when YARN fulfills these requests.
 * YarnAllocator负责请求containers从YARN ResourceManager并且当YARN满足这些请求时决定如何处理containers。
 *
 *
 * This class makes use of YARN's AMRMClient APIs. We interact with the AMRMClient in three ways:
 * * Making our resource needs known, which updates local bookkeeping about containers requested.
 * * Calling "allocate", which syncs our local container requests with the RM, and returns any
 *   containers that YARN has granted to us.  This also functions as a heartbeat.
 * * Processing the containers granted to us to possibly launch executors inside of them.
 * 这个类使用了YARN的AMRMClient api。我们以三种方式与AMRMClient交互：
 * 1. 告知我们的资源需求，这将更新所请求的容器的本地簿记
 * 2. 调用“allocate”，它将本地容器请求与RM同步，并返回YARN授予我们的所有容器。这也起到心跳的作用。
 * 3. 处理授予我们可能在其中启动executors的容器。
 * The public methods of this class are thread-safe.  All methods that mutate state are
 * synchronized.
 */
private[yarn] class YarnAllocator(
    driverUrl: String,
    driverRef: RpcEndpointRef,
    conf: YarnConfiguration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest], /*RM的客户端，AM通过此客户端向RM注册、心跳*/
    appAttemptId: ApplicationAttemptId,
    securityMgr: SecurityManager,
    localResources: Map[String, LocalResource])
  extends Logging {

  import YarnAllocator._

  // RackResolver logs an INFO message whenever it resolves a rack, which is way too often.
  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  // Visible for testing.
  val allocatedHostToContainersMap = new HashMap[String, collection.mutable.Set[ContainerId]]
  val allocatedContainerToHostMap = new HashMap[ContainerId, String]

  // Containers that we no longer care about. We've either already told the RM to release them or
  // will on the next heartbeat. Containers get removed from this map after the RM tells us they've
  // completed.
  private val releasedContainers = Collections.newSetFromMap[ContainerId](
    new ConcurrentHashMap[ContainerId, java.lang.Boolean])

  @volatile private var numExecutorsRunning = 0

  /**
   * Used to generate a unique ID per executor
   *
   * Init `executorIdCounter`. when AM restart, `executorIdCounter` will reset to 0. Then
   * the id of new executor will start from 1, this will conflict with the executor has
   * already created before. So, we should initialize the `executorIdCounter` by getting
   * the max executorId from driver.
   *
   * And this situation of executorId conflict is just in yarn client mode, so this is an issue
   * in yarn client mode. For more details, can check in jira.
   *
   * @see SPARK-12864
   */
  private var executorIdCounter: Int =
    driverRef.askWithRetry[Int](RetrieveLastAllocatedExecutorId)

  // Queue to store the timestamp of failed executors
  /*队列来存储失败的executors的时间戳*/
  private val failedExecutorsTimeStamps = new Queue[Long]()

  private var clock: Clock = new SystemClock

  /*校验executor是否失败的时间间隔*/
  private val executorFailuresValidityInterval =
    sparkConf.get(EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS).getOrElse(-1L)

  /*根据是否启用动态分配获得*/
  @volatile private var targetNumExecutors =
    YarnSparkHadoopUtil.getInitialTargetExecutorNumber(sparkConf)

  /*记录当前NM的黑名单*/
  private var currentNodeBlacklist = Set.empty[String]

  // Executor loss reason requests that are pending - maps from executor ID for inquiry to a
  // list of requesters that should be responded to once we find out why the given executor
  // was lost.
  private val pendingLossReasonRequests = new HashMap[String, mutable.Buffer[RpcCallContext]]

  // Maintain loss reasons for already released executors, it will be added when executor loss
  // reason is got from AM-RM call, and be removed after querying this loss reason.
  /*存放释放的executor 和 释放原因*/
  private val releasedExecutorLossReasons = new HashMap[String, ExecutorLossReason]

  // Keep track of which container is running which executor to remove the executors later
  // Visible for testing.
  /*跟踪哪个container正在运行哪个executor，以便稍后删除executor
  * 记录executorId 和 Container之间的映射关系*/
  private[yarn] val executorIdToContainer = new HashMap[String, Container]
  /*意外释放的Container个数*/
  private var numUnexpectedContainerRelease = 0L
  /*ContainerId和executorId的映射关系*/
  private val containerIdToExecutorId = new HashMap[ContainerId, String]

  /*从sparkConf配置中读取用户设置的executor的内存和虚拟core个数*/
  // Executor memory in MB.
  /*Executor内存，如果没设置，默认为1g*/
  protected val executorMemory = sparkConf.get(EXECUTOR_MEMORY).toInt
  // Additional memory overhead.
  /*额外的内存开销，如果没设置，默认为max(executorMemory*0.1,384) */
  protected val memoryOverhead: Int = sparkConf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse(
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN)).toInt
  // Number of cores per executor.
  /*每个executor的虚拟cores，默认为1*/
  protected val executorCores = sparkConf.get(EXECUTOR_CORES)
  // Resource capability requested for each executors
  /*根据需要的申请的内存和虚拟cores，new一个Resource*/
  private[yarn] val resource = Resource.newInstance(executorMemory + memoryOverhead, executorCores)

  /*ContainerLauncher一个线程池执行器，负责要求NM启动Container*/
  private val launcherPool = ThreadUtils.newDaemonCachedThreadPool(
    "ContainerLauncher", sparkConf.get(CONTAINER_LAUNCH_MAX_THREADS))

  // For testing
  private val launchContainers = sparkConf.getBoolean("spark.yarn.launchContainers", true)

  private val labelExpression = sparkConf.get(EXECUTOR_NODE_LABEL_EXPRESSION)


  // A map to store preferred hostname and possible task numbers running on it.
  /*存储host 和 task运行在期望的host的个数 映射*/
  private var hostToLocalTaskCounts: Map[String, Int] = Map.empty

  // Number of tasks that have locality preferences in active stages
  /*在当前stage中有数据本地性的task个数 */
  private var numLocalityAwareTasks: Int = 0

  // A container placement strategy based on pending tasks' locality preference
  /*容器放置策略，根据等待的task的本地性偏好*/
  private[yarn] val containerPlacementStrategy =
    new LocalityPreferredContainerPlacementStrategy(sparkConf, conf, resource)

  /**
   * Use a different clock for YarnAllocator. This is mainly used for testing.
   */
  def setClock(newClock: Clock): Unit = {
    clock = newClock
  }

  def getNumExecutorsRunning: Int = numExecutorsRunning

  /*获取失败的Executors个数*/
  def getNumExecutorsFailed: Int = synchronized {
    val endTime = clock.getTimeMillis()

    /*executorFailuresValidityInterval默认为-1，不会执行while循环*/
    while (executorFailuresValidityInterval > 0
      && failedExecutorsTimeStamps.nonEmpty
      && failedExecutorsTimeStamps.head < endTime - executorFailuresValidityInterval) {
      failedExecutorsTimeStamps.dequeue()
    }
    /*失败的Executors个数*/
    failedExecutorsTimeStamps.size
  }

  /**
   * A sequence of pending container requests that have not yet been fulfilled.
   * 获取ANY_HOST=*，也就是所有，尚未完成分配的等待中的容器请求序列。
   */
  def getPendingAllocate: Seq[ContainerRequest] = getPendingAtLocation(ANY_HOST)

  /**
   * A sequence of pending container requests at the given location that have not yet been
   * fulfilled.
   */
  /*根据期望资源位置location,获取尚未完成分配的等待中的容器请求序列。
  【在通过RMClient向RM发送资源申请的时候都会记录到RMClient
  中对应的数据结构remoteRequestsTable中，收到分配的ContainerRequest就会从中移除】*/
  private def getPendingAtLocation(location: String): Seq[ContainerRequest] = {
    amClient.getMatchingRequests(RM_REQUEST_PRIORITY, location, resource).asScala
      .flatMap(_.asScala)
      .toSeq
  }

  /**
   * Request as many executors from the ResourceManager as needed to reach the desired total. If
   * the requested total is smaller than the current number of running executors, no executors will
   * be killed.
   * 根据需要向ResourceManager请求尽可能多的executors，以达到所需的总数。
   * 如果请求的总数小于正在运行的executors的当前数量，则不会杀死任何executors。
   *
   * @param requestedTotal total number of containers requested
   * @param localityAwareTasks number of locality aware tasks to be used as container placement hint
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
   * @param nodeBlacklist a set of blacklisted nodes, which is passed in to avoid allocating new
    *                      containers on them. It will be used to update the application master's
    *                      blacklist.
   * @return Whether the new requested total is different than the old value.
   */
  def requestTotalExecutorsWithPreferredLocalities(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int],
      nodeBlacklist: Set[String]): Boolean = synchronized {
    this.numLocalityAwareTasks = localityAwareTasks
    this.hostToLocalTaskCounts = hostToLocalTaskCount

    /*如果Driver申请的Executor数量 和 目标Executor数量不一致，更新targetNumExecutors，返回ture*/
    if (requestedTotal != targetNumExecutors) {
      logInfo(s"Driver requested a total number of $requestedTotal executor(s).")
      targetNumExecutors = requestedTotal

      // Update blacklist infomation to YARN ResouceManager for this application,
      // in order to avoid allocating new Containers on the problematic nodes.
      /*为这个应用程序更新黑名单信息到YARN资源管理器，以避免在有问题的节点上分配新的容器。*/
      val blacklistAdditions = nodeBlacklist -- currentNodeBlacklist
      val blacklistRemovals = currentNodeBlacklist -- nodeBlacklist
      if (blacklistAdditions.nonEmpty) {
        logInfo(s"adding nodes to YARN application master's blacklist: $blacklistAdditions")
      }
      if (blacklistRemovals.nonEmpty) {
        logInfo(s"removing nodes from YARN application master's blacklist: $blacklistRemovals")
      }
      amClient.updateBlacklist(blacklistAdditions.toList.asJava, blacklistRemovals.toList.asJava)
      currentNodeBlacklist = nodeBlacklist
      true
    } else {
      false
    }
  }

  /**
   * Request that the ResourceManager release the container running the specified executor.
   * 请求ResourceManager释放运行指定executor的容器。
   */
  def killExecutor(executorId: String): Unit = synchronized {
    if (executorIdToContainer.contains(executorId)) {
      /*获取executorId对应的ContainerId*/
      val container = executorIdToContainer.get(executorId).get
      internalReleaseContainer(container)
      numExecutorsRunning -= 1
    } else {
      logWarning(s"Attempted to kill unknown executor $executorId!")
    }
  }

  /**
   * Request resources such that, if YARN gives us all we ask for, we'll have a number of containers
   * equal to maxExecutors.
   * 请求资源，这样，如果YARN满足了我们的所有要求，我们将有多个容器相当于maxexecutor。
   *
   * Deal with any containers YARN has granted to us by possibly launching executors in them.
   * 处理任何YARN已批给我们的containers，可能会在其中启动executors
   *
   * This must be synchronized because variables read in this method are mutated by other methods.
   * 这必须同步，因为在这个方法中读取的变量会被其他方法改变。
   */
  /*请求资源*/
  def allocateResources(): Unit = synchronized {
    /*更新资源请求，会先更新AMRMClietn中的发送列表，*/
    updateResourceRequests()
    /*进展指标*/
    val progressIndicator = 0.1f
    // Poll the ResourceManager. This doubles as a heartbeat if there are no pending container
    // requests.
    /*poll方式和ResourceManager通信。如果没有等待的容器请求，它将作为心跳*/
    val allocateResponse = amClient.allocate(progressIndicator)
    /*从响应中获取RM给分配的Containers*/
    val allocatedContainers = allocateResponse.getAllocatedContainers()

    /*如果申请到的Containers个数大于0*/
    if (allocatedContainers.size > 0) {
      logDebug("Allocated containers: %d. Current executor count: %d. Cluster resources: %s."
        .format(
          allocatedContainers.size,
          numExecutorsRunning,
          allocateResponse.getAvailableResources))
      /*处理分配到的Containers，启动executor*/
      /*【这个此方法的核心】*/
      handleAllocatedContainers(allocatedContainers.asScala)
    }
    /*从心跳应答中获取 完成的container列表*/
    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))
      /*处理*/
      processCompletedContainers(completedContainers.asScala)
      logDebug("Finished processing %d completed containers. Current running executor count: %d."
        .format(completedContainers.size, numExecutorsRunning))
    }
  }

  /**
   * Update the set of container requests that we will sync with the RM based on the number of
   * executors we have currently running and our target number of executors.
   * 根据当前运行的executors的数量和目标executors的数量，更新将与RM同步的容器请求集。
   * Visible for testing.
   */
  /*【更新将向RM发送的资源请求，心跳会发送这些请求】*/
  def updateResourceRequests(): Unit = {
    /*获取所有尚未完成分配的等待中的容器请求列表【已发送给RM，但RM还未为其分配资源】*/
    val pendingAllocate = getPendingAllocate
    /*等待中的请求个数*/
    val numPendingAllocate = pendingAllocate.size
    /*缺少的Executor个数 = 目标executor个数 - 等待分配中的请求个数 - 正在运行的Executor个数*/
    val missing = targetNumExecutors - numPendingAllocate - numExecutorsRunning

    /*还需申请的Executor个数*/
    if (missing > 0) {
      logInfo(s"Will request $missing executor container(s), each with " +
        s"${resource.getVirtualCores} core(s) and " +
        s"${resource.getMemory} MB memory (including $memoryOverhead MB of overhead)")

      // Split the pending container request into three groups: locality matched list, locality
      // unmatched list and non-locality list. Take the locality matched container request into
      // consideration of container placement, treat as allocated containers.
      // For locality unmatched and locality free container requests, cancel these container
      // requests, since required locality preference has been changed, recalculating using
      // container placement strategy.
      /*将pendingAllocate分成三组
      * 1.container requests 中指定的node/host节点 和hostToLocalTaskCounts中的host有交集
      * 2.没有交集
      * 3.container requests 中没有指定的node/host节点*/
      val (localRequests, staleRequests, anyHostRequests) = splitPendingAllocationsByLocality(
        hostToLocalTaskCounts, pendingAllocate)

      // cancel "stale" requests for locations that are no longer needed
      /*对没有交集的请求，取消请求*/
      staleRequests.foreach { stale =>
        amClient.removeContainerRequest(stale)
      }
      val cancelledContainers = staleRequests.size
      if (cancelledContainers > 0) {
        logInfo(s"Canceled $cancelledContainers container request(s) (locality no longer needed)")
      }

      // consider the number of new containers and cancelled stale containers available
      /*新容器的数量为 missing+cancelledContainers*/
      val availableContainers = missing + cancelledContainers

      // to maximize locality, include requests with no locality preference that can be cancelled
      val potentialContainers = availableContainers + anyHostRequests.size
      /*根据containerPlacementStrategy(Container放置策略，目标最大化task的数据本地性)*/
      val containerLocalityPreferences = containerPlacementStrategy.localityOfRequestedContainers(
        potentialContainers, numLocalityAwareTasks, hostToLocalTaskCounts,
          allocatedHostToContainersMap, localRequests)

      val newLocalityRequests = new mutable.ArrayBuffer[ContainerRequest]
      containerLocalityPreferences.foreach {
        case ContainerLocalityPreferences(nodes, racks) if nodes != null =>
          newLocalityRequests += createContainerRequest(resource, nodes, racks)
        case _ =>
      }

      if (availableContainers >= newLocalityRequests.size) {
        // more containers are available than needed for locality, fill in requests for any host
        for (i <- 0 until (availableContainers - newLocalityRequests.size)) {
          newLocalityRequests += createContainerRequest(resource, null, null)
        }
      } else {
        val numToCancel = newLocalityRequests.size - availableContainers
        // cancel some requests without locality preferences to schedule more local containers
        anyHostRequests.slice(0, numToCancel).foreach { nonLocal =>
          amClient.removeContainerRequest(nonLocal)
        }
        if (numToCancel > 0) {
          logInfo(s"Canceled $numToCancel unlocalized container requests to resubmit with locality")
        }
      }

      newLocalityRequests.foreach { request =>
        /*在amClient的ContainerRequest列表【包含多个ContainerRequest，
        一次发送多个ContainerRequest】添加ContainerRequest*/
        amClient.addContainerRequest(request)
      }

      if (log.isInfoEnabled()) {
        val (localized, anyHost) = newLocalityRequests.partition(_.getNodes() != null)
        if (anyHost.nonEmpty) {
          logInfo(s"Submitted ${anyHost.size} unlocalized container requests.")
        }
        localized.foreach { request =>
          logInfo(s"Submitted container request for host ${hostStr(request)}.")
        }
      }
    } else if (numPendingAllocate > 0 && missing < 0) {
      val numToCancel = math.min(numPendingAllocate, -missing)
      logInfo(s"Canceling requests for $numToCancel executor container(s) to have a new desired " +
        s"total $targetNumExecutors executors.")

      val matchingRequests = amClient.getMatchingRequests(RM_REQUEST_PRIORITY, ANY_HOST, resource)
      if (!matchingRequests.isEmpty) {
        matchingRequests.iterator().next().asScala
          .take(numToCancel).foreach(amClient.removeContainerRequest)
      } else {
        logWarning("Expected to find pending requests, but found none.")
      }
    }
  }

  private def hostStr(request: ContainerRequest): String = {
    Option(request.getNodes) match {
      case Some(nodes) => nodes.asScala.mkString(",")
      case None => "Any"
    }
  }

  /**
   * Creates a container request, handling the reflection required to use YARN features that were
   * added in recent versions.
   */
  /*创建ContainerRequest消息*/
  private def createContainerRequest(
      resource: Resource,
      nodes: Array[String],
      racks: Array[String]): ContainerRequest = {
    new ContainerRequest(resource, nodes, racks, RM_REQUEST_PRIORITY, true, labelExpression.orNull)
  }

  /**
   * Handle containers granted by the RM by launching executors on them.
   * 通过在RM上启动executors来处理RM授予的容器。
   * Due to the way the YARN allocation protocol works, certain healthy race conditions can result
   * in YARN granting containers that we no longer need. In this case, we release them.
   * 由于YARN分配协议的工作方式，某些健康的竞态条件会导致YARN供应容器我们不再需要。
   * 在这种情况下，我们释放它们。
   *
   * Visible for testing.
   */
  /*处理分配到的Container*/
  def handleAllocatedContainers(allocatedContainers: Seq[Container]): Unit = {
    val containersToUse = new ArrayBuffer[Container](allocatedContainers.size)
    /*----------------start 这部分是在校验分配到的资源 和 请求的资源是否匹配，匹配不上的释放*/
    // Match incoming requests by host
    /*遍历分配到的allocatedContainers列表，
    拿allocatedContainer 和在allocatedContainers所在host上的container请求 匹配，
    * 如果匹配上放入containersToUse，匹配不上放入remainingAfterHostMatches*/
    val remainingAfterHostMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- allocatedContainers) {
      matchContainerToRequest(allocatedContainer, allocatedContainer.getNodeId.getHost,
        containersToUse, remainingAfterHostMatches)
    }

    // Match remaining by rack
    /*拿allocatedContainer 和在allocatedContainers所在rack上的container请求 匹配，
    * 如果匹配上放入containersToUse，匹配不上放入remainingAfterRackMatches*/
    val remainingAfterRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterHostMatches) {
      val rack = RackResolver.resolve(conf, allocatedContainer.getNodeId.getHost).getNetworkLocation
      matchContainerToRequest(allocatedContainer, rack, containersToUse,
        remainingAfterRackMatches)
    }

    // Assign remaining that are neither node-local nor rack-local
    /*拿allocatedContainer 和ANY_HOST请求 匹配，
    * 如果匹配上放入containersToUse，匹配不上放入remainingAfterOffRackMatches*/
    val remainingAfterOffRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterRackMatches) {
      matchContainerToRequest(allocatedContainer, ANY_HOST, containersToUse,
        remainingAfterOffRackMatches)
    }
    /*如果remainingAfterOffRackMatches不为空，释放其中的container*/
    if (!remainingAfterOffRackMatches.isEmpty) {
      logDebug(s"Releasing ${remainingAfterOffRackMatches.size} unneeded containers that were " +
        s"allocated to us")
      for (container <- remainingAfterOffRackMatches) {
        internalReleaseContainer(container)
      }
    }
    /*----------------end*/
    /*【这是该方法的核心，在指派的containers中启动executors】*/
    runAllocatedContainers(containersToUse)

    /*记录日志，收到yarn分配的Containers个数，启动executors个数
    * 【由此可见container和executor是1比1,1个container对应1个executor】*/
    logInfo("Received %d containers from YARN, launching executors on %d of them."
      .format(allocatedContainers.size, containersToUse.size))
  }

  /**
   * Looks for requests for the given location that match the given container allocation. If it
   * finds one, removes the request so that it won't be submitted again. Places the container into
   * containersToUse or remaining.
   * 查找与给定容器分配匹配的给定位置的请求。
   * 如果找到一个，删除请求，这样就不会再次提交了。将容器放入货柜箱内或剩余。
   * @param allocatedContainer container that was given to us by YARN
   * @param location resource name, either a node, rack, or *   资源名称
   * @param containersToUse list of containers that will be used  将被使用的containers
   * @param remaining list of containers that will not be used  剩余的containers
   */
  /*分配到的Container匹配请求的*/
  private def matchContainerToRequest(
      allocatedContainer: Container,  /*分配到的container*/
      location: String,   /*resourceName，分配到Container所在的host或rack*/
      containersToUse: ArrayBuffer[Container],  /*存放帅选出的可以使用的Container列表*/
      remaining: ArrayBuffer[Container]): Unit = {  /*剩下的Container列表*/
    // SPARK-6050: certain Yarn configurations return a virtual core count that doesn't match the
    // request; for example, capacity scheduler + DefaultResourceCalculator. So match on requested
    // memory, but use the asked vcore count for matching, effectively disabling matching on vcore
    // count.
    /*allocatedContainer对应的资源(memory和core个数)*/
    val matchingResource = Resource.newInstance(allocatedContainer.getResource.getMemory,
          resource.getVirtualCores)
    /*分配到的资源 匹配上的申请资源的请求 */
    val matchingRequests = amClient.getMatchingRequests(allocatedContainer.getPriority, location,
      matchingResource)

    // Match the allocation to a request
    if (!matchingRequests.isEmpty) {
      /*匹配上一个*/
      val containerRequest = matchingRequests.get(0).iterator.next
      /*从amClient的ContainerRequest列表删除*/
      amClient.removeContainerRequest(containerRequest)
      containersToUse += allocatedContainer
    } else {
      /*没有匹配上*/
      remaining += allocatedContainer
    }
  }

  /**
   * Launches executors in the allocated containers.
   * 在指派的containers中启动executors
   */
  private def runAllocatedContainers(containersToUse: ArrayBuffer[Container]): Unit = {
    for (container <- containersToUse) {
      /*executorId+1*/
      executorIdCounter += 1
      /*cotainer所在host*/
      val executorHostname = container.getNodeId.getHost
      /*containerId*/
      val containerId = container.getId
      /*executorId*/
      val executorId = executorIdCounter.toString
      assert(container.getResource.getMemory >= resource.getMemory)
      logInfo(s"Launching container $containerId on host $executorHostname")

      def updateInternalState(): Unit = synchronized {
        numExecutorsRunning += 1
        executorIdToContainer(executorId) = container
        containerIdToExecutorId(container.getId) = executorId

        val containerSet = allocatedHostToContainersMap.getOrElseUpdate(executorHostname,
          new HashSet[ContainerId])
        containerSet += containerId
        allocatedContainerToHostMap.put(containerId, executorHostname)
      }
      /*正在运行的Executors小于 targetNumExecutors*/
      if (numExecutorsRunning < targetNumExecutors) {
        if (launchContainers) {
          /*向launcherPool（类似ContainerLauncher）提交一个任务（new一个ExecutorRunnable，执行run方法）*/
          launcherPool.execute(new Runnable {
            override def run(): Unit = {
              try {
                /*new一个ExecutorRunnable*/
                new ExecutorRunnable(
                  Some(container),
                  conf,
                  sparkConf,
                  driverUrl,
                  executorId,
                  executorHostname,
                  executorMemory,
                  executorCores,
                  appAttemptId.getApplicationId.toString,
                  securityMgr,
                  localResources
                ).run()
                updateInternalState()
              } catch {
                case NonFatal(e) =>
                  logError(s"Failed to launch executor $executorId on container $containerId", e)
                  // Assigned container should be released immediately to avoid unnecessary resource
                  // occupation.
                  amClient.releaseAssignedContainer(containerId)
              }
            }
          })
        } else {
          // For test only
          updateInternalState()
        }
        /*如果正在运行的executor已经达到targetNumExecutors，不做任何操作，只记录日志*/
      } else {
        logInfo(("Skip launching executorRunnable as runnning Excecutors count: %d " +
          "reached target Executors count: %d.").format(numExecutorsRunning, targetNumExecutors))
      }
    }
  }

  // Visible for testing.
  /*处理*/
  private[yarn] def processCompletedContainers(completedContainers: Seq[ContainerStatus]): Unit = {
    for (completedContainer <- completedContainers) {
      val containerId = completedContainer.getContainerId
      val alreadyReleased = releasedContainers.remove(containerId)
      val hostOpt = allocatedContainerToHostMap.get(containerId)
      val onHostStr = hostOpt.map(host => s" on host: $host").getOrElse("")
      val exitReason = if (!alreadyReleased) {
        // Decrement the number of executors running. The next iteration of
        // the ApplicationMaster's reporting thread will take care of allocating.
        numExecutorsRunning -= 1
        logInfo("Completed container %s%s (state: %s, exit status: %s)".format(
          containerId,
          onHostStr,
          completedContainer.getState,
          completedContainer.getExitStatus))
        // Hadoop 2.2.X added a ContainerExitStatus we should switch to use
        // there are some exit status' we shouldn't necessarily count against us, but for
        // now I think its ok as none of the containers are expected to exit.
        val exitStatus = completedContainer.getExitStatus
        val (exitCausedByApp, containerExitReason) = exitStatus match {
          case ContainerExitStatus.SUCCESS =>
            (false, s"Executor for container $containerId exited because of a YARN event (e.g., " +
              "pre-emption) and not because of an error in the running job.")
          case ContainerExitStatus.PREEMPTED =>
            // Preemption is not the fault of the running tasks, since YARN preempts containers
            // merely to do resource sharing, and tasks that fail due to preempted executors could
            // just as easily finish on any other executor. See SPARK-8167.
            (false, s"Container ${containerId}${onHostStr} was preempted.")
          // Should probably still count memory exceeded exit codes towards task failures
          case VMEM_EXCEEDED_EXIT_CODE =>
            (true, memLimitExceededLogMessage(
              completedContainer.getDiagnostics,
              VMEM_EXCEEDED_PATTERN))
          case PMEM_EXCEEDED_EXIT_CODE =>
            (true, memLimitExceededLogMessage(
              completedContainer.getDiagnostics,
              PMEM_EXCEEDED_PATTERN))
          case _ =>
            // Enqueue the timestamp of failed executor
            failedExecutorsTimeStamps.enqueue(clock.getTimeMillis())
            (true, "Container marked as failed: " + containerId + onHostStr +
              ". Exit status: " + completedContainer.getExitStatus +
              ". Diagnostics: " + completedContainer.getDiagnostics)

        }
        if (exitCausedByApp) {
          logWarning(containerExitReason)
        } else {
          logInfo(containerExitReason)
        }
        ExecutorExited(exitStatus, exitCausedByApp, containerExitReason)
      } else {
        // If we have already released this container, then it must mean
        // that the driver has explicitly requested it to be killed
        ExecutorExited(completedContainer.getExitStatus, exitCausedByApp = false,
          s"Container $containerId exited from explicit termination request.")
      }

      for {
        host <- hostOpt
        containerSet <- allocatedHostToContainersMap.get(host)
      } {
        containerSet.remove(containerId)
        if (containerSet.isEmpty) {
          allocatedHostToContainersMap.remove(host)
        } else {
          allocatedHostToContainersMap.update(host, containerSet)
        }

        allocatedContainerToHostMap.remove(containerId)
      }

      containerIdToExecutorId.remove(containerId).foreach { eid =>
        executorIdToContainer.remove(eid)
        pendingLossReasonRequests.remove(eid) match {
          case Some(pendingRequests) =>
            // Notify application of executor loss reasons so it can decide whether it should abort
            pendingRequests.foreach(_.reply(exitReason))

          case None =>
            // We cannot find executor for pending reasons. This is because completed container
            // is processed before querying pending result. We should store it for later query.
            // This is usually happened when explicitly killing a container, the result will be
            // returned in one AM-RM communication. So query RPC will be later than this completed
            // container process.
            releasedExecutorLossReasons.put(eid, exitReason)
        }
        if (!alreadyReleased) {
          // The executor could have gone away (like no route to host, node failure, etc)
          // Notify backend about the failure of the executor
          numUnexpectedContainerRelease += 1
          driverRef.send(RemoveExecutor(eid, exitReason))
        }
      }
    }
  }

  /**
   * Register that some RpcCallContext has asked the AM why the executor was lost. Note that
   * we can only find the loss reason to send back in the next call to allocateResources().
   */
  private[yarn] def enqueueGetLossReasonRequest(
      eid: String,
      context: RpcCallContext): Unit = synchronized {
    if (executorIdToContainer.contains(eid)) {
      pendingLossReasonRequests
        .getOrElseUpdate(eid, new ArrayBuffer[RpcCallContext]) += context
    } else if (releasedExecutorLossReasons.contains(eid)) {
      // Executor is already released explicitly before getting the loss reason, so directly send
      // the pre-stored lost reason
      context.reply(releasedExecutorLossReasons.remove(eid).get)
    } else {
      logWarning(s"Tried to get the loss reason for non-existent executor $eid")
      context.sendFailure(
        new SparkException(s"Fail to find loss reason for non-existent executor $eid"))
    }
  }

  /*释放Container*/
  private def internalReleaseContainer(container: Container): Unit = {
    releasedContainers.add(container.getId())
    /*调用RMClient的releaseAssignedContainer释放指定的container*/
    amClient.releaseAssignedContainer(container.getId())
  }

  private[yarn] def getNumUnexpectedContainerRelease = numUnexpectedContainerRelease

  private[yarn] def getNumPendingLossReasonRequests: Int = synchronized {
    pendingLossReasonRequests.size
  }

  /**
   * Split the pending container requests into 3 groups based on current localities of pending
   * tasks.
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
   * @param pendingAllocations A sequence of pending allocation container request.
   * @return A tuple of 3 sequences, first is a sequence of locality matched container
   *         requests, second is a sequence of locality unmatched container requests, and third is a
   *         sequence of locality free container requests.
   */
  /*根据host本地性将pendingAllocations分成三组（container requests满足本地性，不满足，无本地性要求），*/
  private def splitPendingAllocationsByLocality(
      hostToLocalTaskCount: Map[String, Int],
      pendingAllocations: Seq[ContainerRequest]
    ): (Seq[ContainerRequest], Seq[ContainerRequest], Seq[ContainerRequest]) = {
    val localityMatched = ArrayBuffer[ContainerRequest]()
    val localityUnMatched = ArrayBuffer[ContainerRequest]()
    val localityFree = ArrayBuffer[ContainerRequest]()

    val preferredHosts = hostToLocalTaskCount.keySet
    pendingAllocations.foreach { cr =>
      val nodes = cr.getNodes
      if (nodes == null) {
        localityFree += cr
      } else if (nodes.asScala.toSet.intersect(preferredHosts).nonEmpty) {
        localityMatched += cr
      } else {
        localityUnMatched += cr
      }
    }

    (localityMatched.toSeq, localityUnMatched.toSeq, localityFree.toSeq)
  }

}

private object YarnAllocator {
  val MEM_REGEX = "[0-9.]+ [KMG]B"
  val PMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX physical memory used")
  val VMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX virtual memory used")
  val VMEM_EXCEEDED_EXIT_CODE = -103
  val PMEM_EXCEEDED_EXIT_CODE = -104

  def memLimitExceededLogMessage(diagnostics: String, pattern: Pattern): String = {
    val matcher = pattern.matcher(diagnostics)
    val diag = if (matcher.find()) " " + matcher.group() + "." else ""
    ("Container killed by YARN for exceeding memory limits." + diag
      + " Consider boosting spark.yarn.executor.memoryOverhead.")
  }
}
