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

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable
import scala.concurrent.Future

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._

/**
 * A heartbeat from executors to the driver. This is a shared message used by several internal
 * components to convey liveness or execution information for in-progress tasks. It will also
 * expire the hosts that have not heartbeated for more than spark.network.timeout.
 * spark.executor.heartbeatInterval should be significantly less than spark.network.timeout.
 */
private[spark] case class Heartbeat(
    executorId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])], // taskId -> accumulator updates
    blockManagerId: BlockManagerId)

/**
 * An event that SparkContext uses to notify HeartbeatReceiver that SparkContext.taskScheduler is
 * created.
 * 一个事件，SparkContext使用它通知HeartbeatReceiver SparkContext.taskScheduler已经创建了
 */
private[spark] case object TaskSchedulerIsSet
/*检查超时Host的事件*/
private[spark] case object ExpireDeadHosts

private case class ExecutorRegistered(executorId: String)

private case class ExecutorRemoved(executorId: String)

private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * Lives in the driver to receive heartbeats from executors..
 * 运行在driver端，用来接收来自executors的心跳消息
 */
/*既继承了SparkListener是一个监听器，又实现了ThreadSafeRpcEndpoint是一个接收消息端*/
private[spark] class HeartbeatReceiver(sc: SparkContext, clock: Clock)
  extends SparkListener with ThreadSafeRpcEndpoint with Logging {
  //clock:对System.currentTimeMills()的封装，用于获取当前毫秒时间

  def this(sc: SparkContext) {
    this(sc, new SystemClock)
  }

  /*将HeartbeatReceiver添加到事件总线*/
  sc.addSparkListener(this)

  override val rpcEnv: RpcEnv = sc.env.rpcEnv

  private[spark] var scheduler: TaskScheduler = null

  // executor ID -> timestamp of when the last heartbeat from this executor was received
  /*executor ID 和 最后一次收到该Executor的时间戳 映射*/
  private val executorLastSeen = new mutable.HashMap[String, Long]

  // "spark.network.timeout" uses "seconds", while `spark.storage.blockManagerSlaveTimeoutMs` uses
  // "milliseconds"
  /*Executor上blockManager的超时时间，默认为120s*/
  private val slaveTimeoutMs =
    sc.conf.getTimeAsMs("spark.storage.blockManagerSlaveTimeoutMs", "120s")
  /*Executor超时时间，单位为ms，默认为120s*/
  private val executorTimeoutMs =
    sc.conf.getTimeAsSeconds("spark.network.timeout", s"${slaveTimeoutMs}ms") * 1000

  // "spark.network.timeoutInterval" uses "seconds", while
  // "spark.storage.blockManagerTimeoutIntervalMs" uses "milliseconds"
  /*Executor上blockManager超时的间隔，默认为60s*/
  private val timeoutIntervalMs =
    sc.conf.getTimeAsMs("spark.storage.blockManagerTimeoutIntervalMs", "60s")
  /*检查超时的间隔，默认为60s*/
  private val checkTimeoutIntervalMs =
    sc.conf.getTimeAsSeconds("spark.network.timeoutInterval", s"${timeoutIntervalMs}ms") * 1000

  /*向eventLoopThread提交执行超时检查的定时任务后返回的Future*/
  private var timeoutCheckingTask: ScheduledFuture[_] = null

  // "eventLoopThread" is used to run some pretty fast actions. The actions running in it should not
  // block the thread for a long time.
  /*“eventLoopThread”用于运行一些非常快的动作。其中运行的操作不应该长时间阻塞线程。*/
  /*用于执行心跳接收器的超时检查任务，只包含一个线程，线程名称为"heartbeat-receiver-event-loop-thread"*/
  private val eventLoopThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread")

  /*用于杀死Executor的一个线程，主要用于杀死超时的Executor*/
  private val killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")

  /*HeartBeatReceiver启动的时候执行*/
  override def onStart(): Unit = {
    /*创建定时调度，每个60s向自己HeartBeatReceiver的Inbox发送消息ExpireDeadHosts*/
    timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        Option(self).foreach(_.ask[Boolean](ExpireDeadHosts))
      }
    }, 0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  /*处理收到的请求*/
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    // Messages sent and received locally
    /*本地发送和接收的消息*/
    case ExecutorRegistered(executorId) =>
      /*处理注册Executor的请求*/
      /*记录executorId最后一次见面时间*/
      executorLastSeen(executorId) = clock.getTimeMillis()
      context.reply(true)
      /*删除Executor*/
    case ExecutorRemoved(executorId) =>
      executorLastSeen.remove(executorId)
      context.reply(true)
      /*TaskScheduler已经设置，赋值给scheduler*/
    case TaskSchedulerIsSet =>
      scheduler = sc.taskScheduler
      context.reply(true)
      /*检查超时的Executor*/
    case ExpireDeadHosts =>
      expireDeadHosts()
      context.reply(true)

    // Messages received from executors
      /*从executors接收到的心跳Heartbeat消息*/
    case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId) =>
      /*如果此时TaskScheduler不为空*/
      if (scheduler != null) {
        /*当前executorLastSeen包含发送心跳的Executor，也就是说此Executor注册过*/
        if (executorLastSeen.contains(executorId)) {
          /*跟新此Executor的最后一次见面时间*/
          executorLastSeen(executorId) = clock.getTimeMillis()
          /**/
          eventLoopThread.submit(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, accumUpdates, blockManagerId)
              val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
              context.reply(response)
            }
          })
        } else {
          /*当前executorLastSeen不包含发送心跳的Executor，也就是说此Executor还没注册过*/
          // This may happen if we get an executor's in-flight heartbeat immediately
          // after we just removed it. It's not really an error condition so we should
          // not log warning here. Otherwise there may be a lot of noise especially if
          // we explicitly remove executors (SPARK-4134).
          /*如果我们在移除heartbeat后立即获得heartbeat的在删除前发送的心跳，就可能发生这种情况。
          这不是一个真正的错误条件，所以我们不应该在这里记录警告。
          否则，可能会有很多噪音，特别是如果我们显式删除executor (SPARK-4134)。*/
          logDebug(s"Received heartbeat from unknown executor $executorId")
          /*要求executor再次注册自己*/
          context.reply(HeartbeatResponse(reregisterBlockManager = true))
        }
      } else {
        /*如果此时TaskScheduler为空*/
        // Because Executor will sleep several seconds before sending the first "Heartbeat", this
        // case rarely happens. However, if it really happens, log it and ask the executor to
        // register itself again.
        /*因为Executor在发送第一个“心跳”之前会休眠几秒钟，所以这种情况很少发生。
        * 但是，如果它真的发生了，请将其记录下来，并要求executor再次注册自己。*/
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager = true))
      }
  }

  /**
   * Send ExecutorRegistered to the event loop to add a new executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  /*向HeartBeatReceiver自己发送ExecutorRegistered消息*/
  def addExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor registrations.
   */
  /*注册Executor*/
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    addExecutor(executorAdded.executorId)
  }

  /**
   * Send ExecutorRemoved to the event loop to remove an executor. Only for test.
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
   */
  def removeExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor removals so it doesn't
   * log superfluous errors.
   *
   * Note that we must do this after the executor is actually removed to guard against the
   * following race condition: if we remove an executor's metadata from our data structure
   * prematurely, we may get an in-flight heartbeat from the executor before the executor is
   * actually removed, in which case we will still mark the executor as a dead host later
   * and expire it with loud error messages.
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    removeExecutor(executorRemoved.executorId)
  }

  /*检查超时的Executor*/
  /*遍历executorLastSeen中每个Executor的最后一次见面时间，和当前时间做差，如果大于executorTimeoutMs
  * （120s）就认为它超时了*/
  private def expireDeadHosts(): Unit = {
    logTrace("Checking for hosts with no recent heartbeats in HeartbeatReceiver.")
    val now = clock.getTimeMillis()
    for ((executorId, lastSeenMs) <- executorLastSeen) {
      if (now - lastSeenMs > executorTimeoutMs) {
        logWarning(s"Removing executor $executorId with no recent heartbeats: " +
          s"${now - lastSeenMs} ms exceeds timeout $executorTimeoutMs ms")
        /*移除丢失的executor并对executor上正在运行的task重新分配资源后调度*/
        scheduler.executorLost(executorId, SlaveLost("Executor heartbeat " +
          s"timed out after ${now - lastSeenMs} ms"))
          // Asynchronously kill the executor to avoid blocking the current thread
        /*异步杀死避免阻塞当前线程*/
        /*向killExecutorThread线程提交杀死Executor的任务*/
        killExecutorThread.submit(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // Note: we want to get an executor back after expiring this one,
            // so do not simply call `sc.killExecutor` here (SPARK-8119)
            /*注:我们希望在此executor超时后能重新获得一个后背executor，
            所以不要简单地调用sc.killExecutor见(SPARK-8119)*/
            sc.killAndReplaceExecutor(executorId)
          }
        })
        /*从executorLastSeen移除executorId*/
        executorLastSeen.remove(executorId)
      }
    }
  }

  override def onStop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    eventLoopThread.shutdownNow()
    killExecutorThread.shutdownNow()
  }
}


private[spark] object HeartbeatReceiver {
  val ENDPOINT_NAME = "HeartbeatReceiver"
}
