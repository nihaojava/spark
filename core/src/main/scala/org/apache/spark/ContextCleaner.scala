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

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, ThreadUtils, Utils}

/**
 * Classes that represent cleaning tasks.
 * 清理任务
 */
private sealed trait CleanupTask
private case class CleanRDD(rddId: Int) extends CleanupTask
private case class CleanShuffle(shuffleId: Int) extends CleanupTask
private case class CleanBroadcast(broadcastId: Long) extends CleanupTask
private case class CleanAccum(accId: Long) extends CleanupTask
private case class CleanCheckpoint(rddId: Int) extends CleanupTask

/**
 * A WeakReference associated with a CleanupTask.
 *
 *
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
 * 当referent对象变得只能弱可及时，相应的CleanupTaskWeakReference会自动添加到给定的引用队列中。
 */
/*【当referent对象只有此弱引用对象CleanupTaskWeakReference引用时（弱可达），referent对象会被gc回收掉，
 * 同时将它的弱引用对象CleanupTaskWeakReference放入关联的referenceQueue中】*/
/*可以参考这篇文章https://www.jianshu.com/p/964fbc30151a，讲的不错*/
private class CleanupTaskWeakReference(
    val task: CleanupTask,  //清理任务
    referent: AnyRef, //要清理的对象
    referenceQueue: ReferenceQueue[AnyRef]) //引用队列
  extends WeakReference(referent, referenceQueue)

/**
 * An asynchronous cleaner for RDD, shuffle, and broadcast state.
 * 一个异步的清理器，用于清理超出应用范围的  RDD，shuffle对应的map任务状态、Shuffle元数据、
 * broadcast对象以及RDD的Checkpoint数据。
 *
 * This maintains a weak reference for each RDD, ShuffleDependency, and Broadcast of interest,
 * to be processed when the associated object goes out of scope of the application. Actual
 * cleanup is performed in a separate daemon thread.
 * 这主要包含每个RDD, ShuffleDependency,和感兴趣的Broadcast的一个弱引用，当它关联对象超出了application
 * 的范围时将被处理。实际上的清理是有一个后台守护线程完成的。
 */
private[spark] class ContextCleaner(sc: SparkContext) extends Logging {

  /**
   * A buffer to ensure that `CleanupTaskWeakReference`s are not garbage collected as long as they
   * have not been handled by the reference queue.
   * 一个缓冲区，来保证只要' CleanupTaskWeakReference '没有被引用队列处理就不会被垃圾回收。
   */
  /*缓存CleanupTaskWeakReference，确保它在没执行前不会被gc*/
  private val referenceBuffer =
    Collections.newSetFromMap[CleanupTaskWeakReference](new ConcurrentHashMap)

  /*缓存顶级的AnyRef引用，里面元素是CleanupTaskWeakReference*/
  private val referenceQueue = new ReferenceQueue[AnyRef]

  /*缓存清理工作的监听器数组*/
  private val listeners = new ConcurrentLinkedQueue[CleanerListener]()

  /*用于具体清理工作的线程*/
  private val cleaningThread = new Thread() { override def run() { keepCleaning() }}

  /*类型为ScheduledExecutorService，用于执行GC的调度线程池，此线程只包含一个线程*/
  private val periodicGCService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("context-cleaner-periodic-gc")

  /**
   * How often to trigger a garbage collection in this JVM.
   *
   * This context cleaner triggers cleanups only when weak references are garbage collected.
   * In long-running applications with large driver JVMs, where there is little memory pressure
   * on the driver, this may happen very occasionally or not at all. Not cleaning at all may
   * lead to executors running out of disk space after a while.
   */
  /*执行GC的时间间隔，默认30min*/
  private val periodicGCInterval =
    sc.conf.getTimeAsSeconds("spark.cleaner.periodicGC.interval", "30min")

  /**
   * Whether the cleaning thread will block on cleanup tasks (other than shuffle, which
   * is controlled by the `spark.cleaner.referenceTracking.blocking.shuffle` parameter).
   *
   * Due to SPARK-3015, this is set to true by default. This is intended to be only a temporary
   * workaround for the issue, which is ultimately caused by the way the BlockManager endpoints
   * issue inter-dependent blocking RPC messages to each other at high frequencies. This happens,
   * for instance, when the driver performs a GC and cleans up all broadcast blocks that are no
   * longer in scope.
   */
  /*清理非Shuffle的其他数据是否是阻塞式的*/
  private val blockOnCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking", true)

  /**
   * Whether the cleaning thread will block on shuffle cleanup tasks.
   *
   * When context cleaner is configured to block on every delete request, it can throw timeout
   * exceptions on cleanup of shuffle blocks, as reported in SPARK-3139. To avoid that, this
   * parameter by default disables blocking on shuffle cleanups. Note that this does not affect
   * the cleanup of RDDs and broadcasts. This is intended to be a temporary workaround,
   * until the real RPC issue (referred to in the comment above `blockOnCleanupTasks`) is
   * resolved.
   */
  /*清理Shuffle的数据是否是阻塞式的*/
  private val blockOnShuffleCleanupTasks = sc.conf.getBoolean(
    "spark.cleaner.referenceTracking.blocking.shuffle", false)

  /*是否停止*/
  @volatile private var stopped = false

  /** Attach a listener object to get information of when objects are cleaned. */
  def attachListener(listener: CleanerListener): Unit = {
    listeners.add(listener)
  }

  /** Start the cleaner. */
  def start(): Unit = {
    /*设置cleaningThread为守护线程、名称为Spark Context Cleaner、启动该线程*/
    cleaningThread.setDaemon(true)
    cleaningThread.setName("Spark Context Cleaner")
    cleaningThread.start()
    /*以默认30min的时间间隔定时进行GC操作的任务
    * 为什么要定时GC呢？*/
    periodicGCService.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = System.gc()
    }, periodicGCInterval, periodicGCInterval, TimeUnit.SECONDS)
  }

  /**
   * Stop the cleaning thread and wait until the thread has finished running its current task.
   */
  def stop(): Unit = {
    stopped = true
    // Interrupt the cleaning thread, but wait until the current task has finished before
    // doing so. This guards against the race condition where a cleaning thread may
    // potentially clean similarly named variables created by a different SparkContext,
    // resulting in otherwise inexplicable block-not-found exceptions (SPARK-6132).
    synchronized {
      cleaningThread.interrupt()
    }
    cleaningThread.join()
    periodicGCService.shutdown()
  }

  /** Register an RDD for cleanup when it is garbage collected. */
  def registerRDDForCleanup(rdd: RDD[_]): Unit = {
    registerForCleanup(rdd, CleanRDD(rdd.id))
  }

  def registerAccumulatorForCleanup(a: AccumulatorV2[_, _]): Unit = {
    registerForCleanup(a, CleanAccum(a.id))
  }

  /** Register a ShuffleDependency for cleanup when it is garbage collected. */
  /*注册一个ShuffleDependency，以便在垃圾收集时进行清理。*/
  def registerShuffleForCleanup(shuffleDependency: ShuffleDependency[_, _, _]): Unit = {
    registerForCleanup(shuffleDependency, CleanShuffle(shuffleDependency.shuffleId))
  }

  /** Register a Broadcast for cleanup when it is garbage collected. */
  def registerBroadcastForCleanup[T](broadcast: Broadcast[T]): Unit = {
    registerForCleanup(broadcast, CleanBroadcast(broadcast.id))
  }

  /** Register a RDDCheckpointData for cleanup when it is garbage collected.
    * 注册一个RDDCheckpointData清理，以便在垃圾收集时进行清理*/
  def registerRDDCheckpointDataForCleanup[T](rdd: RDD[_], parentId: Int): Unit = {
    /*传递的参数为，要清理的对象，清理任务*/
    registerForCleanup(rdd, CleanCheckpoint(parentId))
  }

  /** Register an object for cleanup.
    * 注册一个对象为了清理。*/
  private def registerForCleanup(objectForCleanup: AnyRef, task: CleanupTask): Unit = {
    /*new一个CleanupTaskWeakReference，然后添加到referenceBuffer（为了不被gc回收）*/
    referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue))
  }

  /** Keep cleaning RDD, shuffle, and broadcast state.
    * 保持RDD、shuffle和broadcast状态的清洁。*/
  /*cleaningThread清理线程执行的任务*/
  private def keepCleaning(): Unit = Utils.tryOrStopSparkContext(sc) {
    /*没有stop就一直循环执行*/
    while (!stopped) {
      try {
        /*referenceQueue.remove：如果referenceQueue不为空，移除此引用队列referenceQueue中的下一个引用对象，
        并返回该引用。如果为空，等待100ms，然后再试一次，如果还是为空，返回null。
        * 这里timeout为100ms*/
        val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT))
          .map(_.asInstanceOf[CleanupTaskWeakReference])
        // Synchronize here to avoid being interrupted on stop()
        /*在这里同步，以避免在stop()时被中断*/
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            /*CleanupTaskWeakReference中的cleanTask要执行了，
            已经完成了任务，可以从referenceBuffer中移除它*/
            referenceBuffer.remove(ref)
            /*根据模式匹配执行相应的方法*/
            ref.task match {
                /*清理RDD*/
              case CleanRDD(rddId) =>
                doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
                /*清理Shuffle*/
              case CleanShuffle(shuffleId) =>
                doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
                /*清理Broadcast*/
              case CleanBroadcast(broadcastId) =>
                doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
                /*清理Accum累加器*/
              case CleanAccum(accId) =>
                doCleanupAccum(accId, blocking = blockOnCleanupTasks)
                /*清理Checkpoint*/
              case CleanCheckpoint(rddId) =>
                doCleanCheckpoint(rddId)
            }
          }
        }
      } catch {
        case ie: InterruptedException if stopped => // ignore
        case e: Exception => logError("Error in cleaning thread", e)
      }
    }
  }

  /** Perform RDD cleanup.
    * 清理RDD*/
  def doCleanupRDD(rddId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning RDD " + rddId)
      /*从磁盘或内存删除此RDD数据*/
      sc.unpersistRDD(rddId, blocking)
      /*调用所有监听器CleanerListener的rddCleaned方法（记录日志）*/
      listeners.asScala.foreach(_.rddCleaned(rddId))
      logInfo("Cleaned RDD " + rddId)
    } catch {
      case e: Exception => logError("Error cleaning RDD " + rddId, e)
    }
  }

  /** Perform shuffle cleanup.
    * 清理Shuffle*/
  def doCleanupShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning shuffle " + shuffleId)
      mapOutputTrackerMaster.unregisterShuffle(shuffleId)
      blockManagerMaster.removeShuffle(shuffleId, blocking)
      listeners.asScala.foreach(_.shuffleCleaned(shuffleId))
      logInfo("Cleaned shuffle " + shuffleId)
    } catch {
      case e: Exception => logError("Error cleaning shuffle " + shuffleId, e)
    }
  }

  /** Perform broadcast cleanup.
    * 清理broadcast*/
  def doCleanupBroadcast(broadcastId: Long, blocking: Boolean): Unit = {
    try {
      logDebug(s"Cleaning broadcast $broadcastId")
      broadcastManager.unbroadcast(broadcastId, true, blocking)
      listeners.asScala.foreach(_.broadcastCleaned(broadcastId))
      logDebug(s"Cleaned broadcast $broadcastId")
    } catch {
      case e: Exception => logError("Error cleaning broadcast " + broadcastId, e)
    }
  }

  /** Perform accumulator cleanup.
    * 清理Accum*/
  def doCleanupAccum(accId: Long, blocking: Boolean): Unit = {
    try {
      logDebug("Cleaning accumulator " + accId)
      AccumulatorContext.remove(accId)
      listeners.asScala.foreach(_.accumCleaned(accId))
      logInfo("Cleaned accumulator " + accId)
    } catch {
      case e: Exception => logError("Error cleaning accumulator " + accId, e)
    }
  }

  /**
   * Clean up checkpoint files written to a reliable storage.
   * Locally checkpointed files are cleaned up separately through RDD cleanups.
   * 清理写入可靠存储的检查点文件。
   * 本地检查点文件通过RDD清理单独进行清理。
   */
  def doCleanCheckpoint(rddId: Int): Unit = {
    try {
      logDebug("Cleaning rdd checkpoint data " + rddId)
      ReliableRDDCheckpointData.cleanCheckpoint(sc, rddId)
      listeners.asScala.foreach(_.checkpointCleaned(rddId))
      logInfo("Cleaned rdd checkpoint data " + rddId)
    }
    catch {
      case e: Exception => logError("Error cleaning rdd checkpoint data " + rddId, e)
    }
  }

  private def blockManagerMaster = sc.env.blockManager.master
  private def broadcastManager = sc.env.broadcastManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}

private object ContextCleaner {
  private val REF_QUEUE_POLL_TIMEOUT = 100
}

/**
 * Listener class used for testing when any item has been cleaned by the Cleaner class.
 * 此接口测试RDDs, shuffles,等是否已经被Cleaner成功删除。
 */
private[spark] trait CleanerListener {
  def rddCleaned(rddId: Int): Unit
  def shuffleCleaned(shuffleId: Int): Unit
  def broadcastCleaned(broadcastId: Long): Unit
  def accumCleaned(accId: Long): Unit
  def checkpointCleaned(rddId: Long): Unit
}
