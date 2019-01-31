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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Future, Promise}

import org.apache.spark.internal.Logging

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 * 等待DAGScheduler作业完成的对象。当任务完成时,它将结果传递给给定的处理函数。
 */
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler, /*当前job的调度者*/
    val jobId: Int, /*等待执行完成的job*/
    totalTasks: Int, /*等待执行完成的job包含的task个数*/
    resultHandler: (Int, T) => Unit)  /*执行结果的处理器*/
  extends JobListener with Logging {

  /*等待完成的job已经完成的task数量*/
  private val finishedTasks = new AtomicInteger(0)
  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  /*用来代表job完成后的结果。
  如果totalTasks==0，说明没有task需要执行，此时jobPromise将被直接设置为success。
  不为0的话，利用Promise的伴生对象new一个Promise*/
  private val jobPromise: Promise[Unit] =
    if (totalTasks == 0) Promise.successful(()) else Promise()

  /*job是否已经完成*/
  def jobFinished: Boolean = jobPromise.isCompleted

  /*jobPromise的Future*/
  def completionFuture: Future[Unit] = jobPromise.future

  /**
   * Sends a signal to the DAGScheduler to cancel the job. The cancellation itself is handled
   * asynchronously. After the low level scheduler cancels all the tasks belonging to this job, it
   * will fail this job with a SparkException.
   */
  /*取消对job的执行*/
  def cancel() {
    dagScheduler.cancelJob(jobId)
  }

  /*重写的方法，*/
  override def taskSucceeded(index: Int, result: Any): Unit = {
    // resultHandler call must be synchronized in case resultHandler itself is not thread safe.
    synchronized {
      /*调用resultHandler来处理job中每个task的执行结果*/
      resultHandler(index, result.asInstanceOf[T])
    }
    /*增加已完成的Task的个数，
    * 如果job的所有task都已经完成，那么将jobPromise设置为success*/
    if (finishedTasks.incrementAndGet() == totalTasks) {
      jobPromise.success(())
    }
  }

  /*将jobPromise设置为failure*/
  override def jobFailed(exception: Exception): Unit = {
    if (!jobPromise.tryFailure(exception)) {
      logWarning("Ignore failure", exception)
    }
  }

}
