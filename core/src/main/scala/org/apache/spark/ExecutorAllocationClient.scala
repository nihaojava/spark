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

/**
 * A client that communicates with the cluster manager to request or kill executors.
 * This is currently supported only in YARN mode.
 * 一个客户端，它和cluster manager通信以请求 添加或杀死 executors。
 * 目前只支持YARN模式。
 */
private[spark] trait ExecutorAllocationClient {


  /** Get the list of currently active executors */
  /*获取当前活跃的executors ID 列表*/
  private[spark] def getExecutorIds(): Seq[String]

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * 根据我们的调度需求更新集群管理器。其中包含3位信息以帮助它做出决策。
   *
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
   *                     我们想要的executors的总数。集群管理器不应该杀死任何正在运行的executor来达到这个数字，
   *                     但是，如果所有现有的executor都将死亡，那么这就是我们希望分配的executor数量。
   *
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   *                           具有本地优先的所有活动阶段中的任务数量。这包括运行、挂起和完成的任务。
   *
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   *                             主机到希望在该主机上运行的所有活动阶段的任务数量的映射。
   *                             这包括运行、挂起和完成的任务。
   * @return whether the request is acknowledged by the cluster manager.
   *         请求是否得到集群管理器的确认。
   */
  /*根据我们的调度需求请求更新集群管理器。返回：请求是否得到集群管理器的确认。*/
  private[spark] def requestTotalExecutors(
      numExecutors: Int,  //我们希望的executors的总数。
      localityAwareTasks: Int,  //具有数据本地性的task数
      hostToLocalTaskCount: Map[String, Int]): Boolean //主机 到 希望在此主机上运行的task的个数 之间的映射

  /**
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is acknowledged by the cluster manager.
   */
  /*请求添加numAdditionalExecutors个Executors*/
  /*返回是否成功*/
  def requestExecutors(numAdditionalExecutors: Int): Boolean

  /**
   * Request that the cluster manager kill the specified executors.
   * @return the ids of the executors acknowledged by the cluster manager to be removed.
   */
  /*请求kill杀死executorIds列表里的executor*/
  /*返回已经killed的cluster列表*/
  def killExecutors(executorIds: Seq[String]): Seq[String]

  /**
   * Request that the cluster manager kill the specified executor.
   * @return whether the request is acknowledged by the cluster manager.
   */
  /*请求杀死id为executorId的executor，实际调用的killExecutors*/
  def killExecutor(executorId: String): Boolean = {
    val killedExecutors = killExecutors(Seq(executorId))
    killedExecutors.nonEmpty && killedExecutors(0).equals(executorId)
  }
}
