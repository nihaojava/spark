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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.Partition

/**
 * Enumeration to manage state transitions of an RDD through checkpointing
 * 枚举 一个RDD执行checkpoint过程中 的各种管理状态
 *
 * [ 初始化，进行中，完成 ]
 * [ Initialized --{@literal >} checkpointing in progress --{@literal >} checkpointed ]
 */
private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value
  val Initialized, CheckpointingInProgress, Checkpointed = Value
}

/**
 * This class contains all the information related to RDD checkpointing. Each instance of this
 * class is associated with an RDD. It manages process of checkpointing of the associated RDD,
 * as well as, manages the post-checkpoint state by providing the updated partitions,
 * iterator and preferred locations of the checkpointed RDD.
 * 这个类包含了所关联RDD检查点的所有信息。这个类的每一个实例都关联一个RDD。该实例管理关联RDD的检查点过程，
 * 也管理 过去检查点的状态，通过提供checkpointed RDD 的跟新的 分区，迭代器和优先位置。
 */
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends Serializable {

  import CheckpointState._

  // The checkpoint state of the associated RDD.
  // 关联RDD的checkpoint状态，刚开始为Initialized
  protected var cpState = Initialized

  // The RDD that contains our checkpointed data
  // cpRDD 包含我们已经完成checkpointed的数据
  // 【一个RDD checkpoint完成之后，会生成一个cpRDD，表示该RDD checkpoint之后的数据】
  private var cpRDD: Option[CheckpointRDD[T]] = None

  // TODO: are we sure we need to use a global lock in the following methods?

  /**
   * Return whether the checkpoint data for this RDD is already persisted.
   * 返回此RDD是否已经checkpoint完成
   */
  def isCheckpointed: Boolean = RDDCheckpointData.synchronized {
    cpState == Checkpointed
  }

  /**
   * Materialize this RDD and persist its content.
   * This is called immediately after the first action invoked on this RDD has completed.
   * 实例化此RDD 并 持久化其内容。
   * 在此RDD上调用的第一个action完成之后，立即被调用。
   * 【重要，】
   */
  final def checkpoint(): Unit = {
    // Guard against multiple threads checkpointing the same RDD by
    // atomically flipping the state of this RDDCheckpointData
    // 通过加锁修改RDDCheckpointData这个状态，来防止多个线程同时checkpointing同一个RDD
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) {
        cpState = CheckpointingInProgress
      } else {
        return
      }
    }
    // 这里调用的是子类的doCheckpoint()，将数据checkPoint，并且返回一个新RDD
    // 【代表checkpointed文件中的数据】
    val newRDD = doCheckpoint()

    // Update our state and truncate the RDD lineage
    // 更新状态 并且 重构此RDD的lineage（依赖关系）
    RDDCheckpointData.synchronized {
      cpRDD = Some(newRDD)
      cpState = Checkpointed
      // 清空RDD的依赖，但是没找到在哪里将newRDD设置为新的 父RDD？
      // 在新版本中，在checkpoint的过程中确实没有将newRDD设置为新的父RDD,
      // 而是将其放到了RDD的dependencies方法里，调用getDependencies的时候，会赋值
      // 应该是为了Lazy执行吧，用的时候再赋值
      rdd.markCheckpointed()
    }
  }

  /**
   * Materialize this RDD and persist its content.
   * 实例化此RDD（CheckpointRDD类型） 并 持久化其内容
   * Subclasses should override this method to define custom checkpointing behavior.
   * @return the checkpoint RDD created in the process.
   */
  protected def doCheckpoint(): CheckpointRDD[T]

  /**
   * Return the RDD that contains our checkpointed data.
   * This is only defined if the checkpoint state is `Checkpointed`.
   * 返回cpRDD，它包含了我们checkpointed的数据。
   * 只有在checkpoint的状态是`Checkpointed`的时候才定义
   */
  def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized { cpRDD }

  /**
   * Return the partitions of the resulting checkpoint RDD.
   * For tests only.
   * 返回cpRDD的分区。
   * 只用于测试。
   */
  def getPartitions: Array[Partition] = RDDCheckpointData.synchronized {
    cpRDD.map(_.partitions).getOrElse { Array.empty }
  }

}

/**
 * Global lock for synchronizing checkpoint operations.
 * 全局锁用于checkpoint 操作的同步。
 */
private[spark] object RDDCheckpointData
