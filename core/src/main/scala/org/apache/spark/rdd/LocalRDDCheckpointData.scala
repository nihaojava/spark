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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * An implementation of checkpointing implemented on top of Spark's caching layer.
 * checkpointing的一个实现，在spark缓存层上实现的。
 *
 * Local checkpointing trades off fault tolerance for performance by skipping the expensive
 * step of saving the RDD data to a reliable and fault-tolerant storage. Instead, the data
 * is written to the local, ephemeral block storage that lives in each executor. This is useful
 * for use cases where RDDs build up long lineages that need to be truncated often (e.g. GraphX).
 * Local checkpointing 是容错和性能的一个折中，通过跳过保存RDD数据到可靠的存储这些计算昂贵的步骤。
 * 改为将数据写到本地，在每个executor中临时的块存储。在RDD构建长lineages，需要经常重建的时候非常有用。
 * （例如 graphX）
 */
private[spark] class LocalRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  /**
   * Ensure the RDD is fully cached so the partitions can be recovered later.
   * 确保RDD完全缓存，因为稍后这些分区将被恢复。
   */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val level = rdd.getStorageLevel

    // Assume storage level uses disk; otherwise memory eviction may cause data loss
    // 假设RDD的存储级别使用了磁盘；否则内存回收有可能丢失数据
    assume(level.useDisk, s"Storage level $level is not appropriate for local checkpointing")

    // Not all actions compute all partitions of the RDD (e.g. take). For correctness, we
    // must cache any missing partitions. TODO: avoid running another job here (SPARK-8582).
    val action = (tc: TaskContext, iterator: Iterator[T]) => Utils.getIteratorSize(iterator)
    // 得到rdd没有缓存的分区 ？
    val missingPartitionIndices = rdd.partitions.map(_.index).filter { i =>
      !SparkEnv.get.blockManager.master.contains(RDDBlockId(rdd.id, i))
    }
    if (missingPartitionIndices.nonEmpty) {
      rdd.sparkContext.runJob(rdd, action, missingPartitionIndices)
    }

    new LocalCheckpointRDD[T](rdd)
  }

}

private[spark] object LocalRDDCheckpointData {

  val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

  /**
   * Transform the specified storage level to one that uses disk.
   * 修改storage level 为指定的，并且useDisk = true
   *
   * This guarantees that the RDD can be recomputed multiple times correctly as long as
   * executors do not fail. Otherwise, if the RDD is cached in memory only, for instance,
   * the checkpoint data will be lost if the relevant block is evicted from memory.
   *
   * This method is idempotent.
   */
  def transformStorageLevel(level: StorageLevel): StorageLevel = {
    StorageLevel(useDisk = true, level.useMemory, level.deserialized, level.replication)
  }
}
