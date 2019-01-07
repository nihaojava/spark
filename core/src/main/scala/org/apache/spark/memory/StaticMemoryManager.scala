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

package org.apache.spark.memory

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

/**
 * A [[MemoryManager]] that statically partitions the heap space into disjoint regions.
 * 一个MemoryManager的实现，它静态地将堆空间划分为不相交的区域。
 * 【就是固定的将堆内存划分为两块，用于存储和计算】
 * 【好处：简单管理，缺点：内存利用率不高，可能有一块内存空闲很多，另一块却不够用】
 *
 * The sizes of the execution and storage regions are determined through
 * `spark.shuffle.memoryFraction` and `spark.storage.memoryFraction` respectively. The two
 * regions are cleanly separated such that neither usage can borrow memory from the other.
 * 执行内存和存储内存的大小由`spark.shuffle.memoryFraction`和`spark.storage.memoryFraction`决定。
 * 这两个区域被清晰地分开，这样一个区域就不能从另一个区域中借用内存。
 */
private[spark] class StaticMemoryManager(
    conf: SparkConf,
    maxOnHeapExecutionMemory: Long,
    override val maxOnHeapStorageMemory: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    maxOnHeapStorageMemory,
    maxOnHeapExecutionMemory) {

  def this(conf: SparkConf, numCores: Int) {
    this(
      conf,
      StaticMemoryManager.getMaxExecutionMemory(conf),
      StaticMemoryManager.getMaxStorageMemory(conf),
      numCores)
  }

  // The StaticMemoryManager does not support off-heap storage memory:
  /*【StaticMemoryManager 不支持堆外存储，支持堆外计算】*/
  offHeapExecutionMemoryPool.incrementPoolSize(offHeapStorageMemoryPool.poolSize)
  offHeapStorageMemoryPool.decrementPoolSize(offHeapStorageMemoryPool.poolSize)

  // Max number of bytes worth of blocks to evict when unrolling
  /*最大的展开内存=最大的存储内存*0.2*/
  private val maxUnrollMemory: Long = {
    (maxOnHeapStorageMemory * conf.getDouble("spark.storage.unrollFraction", 0.2)).toLong
  }

  override def maxOffHeapStorageMemory: Long = 0L

  /*为blockId申请numBytes字节的内存*/
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    require(memoryMode != MemoryMode.OFF_HEAP,
      "StaticMemoryManager does not support off-heap storage memory")
    if (numBytes > maxOnHeapStorageMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxOnHeapStorageMemory bytes)")
      false
    } else {
      onHeapStorageMemoryPool.acquireMemory(blockId, numBytes)
    }
  }

  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    require(memoryMode != MemoryMode.OFF_HEAP,
      "StaticMemoryManager does not support off-heap unroll memory")
    val currentUnrollMemory = onHeapStorageMemoryPool.memoryStore.currentUnrollMemory
    val freeMemory = onHeapStorageMemoryPool.memoryFree
    // When unrolling, we will use all of the existing free memory, and, if necessary,
    // some extra space freed from evicting cached blocks. We must place a cap on the
    // amount of memory to be evicted by unrolling, however, otherwise unrolling one
    // big block can blow away the entire cache.
    val maxNumBytesToFree = math.max(0, maxUnrollMemory - currentUnrollMemory - freeMemory)
    // Keep it within the range 0 <= X <= maxNumBytesToFree
    val numBytesToFree = math.max(0, math.min(maxNumBytesToFree, numBytes - freeMemory))
    onHeapStorageMemoryPool.acquireMemory(blockId, numBytes, numBytesToFree)
  }

  private[memory]
  override def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
    }
  }
}


private[spark] object StaticMemoryManager {

  private val MIN_MEMORY_BYTES = 32 * 1024 * 1024 // 32M

  /**
   * Return the total amount of memory available for the storage region, in bytes.
   * 返回总共可用于存储的内存（字节）
   */
  // 运行时jvm可使用的最大内存 * 0.6 * 0.9
  private def getMaxStorageMemory(conf: SparkConf): Long = {
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

  /**
   * Return the total amount of memory available for the execution region, in bytes.
   * 返回总共可用于计算的内存（字节）
   * 运行时jvm可使用的最大内存 * 0.2 * 0.8
   */
  private def getMaxExecutionMemory(conf: SparkConf): Long = {
    // 运行时jvm可使用的最大内存
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    // 运行时jvm可使用的最大内存 不能小于 32M
    if (systemMaxMemory < MIN_MEMORY_BYTES) {
      throw new IllegalArgumentException(s"System memory $systemMaxMemory must " +
        s"be at least $MIN_MEMORY_BYTES. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
      /*否则，抛出异常（jvm系统可以使用内存最少是32M。请增加堆内存，通过使用--driver-memory选项或
      spark配置文件中的spark.driver.memory）*/
    }
    /*检查配置的spark.executor.memory是否不小于32M*/
    if (conf.contains("spark.executor.memory")) {
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      if (executorMemory < MIN_MEMORY_BYTES) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$MIN_MEMORY_BYTES. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

}
