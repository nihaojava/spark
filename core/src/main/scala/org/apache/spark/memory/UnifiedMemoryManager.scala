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
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
 * 一个MemoryManager，它制定了一个软的边界在执行 和 存储之间，一边可以向另一边借。
 *
 * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
 * configurable through `spark.memory.fraction` (default 0.6). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.6 * 0.5 = 0.3 of the heap space by default.
 * 在执行和存储之间共享的区域是(the total heap space - 300MB)的比例部分，通过spark.memory.fraction来配置，
 * 默认是0.6.在这个空间内边界的位置是由spark.memory.storageFraction来决定的，默认是0.5.
 * 这意味着存储区域的默认大小是：(the total heap space - 300MB) 的0.3倍。
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.
 * 存储可以借用尽可能多的空闲的执行内存，直到执行收回其空间。
 * 当这种情况发生时，缓存块将从内存中驱逐，直到释放了足够的借来的内存来满足execution要求释放的内存为止。
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.
 * 相似的，execution也可以尽可能多的借storage空闲的内存。然后，由于执行的复杂性，execution 内存从来不会被
 * 驱逐。这意味着，如果执行已经耗尽借来的存储内存，缓存块的尝试可能会失败。在这种情况下，根据他们各自的储存水平，
 * 新的块将被立即驱逐。
 *
 * @param onHeapStorageRegionSize Size of the storage region, in bytes.
 *                          This region is not statically reserved; execution can borrow from
 *                          it if necessary. Cached blocks can be evicted only if actual
 *                          storage memory usage exceeds this region.
 *                          storage区域的大小（单位字节）。这个区域不会静态的预留；如果必要，execution
 *                          可以向它借。只有在实际的存储内存使用超过此区域，缓存的块才会被回收。
 */
/*【展开内存：可以理解为为了存储block预留（提前申请）的内存】*/
/*构造方法申明为private，所以只能通过伴生对象的apply来构造UnifiedMemoryManager*/
private[spark] class UnifiedMemoryManager private[memory] (
    conf: SparkConf,
    val maxHeapMemory: Long, //最大堆内存，(the total heap space - 300MB)*0.6
                             /*也可以理解为存储和计算总共可以使用的内存*/
    onHeapStorageRegionSize: Long, //用于存储的堆内存大小。(the total heap space - 300MB)*0.6*0.5
                             /*默认用于存储的内存大小*/
    numCores: Int) // cup内核数
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize) {

  /*断言*/
  private def assertInvariants(): Unit = {
    /*用于计算的堆内存池大小 + 用于存储的堆内存池大小 = 最大堆内存*/
    assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
    /*用于计算的堆外内存池大小 + 用于存储的堆外内存池大小 = 最大堆外内存*/
    assert(
      offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
  }

  assertInvariants()
  // 返回可以用于存储的最大堆内存
  // 注意：存储和计算总共的堆内存 - 计算已经使用的堆内存，因为可借嘛
  override def maxOnHeapStorageMemory: Long = synchronized {
    maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  // 返回可以用于存储的最大堆外内存
  // 注意：存储和计算总共的堆外内存 - 计算已经使用的堆外内存，因为可借嘛
  override def maxOffHeapStorageMemory: Long = synchronized {
    maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        onHeapStorageRegionSize,
        maxHeapMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        offHeapStorageMemory,
        maxOffHeapMemory)
    }

    /**
     * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
     *
     * When acquiring memory for a task, the execution pool may need to make multiple
     * attempts. Each attempt must be able to evict storage in case another task jumps in
     * and caches a large block between the attempts. This is called once per attempt.
     */
    def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
      if (extraMemoryNeeded > 0) {
        // There is not enough free memory in the execution pool, so try to reclaim memory from
        // storage. We can reclaim any free memory from the storage pool. If the storage pool
        // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
        // the memory that storage has borrowed from execution.
        val memoryReclaimableFromStorage = math.max(
          storagePool.memoryFree,
          storagePool.poolSize - storageRegionSize)
        if (memoryReclaimableFromStorage > 0) {
          // Only reclaim as much space as is necessary and available:
          val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
            math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
          storagePool.decrementPoolSize(spaceToReclaim)
          executionPool.incrementPoolSize(spaceToReclaim)
        }
      }
    }

    /**
     * The size the execution pool would have after evicting storage memory.
     * 删除存储内存后执行池的大小。
     * The execution memory pool divides this quantity among the active tasks evenly to cap
     * the execution memory allocation for each task. It is important to keep this greater
     * than the execution pool size, which doesn't take into account potential memory that
     * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
     *
     * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
     * in execution memory allocation across tasks, Otherwise, a task may occupy more than
     * its fair share of execution memory, mistakenly thinking that other tasks can acquire
     * the portion of storage memory that cannot be evicted.
     */
    /*这个是动态变化的值，当存储内存有空闲时 = 计算内存池和存储内存池的总大小 - 已经使用的存储内；
    * 当存储内存没有空闲还借用了计算内存池的时候 = 计算内存池的大小*/
    /*也可以这样理解，存储有空闲时，= 存储空闲的+计算的；
    * 存储没空闲时， =计算的*/
     def computeMaxExecutionPoolSize(): Long = {
      maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
    }

    executionPool.acquireMemory(
      numBytes, taskAttemptId, maybeGrowExecutionPool, computeMaxExecutionPoolSize)
  }

  // 获取numBytes个字节的内存来缓存给定的block，如果空闲内存不够，驱逐现有的一些块（已经在缓存中的）。
  override def acquireStorageMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    assertInvariants()
    assert(numBytes >= 0)
    // 获取内存模式对应的计算内存池、存储内存池和可以存储的最大空间
    val (executionPool, storagePool, maxMemory) = memoryMode match {
      case MemoryMode.ON_HEAP => (
        onHeapExecutionMemoryPool,
        onHeapStorageMemoryPool,
        maxOnHeapStorageMemory)
      case MemoryMode.OFF_HEAP => (
        offHeapExecutionMemoryPool,
        offHeapStorageMemoryPool,
        maxOffHeapMemory) // 为什么是maxOffHeapMemory ，而不是maxOffHeapStorageMemory ？
    }
    // 所需申请的空间numBytes > 大于可以存储的最大空间，无法提供，返回flase,否则继续往下
    if (numBytes > maxMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxMemory bytes)")
      return false
    }
    // 如果申请的空间numBytes > 存储内存池空闲空间，需要到计算内存池去借用
    if (numBytes > storagePool.memoryFree) {
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      // 计算要借多少内存，取 计算池的空闲内存 和 申请内存 之间的最小值，
      // 【尽可能少的借，哪为什么不差多少借多少呢？借numBytes，而不是numBytes-storagePool.memoryFree】
      val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree, numBytes)
      // 借用一定成功，计算池减少
      /*因为memoryBorrowedFromExecution一定<=executionPool.memoryFree*/
      executionPool.decrementPoolSize(memoryBorrowedFromExecution)
      // 存储池增加
      storagePool.incrementPoolSize(memoryBorrowedFromExecution)
    }
    // 从存储内存池申请numBytes字节的空间
    storagePool.acquireMemory(blockId, numBytes)
  }

  // 为展开blockid对应的block，获取numBytes指定大小的内存，实际调用的是acquireStorageMemory
  override def acquireUnrollMemory(
      blockId: BlockId,
      numBytes: Long,
      memoryMode: MemoryMode): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes, memoryMode)
  }
}

object UnifiedMemoryManager {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  /*为非存储、非执行目的预留固定数量的内存。*/
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
  /*它的功能类似于“spark.memory.fraction”，但是可以保证我们为系统保留了足够的内存*/
  /*例如，我们jvm的内存是1G（xmx），那么用于存储和计算的内存将默认是 (1024 - 300) * 0.6 = 434MB*/
  /*jvm系统预留内存300M*/
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    /*获取memoryManager可以使用的最大堆内存，例如：(1024 - 300) * 0.6 = 434MB*/
    val maxMemory = getMaxMemory(conf)
    new UnifiedMemoryManager(
      conf,
      maxHeapMemory = maxMemory,
      /*用于存储的堆内存，默认为maxMemory的一半*/
      onHeapStorageRegionSize =
        (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   * 返回在计算和存储之间共享的总内存大小
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    /*jvm系统可用最大内存，会比xmx设置的小吧？*/
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    /*预留内存默认为300M*/
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    /*要求的系统最小内存，预留内存的1.5倍，默认是300*1.5=450M*/
    val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
    /*jvm系统可用最大内存 比 要求的系统最小内存 还小的话，抛出异常*/
    /*这里应该检查的是driver的内存*/
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
        s"option or spark.driver.memory in Spark configuration.")
    }
    // SPARK-12759 Check executor memory to fail fast if memory is insufficient
    /*检查executor内存是否充足*/
    if (conf.contains("spark.executor.memory")) {
      val executorMemory = conf.getSizeAsBytes("spark.executor.memory")
      if (executorMemory < minSystemMemory) {
        throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
          s"$minSystemMemory. Please increase executor memory using the " +
          s"--executor-memory option or spark.executor.memory in Spark configuration.")
      }
    }
    /*memoryManager可以使用的最大堆内存*/
    val usableMemory = systemMemory - reservedMemory
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.6)
    (usableMemory * memoryFraction).toLong
  }
}
