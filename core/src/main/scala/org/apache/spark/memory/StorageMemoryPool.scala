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

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore

/**
 * Performs bookkeeping for managing an adjustable-size pool of memory that is used for storage
 * (caching).
 * 为了管理可调大小的内存池，进行记账。这个内存池是用于存储的（缓存）。
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 * memoryMode: 内存模式。由此可以断定用于存储的内存池包括堆内存的内存池和对外内存的内存池。
 * StorageMemoryPool的堆外和堆内存都是逻辑上的区分，前者是使用unsafe的API分配的系统内存，后者是
 * 系统分配给jvm堆内存的一部分。
 */
private[memory] class StorageMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  // 内存池的名称
  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap storage"
    case MemoryMode.OFF_HEAP => "off-heap storage"
  }

  //已使用的内存大小（单位为Byte）
  @GuardedBy("lock")
  private[this] var _memoryUsed: Long = 0L
  //实际返回_memoryUsed
  override def memoryUsed: Long = lock.synchronized {
    _memoryUsed
  }

  // 当前StorageMemoryPool所关联的MemoryStore
  private var _memoryStore: MemoryStore = _
  def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalStateException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   * 设置MemoryStore，用于通过此管理器回收缓存块。
   * 由于初始化顺序的约束，这必须在构造之后设置。
   * 【先构造StorageMemoryPool，然后把它当做构造函数的参数，来构造MemoryStore，所以】
   * 【可见StorageMemoryPool和MemoryStore是双向引用，1对1】
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   * 获取N个字节的内存来缓存给定的块，如果需要，删除现有的一些块（已经缓存在内存池中的）。
   * @return whether all N bytes were successfully granted.
   * 用于给Blockid对应的block获取numBytes指定大小的内存
   */
  def acquireMemory(blockId: BlockId, numBytes: Long): Boolean = lock.synchronized {
    // 计算要申请的内存大小numBytes 和空闲内存memoryFree的差值，和 0 比较，取其中大的
    // 如果numBytesToFree大于0，则说明空闲内存不够用，需要腾出部分Block所占用的内存空间。
    val numBytesToFree = math.max(0, numBytes - memoryFree)
    // 申请内存
    acquireMemory(blockId, numBytes, numBytesToFree)
  }

  /**
   * Acquire N bytes of storage memory for the given block, evicting existing ones if necessary.
   * 获取N个字节的内存来缓存给定的块，如果必要，删除现有的一些块（已经在缓存中的）。
   *
   * @param blockId the ID of the block we are acquiring storage memory for
   * @param numBytesToAcquire the size of this block   // block的大小
   * @param numBytesToFree the amount of space to be freed through evicting blocks // 需要驱逐block来腾出的空间大小Bytes
   * @return whether all N bytes were successfully granted. //是否成功获取到内存。
   */
  def acquireMemory(
      blockId: BlockId,
      numBytesToAcquire: Long,
      numBytesToFree: Long): Boolean = lock.synchronized {
    assert(numBytesToAcquire >= 0)
    assert(numBytesToFree >= 0)
    assert(memoryUsed <= poolSize)
    if (numBytesToFree > 0) { // 需要腾出numBytesToFree大小的空间
      // 腾出numBytesToFree大小的空间
      memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode)
    }
    // NOTE: If the memory store evicts blocks, then those evictions will synchronously call
    // back into this StorageMemoryPool in order to free memory. Therefore, these variables
    // should have been updated.
    // 如果 memory store删除了block，为了释放内存这些删除会同步回调到这个StorageMemoryPool。
    // 因此，这些变量已经更新了。
    val enoughMemory = numBytesToAcquire <= memoryFree // 判断内存是否充足
    if (enoughMemory) { //如果空闲充足，增加已使用内存的大小（即逻辑上获得了用于储存block的内存空间）
      _memoryUsed += numBytesToAcquire
    }
    enoughMemory  // 返回是否成功获的了用于存储Block的内存空间
  }

  // 释放size指定大小的内存【只是逻辑上的释放，引用链不可达，何时真的被回收，内存不够用的时候gc嘛？】
  def releaseMemory(size: Long): Unit = lock.synchronized {
    if (size > _memoryUsed) { /*如果释放的内存大小 > 已经使用的内存大小，记录警告，将_memoryUsed全部释放*/
      logWarning(s"Attempted to release $size bytes of storage " +
        s"memory when we only have ${_memoryUsed} bytes")
      _memoryUsed = 0
    } else {  /*否则，释放指定大小内存*/
      _memoryUsed -= size
    }
  }
  // 释放当前内存池的所有内存，即将_memoryUsed设置为0.
  def releaseAllMemory(): Unit = lock.synchronized {
    _memoryUsed = 0
  }

  /**
   * Free space to shrink the size of this storage memory pool by `spaceToFree` bytes.
   * Note: this method doesn't actually reduce the pool size but relies on the caller to do so.
   * 释放spaceToFree指定的字节空间以缩小storage memory pool 的大小。
   * 注意：此方法不会真正的减少pool的大小 而是 依赖于调用者来减少。
   *
   * @return number of bytes to be removed from the pool's capacity.
   *         返回 将被从内存池容量中删除的字节大小
   */
  /*步骤：先从空闲空间释放，空闲空间不够的话，先将空闲内存释放完，再驱逐block释放一部分*/
  def freeSpaceToShrinkPool(spaceToFree: Long): Long = lock.synchronized {
    //取 要释放的内存 和 空闲内存 其中小的，得到通过释放未使用的内存而释放的空间
    val spaceFreedByReleasingUnusedMemory = math.min(spaceToFree, memoryFree)
    /*  如果spaceToFree>memoryFree(要释放的 > 空闲的),那么spaceFreedByReleasingUnusedMemory=空闲的
        spaceToFree - spaceFreedByReleasingUnusedMemory = 要释放的 - 空闲的，表示除了空闲空间外，还需要腾出的空间
        如果spaceToFree<memoryFree(要释放的 < 空闲的),那么spaceFreedByReleasingUnusedMemory=要释放的
        spaceToFree - spaceFreedByReleasingUnusedMemory = 要释放的 - 要释放的，表示除了空闲空间外，还需要腾出的空间,此时=0
    */
    /*remainingSpaceToFree表示表示除了释放空闲内存空间外，还需要腾出的空间*/
    val remainingSpaceToFree = spaceToFree - spaceFreedByReleasingUnusedMemory
    if (remainingSpaceToFree > 0) { //表示空闲空间还是不够，还需要腾出remainingSpaceToFree空间
      // If reclaiming free memory did not adequately shrink the pool, begin evicting blocks:
      // 如果回收空闲内存还是不够，那么开始回收块:
      val spaceFreedByEviction =
        memoryStore.evictBlocksToFreeSpace(None, remainingSpaceToFree, memoryMode)
      // When a block is released, BlockManager.dropFromMemory() calls releaseMemory(), so we do
      // not need to decrement _memoryUsed here. However, we do need to decrement the pool size.
      // 此时返回 memoryFree + spaceFreedByEviction
      spaceFreedByReleasingUnusedMemory + spaceFreedByEviction
    } else {  //表示空闲内存已经足够，此时返回spaceToFree
      spaceFreedByReleasingUnusedMemory
    }
  }
}
