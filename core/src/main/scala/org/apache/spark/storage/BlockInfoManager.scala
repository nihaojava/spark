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

package org.apache.spark.storage

import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import com.google.common.collect.ConcurrentHashMultiset

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging


/**
 * Tracks metadata for an individual block.
 * 一个块的元数据信息（大小，存储级别，读写锁）
 * Instances of this class are _not_ thread-safe and are protected by locks in the
 * [[BlockInfoManager]].
 * 线程不安全的，需要加锁。
 *
 * @param level the block's storage level. This is the requested persistence level, not the
 *              effective storage level of the block (i.e. if this is MEMORY_AND_DISK, then this
 *              does not imply that the block is actually resident in memory).
 * @param classTag the block's [[ClassTag]], used to select the serializer
 * @param tellMaster whether state changes for this block should be reported to the master. This
 *                   is true for most blocks, but is false for broadcast blocks.
 */
private[storage] class BlockInfo(
    val level: StorageLevel,  // 存储级别
    val classTag: ClassTag[_], // block类别
    val tellMaster: Boolean) {  // BlockInfo所描述的Block是否需要告知Master

  /**
   * The size of the block (in bytes)
   */
  def size: Long = _size  //大小
  def size_=(s: Long): Unit = {
    _size = s
    checkInvariants()
  }
  private[this] var _size: Long = 0

  /**
   * The number of times that this block has been locked for reading.
   * BlockInfo所描述的Block现在被加的读锁个数。
   * block所加的读锁个数【同一时间一个block可以被加 多个读锁 或者 只有一个写锁】
   * 【读读 共享锁；读写，写读，写写 都是排他锁（互斥锁）】
   */
  def readerCount: Int = _readerCount
  def readerCount_=(c: Int): Unit = {
    _readerCount = c
    checkInvariants()
  }
  private[this] var _readerCount: Int = 0

  /**
   * The task attempt id of the task which currently holds the write lock for this block, or
   * [[BlockInfo.NON_TASK_WRITER]] if the write lock is held by non-task code, or
   * [[BlockInfo.NO_WRITER]] if this block is not locked for writing.
   * 任务尝试在对block进行写操作前，首先必须获得对应BlockInfo的写锁。_writerTask用于保存任务尝试的ID
   * （每个任务在实际执行时，会多次尝试，每次尝试都会分配一个ID）
   * 保存尝试task的ID
   */
  def writerTask: Long = _writerTask
  def writerTask_=(t: Long): Unit = {
    _writerTask = t
    checkInvariants()
  }
  private[this] var _writerTask: Long = BlockInfo.NO_WRITER //默认任务尝试ID是BlockInfo.NO_WRITER，即-1，表示没有

  // 检查不可变的量
  private def checkInvariants(): Unit = {
    // A block's reader count must be non-negative:
    assert(_readerCount >= 0) // _readerCount必须大于等于0
    // A block is either locked for reading or for writing, but not for both at the same time:
    assert(_readerCount == 0 || _writerTask == BlockInfo.NO_WRITER) // 不能同时获得一个block的读锁和写锁
  }

  checkInvariants()
}

private[storage] object BlockInfo {

  /**
   * Special task attempt id constant used to mark a block's write lock as being unlocked.
   * 特殊任务尝试id常量，用于表示一个block的写锁当前是解锁状态
   */
  /*写锁是解锁状态*/
  val NO_WRITER: Long = -1

  /**
   * Special task attempt id constant used to mark a block's write lock as being held by
   * a non-task thread (e.g. by a driver thread or by unit test code).
   * 特殊任务尝试id常量，用于表示一个block的写锁被一个非task线程持有。（例如：被一个driver或测试线程）
   */
  val NON_TASK_WRITER: Long = -1024
}

/**
 * Component of the [[BlockManager]] which tracks metadata for blocks and manages block locking.
 * BlockManager的组件，用于管理blocks的元数据 和 block的锁。
 *
 * The locking interface exposed by this class is readers-writer lock. Every lock acquisition is
 * automatically associated with a running task and locks are automatically released upon task
 * completion or failure.
 * 此类暴露了一个读写锁接口。每一个锁获取是自动与正在运行的任务关联，并在任务结束时时自动释放锁
 * 不管任务是完成或失败。
 *
 * BlockInfoManager对block的锁管理采用了共享锁与排它锁，其中 读锁是共享锁，写锁是排他锁。
 *
 * This class is thread-safe.
 * 线程安全
 */
private[storage] class BlockInfoManager extends Logging {

  private type TaskAttemptId = Long

  /**
   * Used to look up metadata for individual blocks. Entries are added to this map via an atomic
   * set-if-not-exists operation ([[lockNewBlockForWriting()]]) and are removed
   * by [[removeBlock()]].
   * 用于查找一个单独block的元数据信息BlockInfo。map里面的元素通过lockNewBlockForWriting中的
   * set-if-not-exists自动的添加，通过removeBlock自动删除。
   */
  // blockId 与 BlockInfo 之间的映射关系的缓存
  /*因为BlockInfo中没有BlockId属性，所以需要存储它们的映射关系*/
  @GuardedBy("this")
  private[this] val infos = new mutable.HashMap[BlockId, BlockInfo]

  /**
   * Tracks the set of blocks that each task has locked for writing.
   */
  // 记录每个task获取了哪些block的写锁。 一对多
  @GuardedBy("this")
  private[this] val writeLocksByTask =
    new mutable.HashMap[TaskAttemptId, mutable.Set[BlockId]]
      with mutable.MultiMap[TaskAttemptId, BlockId]

  /**
   * Tracks the set of blocks that each task has locked for reading, along with the number of times
   * that a block has been locked (since our read locks are re-entrant).
   * 记录每个task获取了哪些block的读锁，以及一个block被此task获取的读锁次数。(因为我们的读锁是可重入的)
   */
  // 记录每个task获取了哪些block的读锁。一对多
  @GuardedBy("this")
  private[this] val readLocksByTask =
    new mutable.HashMap[TaskAttemptId, ConcurrentHashMultiset[BlockId]]
  //ConcurrentHashMultiset 允许有相同元素并且并发安全的Set
  /*说明一个taskAttempt可能多次获取了一个block的读锁*/

  // ----------------------------------------------------------------------------------------------

  // Initialization for special task attempt ids:
  // 初始化注册一个特殊的任务尝试id，BlockInfo.NON_TASK_WRITER
  registerTask(BlockInfo.NON_TASK_WRITER)

  // ----------------------------------------------------------------------------------------------

  /**
   * Called at the start of a task in order to register that task with this [[BlockInfoManager]].
   * This must be called prior to calling any other BlockInfoManager methods from that task.
   * 在启动一个task的时候被调用，为了将task注册到BlockInfoManager。
   * 此方法一定在那个那个task调用BlockInfoManager上的其他方法之前调用。
   */
  def registerTask(taskAttemptId: TaskAttemptId): Unit = synchronized {
    require(!readLocksByTask.contains(taskAttemptId),
      s"Task attempt $taskAttemptId is already registered")
    /*在readLocksByTask中注册taskAttemptId*/
    readLocksByTask(taskAttemptId) = ConcurrentHashMultiset.create()
  }

  /**
   * Returns the current task's task attempt id (which uniquely identifies the task), or
   * [[BlockInfo.NON_TASK_WRITER]] if called by a non-task thread.
   */
  /*获取当前正在执行的TaskAttemptId*/
  private def currentTaskAttemptId: TaskAttemptId = {
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(BlockInfo.NON_TASK_WRITER)
  }

  /**
   * Lock a block for reading and return its metadata.
   *
   * If another task has already locked this block for reading, then the read lock will be
   * immediately granted to the calling task and its lock count will be incremented.
   * 如果另外一个task已经获取此block的读锁，那么读锁将立即授予调用task，并增加其锁计数。
   *
   * If another task has locked this block for writing, then this call will block until the write
   * lock is released or will return immediately if `blocking = false`.
   *
   * A single task can lock a block multiple times for reading, in which case each lock will need
   * to be released separately.
   * 【一个单独的task可以为了读一个lock而锁定它多次，在这种情况下需要分别释放每个锁。】
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for reading).
   */
  // 【获取指定blockId的读锁，获取成功返回BlockInfo，否则返回None】
  def lockForReading(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire read lock for $blockId")
    do {
      /*1.从infos中获取BlockInfo*/
      infos.get(blockId) match {
        case None => return None
        case Some(info) =>
          // 如果此block写锁是解锁状态【因为读写互斥，读读共享，所以如果写锁被占用那么就不能获取到读锁】
          if (info.writerTask == BlockInfo.NO_WRITER) {
            // 此block的读锁个数+1
            info.readerCount += 1
            /*增加当前task获取的读锁的block*/
            readLocksByTask(currentTaskAttemptId).add(blockId)
            /*记录日志，$task获取了$blockid的读锁*/
            logTrace(s"Task $currentTaskAttemptId acquired read lock for $blockId")
            return Some(info)
          }
      }
      /*如果此block写锁是占用状态，阻塞等待，占有写锁的线程释放Block的写锁后唤醒当前线程，再次去获取读锁*/
      if (blocking) {
        wait()
      }
    } while (blocking)
    None
  }

  /**
   * Lock a block for writing and return its metadata.
   * 锁住一个block用于写 并且 返回block的BlockInfo
   * If another task has already locked this block for either reading or writing, then this call
   * will block until the other locks are released or will return immediately if `blocking = false`.
   * 如果另外一个task已经锁住了此block用于读或写，那么此次调用将被阻塞直到锁释放 或者 如果设置了`blocking = false`
   * 此次调用将立即返回。
   *
   * @param blockId the block to lock.
   * @param blocking if true (default), this call will block until the lock is acquired. If false,
   *                 this call will return immediately if the lock acquisition fails.
   * @return None if the block did not exist or was removed (in which case no lock is held), or
   *         Some(BlockInfo) (in which case the block is locked for writing).
   */
  // 【获取写锁】
  def lockForWriting(
      blockId: BlockId,
      blocking: Boolean = true): Option[BlockInfo] = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to acquire write lock for $blockId")
    do {
      // 从infos中获取blockid对应的BlockInfo
      infos.get(blockId) match {
        // 如果不存在返回None
        case None => return None
        case Some(info) =>
          /* 如果写锁没有被其他线程占用，且没有线程正在读取此block，
          则由当前任务尝试线程持有写锁并返回BlockInfo*/
          if (info.writerTask == BlockInfo.NO_WRITER && info.readerCount == 0) {
            /*记录写锁的taskAttemptId*/
            info.writerTask = currentTaskAttemptId
            /*增加当前task获取的写锁的block*/
            writeLocksByTask.addBinding(currentTaskAttemptId, blockId)
            logTrace(s"Task $currentTaskAttemptId acquired write lock for $blockId")
            return Some(info)
          }
      } // 说明有线程占用了此block的读锁或写锁，如果允许阻塞，那么阻塞当前线程，等待锁释放后，再去竞争写锁
      if (blocking) {
        wait()
      }
    } while (blocking)
    None
  }

  /**
   * Throws an exception if the current task does not hold a write lock on the given block.
   * Otherwise, returns the block's BlockInfo.
   */
  def assertBlockIsLockedForWriting(blockId: BlockId): BlockInfo = synchronized {
    infos.get(blockId) match {
      case Some(info) =>
        if (info.writerTask != currentTaskAttemptId) {
          throw new SparkException(
            s"Task $currentTaskAttemptId has not locked block $blockId for writing")
        } else {
          info
        }
      case None =>
        throw new SparkException(s"Block $blockId does not exist")
    }
  }

  /**
   * Get a block's metadata without acquiring any locks. This method is only exposed for use by
   * [[BlockManager.getStatus()]] and should not be called by other code outside of this class.
   * 获取blockId对应的BlockInfo
   */
  private[storage] def get(blockId: BlockId): Option[BlockInfo] = synchronized {
    infos.get(blockId)
  }

  /**
   * Downgrades an exclusive write lock to a shared read lock.
   * 锁降级。将一个独享的写锁降级成一个共享的读锁。
   */
  def downgradeLock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId downgrading write lock for $blockId")
    val info = get(blockId).get // 获取blockInfo
    // 只能降级当前自己线程的写锁，只能降级自己的锁
    require(info.writerTask == currentTaskAttemptId,
      s"Task $currentTaskAttemptId tried to downgrade a write lock that it does not hold on" +
        s" block $blockId")
    unlock(blockId) // 释放写锁
    // 获取读锁，非阻塞方式
    val lockOutcome = lockForReading(blockId, blocking = false)
    // 判断是否成功获取了读锁
    assert(lockOutcome.isDefined)
  }

  /**
   * Release a lock on the given block.
   * 释放一个block上的锁，可能是读锁，也可能是写锁，释放完后，通知所有等待此锁的线程
   */
  def unlock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId releasing lock for $blockId")
    /*1.从infos中获取BlockInfo*/
    val info = get(blockId).getOrElse {
      throw new IllegalStateException(s"Block $blockId not found")
    }
    if (info.writerTask != BlockInfo.NO_WRITER) { // 如果block的写锁被占用了，释放写锁
      info.writerTask = BlockInfo.NO_WRITER
      writeLocksByTask.removeBinding(currentTaskAttemptId, blockId)
    } else { //  如果block的读锁被占用了，释放读锁
      assert(info.readerCount > 0, s"Block $blockId is not locked for reading")
      // 此block的读锁数-1
      info.readerCount -= 1
      // 获取当前尝试线程已经获取那些block的读锁（是一个可重复元素，blockid 的set集合）
      val countsForTask = readLocksByTask(currentTaskAttemptId)
      // 从集合中移除一个blockId，返回remove之前集合中blockId的个数
      // 然后-1，如果大于0，说明当前线程多次获得了这个block的读锁
      val newPinCountForTask: Int = countsForTask.remove(blockId, 1) - 1
      /*如果小于0，抛出异常*/
      assert(newPinCountForTask >= 0,
        s"Task $currentTaskAttemptId release lock on block $blockId more times than it acquired it")
    }
    /*唤醒所有等待此锁的线程*/
    notifyAll()
  }

  /**
   * Attempt to acquire the appropriate lock for writing a new block.
   * 写新block时获取写锁。
   * This enforces the first-writer-wins semantics. If we are the first to write the block,
   * then just go ahead and acquire the write lock. Otherwise, if another thread is already
   * writing the block, then we wait for the write to finish before acquiring the read lock.
   *
   * @return true if the block did not already exist, false otherwise. If this returns false, then
   *         a read lock on the existing block will be held. If this returns true, a write lock on
   *         the new block will be held.
   *
   */
  def lockNewBlockForWriting(
      blockId: BlockId,
      newBlockInfo: BlockInfo): Boolean = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to put $blockId")
    lockForReading(blockId) match {
      case Some(info) =>
        // Block already exists. This could happen if another thread races with us to compute
        // the same block. In this case, just keep the read lock and return.
        // 成功获取到读锁，说明block已经存在了。如果另一个线程与我们竞争计算同一个block,这种情况就可能发生。
        // 在这种情况下，持有读锁，返回false。
        false
      case None =>
        // Block does not yet exist or is removed, so we are free to acquire the write lock
        // block不存在或者已经被移除了，因此我们可以自由地获取写锁
        // 保存blockId 和 BlockInfo的映射关系
        infos(blockId) = newBlockInfo
        // 获取写锁，返回
        lockForWriting(blockId)
        true
    }
  }

  /**
   * Release all lock held by the given task, clearing that task's pin bookkeeping
   * structures and updating the global pin counts. This method should be called at the
   * end of a task (either by a task completion handler or in `TaskRunner.run()`).
   * 释放taskAttemptId对应的task持有的所有的锁。
   * 此方法应该在task执行完后调用（要么通过任务完成handler 要么通过TaskRunner.run()）
   * @return the ids of blocks whose pins were released
   *         返回被释放锁的blockid的集合
   */
  def releaseAllLocksForTask(taskAttemptId: TaskAttemptId): Seq[BlockId] = {
    // 返回被释放锁的blockid的集合
    val blocksWithReleasedLocks = mutable.ArrayBuffer[BlockId]()

    // 从readLocksByTask删除此taskAttemptId，返回获取了哪些blockid的读锁，blockid集合
    val readLocks = synchronized {
      readLocksByTask.remove(taskAttemptId).get
    }
    // 从writeLocksByTask删除此taskAttemptId,返回获取了哪些blockid的写锁，blockid集合
    val writeLocks = synchronized {
      writeLocksByTask.remove(taskAttemptId).getOrElse(Seq.empty)
    }
    //遍历writeLocks，释放blockInfo的写锁
    for (blockId <- writeLocks) {
      infos.get(blockId).foreach { info =>
        assert(info.writerTask == taskAttemptId)
        info.writerTask = BlockInfo.NO_WRITER
      }
      blocksWithReleasedLocks += blockId
    }
    //遍历readLocks，释放blockInfo的读锁
    readLocks.entrySet().iterator().asScala.foreach { entry =>
      val blockId = entry.getElement
      val lockCount = entry.getCount
      blocksWithReleasedLocks += blockId
      synchronized {
        get(blockId).foreach { info =>
          info.readerCount -= lockCount
          assert(info.readerCount >= 0)
        }
      }
    }
    // 通知所有等待获取锁的线程
    synchronized {
      notifyAll()
    }
    // 返回被释放锁的blockid的集合
    blocksWithReleasedLocks
  }

  /**
   * Returns the number of blocks tracked.
   * 返回infos的大小，即所有block的数据。
   */
  def size: Int = synchronized {
    infos.size
  }

  /**
   * Return the number of map entries in this pin counter's internal data structures.
   * This is used in unit tests in order to detect memory leaks.
   */
  private[storage] def getNumberOfMapEntries: Long = synchronized {
    size +
      readLocksByTask.size +
      readLocksByTask.map(_._2.size()).sum +
      writeLocksByTask.size +
      writeLocksByTask.map(_._2.size).sum
  }

  /**
   * Returns an iterator over a snapshot of all blocks' metadata. Note that the individual entries
   * in this iterator are mutable and thus may reflect blocks that are deleted while the iterator
   * is being traversed.
   */
  // 以迭代器形式返回infos。
  def entries: Iterator[(BlockId, BlockInfo)] = synchronized {
    infos.toArray.toIterator
  }

  /**
   * Removes the given block and releases the write lock on it.
   *
   * This can only be called while holding a write lock on the given block.
   */
  // 移除blockId对应的BlockInfo
  def removeBlock(blockId: BlockId): Unit = synchronized {
    logTrace(s"Task $currentTaskAttemptId trying to remove block $blockId")
    infos.get(blockId) match { // 获取blockId对应的blockInfo
      case Some(blockInfo) =>
        // 如果对blockInfo正在写入的任务尝试线程是当前线程的话，当前线程才有权利去移除blockInfo
        if (blockInfo.writerTask != currentTaskAttemptId) {
          throw new IllegalStateException(
            s"Task $currentTaskAttemptId called remove() on block $blockId without a write lock")
        } else { // 移除BlockInfo
          // 将BlockInfo从infos中移除
          infos.remove(blockId)
          // 将blockInfo的读线程数清零
          blockInfo.readerCount = 0
          // 将blockInfo的writerTask置为BlockInfo.NO_WRITER
          blockInfo.writerTask = BlockInfo.NO_WRITER
          // 将任务尝试线程与blockid的关系清除
          writeLocksByTask.removeBinding(currentTaskAttemptId, blockId)
        }
      case None =>
        throw new IllegalArgumentException(
          s"Task $currentTaskAttemptId called remove() on non-existent block $blockId")
    }
    // 通知所有在blockId对应的Block的锁上等待的线程
    notifyAll()
  }

  /**
   * Delete all state. Called during shutdown.
   * 清除BlockInfoManager中所有信息，并通知所有在BlockInfoManager管理器的block的锁上等待的线程
   * 在停止系统过程中被调用。
   */
  def clear(): Unit = synchronized {
    infos.valuesIterator.foreach { blockInfo =>
      blockInfo.readerCount = 0
      blockInfo.writerTask = BlockInfo.NO_WRITER
    }
    infos.clear()
    readLocksByTask.clear()
    writeLocksByTask.clear()
    notifyAll()
  }

}
