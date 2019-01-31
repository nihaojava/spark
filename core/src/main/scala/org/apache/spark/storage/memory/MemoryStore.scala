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

package org.apache.spark.storage.memory

import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.serializer.{SerializationStream, SerializerManager}
import org.apache.spark.storage.{BlockId, BlockInfoManager, StorageLevel, StreamBlockId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

// 代表block在内存中的形式
private sealed trait MemoryEntry[T] {
  def size: Long // 当前block的大小
  def memoryMode: MemoryMode // block存入内存的内存模式
  def classTag: ClassTag[T] // block的类型标记
}
private case class DeserializedMemoryEntry[T]( //反序列化后的MemoryEntry,只能存储在heap上
    value: Array[T],
    size: Long,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  val memoryMode: MemoryMode = MemoryMode.ON_HEAP
}
private case class SerializedMemoryEntry[T]( //序列化后的MemoryEntry
    buffer: ChunkedByteBuffer, //ByteBuffer数组
    memoryMode: MemoryMode,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  def size: Long = buffer.size
}

private[storage] trait BlockEvictionHandler {
  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   * 从内存中删除一个block，如果合适可能将其放到disk。当memory存储达到它的上限，并且需要释放空间的时候被调用。
   *
   * If `data` is not put on disk, it won't be created.
   * 如果数据不能放到磁盘，BlockEvictionHandler不会被创建。
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   * 该方法的调用方必须在调用该方法之前在块上持有写锁。
   * 此方法不会释放写锁。
   *
   * @return the block's new effective StorageLevel.
   *         返回此block新的StorageLevel
   */
  private[storage] def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel
}

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 * MemoryStore负责将block存储到内存。可以存储反序列化Java对象的数组，也可以存储序列化后的bytebuffer。
 * spark通过将广播数据、RDD、Shuffle数据存储到内存 ？，减少了对磁盘IO的依赖，提高了程序的读写效率。
 */
/*【展开内存：可以理解为为了存储block预留（提前申请）的内存】*/
private[spark] class MemoryStore(
    conf: SparkConf,
    blockInfoManager: BlockInfoManager,   // 即block信息管理器
    serializerManager: SerializerManager, // 序列化管理器
    memoryManager: MemoryManager, // 内存管理器,MemoryStore存储block，
    // 使用的就是MemoryManager的maxOnHeapStorageMemory和maxOffHeapStorageMemory两个内存池
    blockEvictionHandler: BlockEvictionHandler) //block驱逐处理器，用于将block从内存中驱逐出去。
    // 实际是blockManager，实现了blockEvictionHandler接口，重写了其中的驱逐块方法dropFromMemory
  extends Logging {

  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!
  // 内存分配的所有修改，特别是放置块、回收块和 获取或释放展开内存，必须在' memoryManager '上同步!

  // 内存中bockid与memoryEntry（block的内存形式）之间映射关系的缓存。
  private val entries = new LinkedHashMap[BlockId, MemoryEntry[_]](32, 0.75f, true)

  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
  // 任务尝试线程的标识taskAttemptId与任务尝试线程在堆内存展开的所有block占用的内存大小之和的映射关系。
  private val onHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()
  // Note: off-heap unroll memory is only used in putIteratorAsBytes() because off-heap caching
  // always stores serialized values.
  // 任务尝试线程的标识taskAttemptId与任务尝试线程在堆外内存展开的所有block占用的内存大小之和的映射关系。
  private val offHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // Initial memory to request before unrolling any block
  // 展开任何block之前，请求的初始内存大小，默认为1M。
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

  /** Total amount of memory available for storage, in bytes. */
  // MemoryStore用于存储block的最大内存，其实质为memoryManager的maxOnHeapStorageMemory和maxOffHeapStorageMemory之和。
  // 如果memoryManager为staticMemoryManager，那么maxMemory的大小是固定的。
  // 如果MemoryManager为UnifiedMemoryManager，那么maxMemory的大小是动态变化的。
  private def maxMemory: Long = {
    memoryManager.maxOnHeapStorageMemory + memoryManager.maxOffHeapStorageMemory
  }

  /*如果用于存储block的最大内存 小于 初始化的展开内存，警告，需要为spark配置更多的内存*/
  /*【展开内存：可以理解为为了存储block预留（提前申请）的内存】*/
  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }
  /*记录日志，MemoryStore启动，容量为maxMemory*/
  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /** Total storage memory used including unroll memory, in bytes.
    * MemoryStore中已经使用的内存大小。其实质为memoryManager中onHeapStorageMemoryPool已使用的大小
    * 和offHeapStorageMemoryPool已使用的大小之和。包含展开内存。
    * */
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
   * Amount of storage memory, in bytes, used for caching blocks.
   * This does not include memory used for unrolling.
   * 用于存储block（即MemoryEntry）使用的内存大小，memoryUsed 减去 currentUnrollMemory 。
   * 不包含展开内存。
   */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    memoryUsed - currentUnrollMemory
  }

  // 获取blockid对应block所占用的大小
  def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  /**
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
   *
   * @return true if the put() succeeded, false otherwise.
   * 将blockid对应的block（已封装为ChunkedByteBuffer）写入内存。
   */
  /*【将block存储到内存，就是传入block对应的数据new一个MemoryEntry，
    然后存储到entries（linkedHashMap）中 blockid 和 MemoryEntry 的对应关系】*/
  def putBytes[T: ClassTag](
      blockId: BlockId,
      size: Long,
      memoryMode: MemoryMode,
      _bytes: () => ChunkedByteBuffer): Boolean = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    // 从memoryManager中获取用于存储blockid对应的block的内存，如果获取失败返回false，否则进入下一步。
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {
      // We acquired enough memory for the block, so go ahead and put it
      // 调用_bytes函数,获取block的数据，即ChunkedByteBuffer
      val bytes = _bytes()
      assert(bytes.size == size)
      // 创建block对应的SerializedMemoryEntry
      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      // 将SerializedMemoryEntry放入entries
      entries.synchronized {
        entries.put(blockId, entry) //将block存入内存
      }
      /*记录日志：此block存储到memory（预测 占用的内存size，剩余的存储空间maxMemory - blocksMemoryUsed）*/
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      true
    } else { /*申请存储空间失败，返回false*/
      false
    }
  }

  /**
   * Attempt to put the given block in memory store as values.
   * 尝试将给定的block放入内存存储，作为值【不序列化Array[T]】
   * It's possible that the iterator is too large to materialize and store in memory. To avoid
   * OOM exceptions, this method will gradually unroll the iterator while periodically checking
   * whether there is enough free memory. If the block is successfully materialized, then the
   * temporary unroll memory used during the materialization is "transferred" to storage memory,
   * so we won't acquire more memory than is actually needed to store the block.
   * 迭代器可能太大而无法实例化和存储到内存中。为了避免OOM异常，此方法会周期性的检查展开内存是否充足。
   * 如果block成功实例化，那么在实例化过程使用的临时展开内存会"transferred"转到存储内存，这样我们
   * 获取的内存不会比存储此block实际需要的内存多。
   * @return in case of success, the estimated size of the stored data. In case of failure, return
   *         an iterator containing the values of the block. The returned iterator will be backed
   *         by the combination of the partially-unrolled block and the remaining elements of the
   *         original input iterator. The caller must either fully consume this iterator or call
   *         `close()` on it in order to free the storage memory consumed by the partially-unrolled
   *         block.
   *         一旦成功，估算存储数据的大小。一旦失败，返回一个包含此block内容的迭代器。返回的迭代器包含
   *         部分展开块和部分iterator中原始的元素。调用者必须完全消费此iterator或者调用close方法，为了
   *         释放展开部分block消耗的内存。
   */
  private[storage] def putIteratorAsValues[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // Number of elements unrolled so far
    /*到目前为止展开的元素个数*/
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    /*是否有充足的内存继续展开此block*/
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes).
    /*展开block之前的初始化内存（默认为1m）*/
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    /*检查内存是否足够的周期，指的不是时间，而是已经展开的元素数量*/
    val memoryCheckPeriod = 16
    // Memory currently reserved by this task for this particular unrolling operation
    /*当前任务用于展开block所保留的内存*/
    var memoryThreshold = initialMemoryThreshold
    /*展开内存不足时，请求增长的因子。当前内存的倍数*/
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Keep track of unroll memory used by this particular block / putIterator() operation
    /*block已经使用的展开内存大小*/
    var unrollMemoryUsedByThisBlock = 0L
    // Underlying vector for unrolling the block
    /*用于存储block（即iterator）每次迭代的数据。*/
    var vector = new SizeTrackingVector[T]()(classTag)

    // Request enough memory to begin unrolling
    // 申请充足的内存开始展开block
    keepUnrolling =
      reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, MemoryMode.ON_HEAP)

    if (!keepUnrolling) { // 申请失败
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {  // 申请成功，block已使用的展开内存 增加 initialMemoryThreshold
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    // 展开此block安全，周期性的检查是否超过阀值
    while (values.hasNext && keepUnrolling) {
      vector += values.next()
      if (elementsUnrolled % memoryCheckPeriod == 0) {  //每展开16个元素，检查
        // If our vector's size has exceeded the threshold, request more memory
        val currentSize = vector.estimateSize() //预估当前vector的大小（安字节）
        if (currentSize >= memoryThreshold) { // 当前vector的大小 大于等于 内存阀值
          /*计算需要扩容的大小：当前vector的大小*增长因子1.5 - 当前阀值*/
          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          /*按需要扩容的大小，申请预留内存*/
          keepUnrolling =
            reserveUnrollMemoryForThisTask(blockId, amountToRequest, MemoryMode.ON_HEAP)
          if (keepUnrolling) {
            /*扩容成功，已使用的展开内存增加 amountToRequest*/
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // New threshold is currentSize * memoryGrowthFactor
          /*更新 阀值*/
          memoryThreshold += amountToRequest
        }
      }
      /*已展开元素+1*/
      elementsUnrolled += 1
    }
    /*申请到足够多的展开内存，数据已经全部展开到vector*/
    if (keepUnrolling) {
      // We successfully unrolled the entirety of this block
      /*我们成功的展开了整个block*/
      val arrayValues = vector.toArray
      vector = null
      /*构造DeserializedMemoryEntry，其中SizeEstimator.estimate(arrayValues)为预估arrayValues的大小*/
      val entry =
        new DeserializedMemoryEntry[T](arrayValues, SizeEstimator.estimate(arrayValues), classTag)
      val size = entry.size
      /*将展开block的内存转换为存储block的内存*/
      def transferUnrollToStorage(amount: Long): Unit = {
        // Synchronize so that transfer is atomic
        /*Synchronize，所以此方法是原子的*/
        memoryManager.synchronized {
          /*释放amount数量的展开内存*/
          releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, amount)
          /*申请amount数量的存储内存*/
          val success = memoryManager.acquireStorageMemory(blockId, amount, MemoryMode.ON_HEAP)
          assert(success, "transferring unroll memory to storage memory failed")
        }
      }
      // Acquire storage memory if necessary to store this block in memory.
      val enoughStorageMemory = {
        if (unrollMemoryUsedByThisBlock <= size) { // 已使用的展开内存 <= block的大小
          val acquiredExtra =
            memoryManager.acquireStorageMemory(
              blockId, size - unrollMemoryUsedByThisBlock, MemoryMode.ON_HEAP)
          if (acquiredExtra) {
            transferUnrollToStorage(unrollMemoryUsedByThisBlock)
          }
          acquiredExtra
        } else { // unrollMemoryUsedByThisBlock > size ，归还多余的展开内存
          // If this task attempt already owns more unroll memory than is necessary to store the
          // block, then release the extra memory that will not be used.
          val excessUnrollMemory = unrollMemoryUsedByThisBlock - size
          releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, excessUnrollMemory)
          transferUnrollToStorage(size)
          true
        }
      }
      /*如果有足够的内存存储block，则将blockId与DeserializedMemoryEntry的映射关系放入entries，并返回Right(size)*/
      if (enoughStorageMemory) {
        entries.synchronized {
          entries.put(blockId, entry)
        }
        logInfo("Block %s stored as values in memory (estimated size %s, free %s)".format(
          blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
        Right(size)
      } else {
      /*如果没有充足的存储内存，则创建PartiallyUnrolledIterator，并返回Left*/
        assert(currentUnrollMemoryForThisTask >= unrollMemoryUsedByThisBlock,
          "released too much unroll memory")
        Left(new PartiallyUnrolledIterator(
          this,
          MemoryMode.ON_HEAP,
          unrollMemoryUsedByThisBlock,  // 已使用的展开内存
          unrolled = arrayValues.toIterator,  // 此时已全部展开到vector
          rest = Iterator.empty)) // 剩余的数据为空
      }
    } else {
    // keepUnrolling为false，说明没有为block申请到足够多的保留内存，此时只展开了一部分，vector中只有values部分数据
    // 创建PartiallyUnrolledIterator
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, vector.estimateSize())
      Left(new PartiallyUnrolledIterator(
        this,
        MemoryMode.ON_HEAP,
        unrollMemoryUsedByThisBlock, // 已使用的展开内存
        unrolled = vector.iterator, //已经展开的iterator
        rest = values)) // 剩余的iterator
    }
  }

  /**
   * Attempt to put the given block in memory store as bytes.
   * 尝试将给定的block放入内存存储，存储为字节【序列化】
   * It's possible that the iterator is too large to materialize and store in memory. To avoid
   * OOM exceptions, this method will gradually unroll the iterator while periodically checking
   * whether there is enough free memory. If the block is successfully materialized, then the
   * temporary unroll memory used during the materialization is "transferred" to storage memory,
   * so we won't acquire more memory than is actually needed to store the block.
   *
   * @return in case of success, the estimated size of the stored data. In case of failure,
   *         return a handle which allows the caller to either finish the serialization by
   *         spilling to disk or to deserialize the partially-serialized block and reconstruct
   *         the original input iterator. The caller must either fully consume this result
   *         iterator or call `discard()` on it in order to free the storage memory consumed by the
   *         partially-unrolled block.
   */
  /*以序列化后的字节数组方式，将blockId对应的Block（已经转换为Iterator）写入内存*/
  private[storage] def putIteratorAsBytes[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode): Either[PartiallySerializedBlock[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    val allocator = memoryMode match {
      case MemoryMode.ON_HEAP => ByteBuffer.allocate _
      case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
    }

    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes).
    val initialMemoryThreshold = unrollMemoryThreshold
    // Keep track of unroll memory used by this particular block / putIterator() operation
    var unrollMemoryUsedByThisBlock = 0L
    // Underlying buffer for unrolling the block
    val redirectableStream = new RedirectableOutputStream
    val chunkSize = if (initialMemoryThreshold > Int.MaxValue) {
      logWarning(s"Initial memory threshold of ${Utils.bytesToString(initialMemoryThreshold)} " +
        s"is too large to be set as chunk size. Chunk size has been capped to " +
        s"${Utils.bytesToString(Int.MaxValue)}")
      // 最大值Int.MaxValue 2^31-1 约等于2G
      Int.MaxValue
    } else {
      initialMemoryThreshold.toInt
    }
    val bbos = new ChunkedByteBufferOutputStream(chunkSize, allocator)
    redirectableStream.setOutputStream(bbos)
    val serializationStream: SerializationStream = {
      val autoPick = !blockId.isInstanceOf[StreamBlockId]
      val ser = serializerManager.getSerializer(classTag, autoPick).newInstance()
      ser.serializeStream(serializerManager.wrapStream(blockId, redirectableStream))
    }

    // Request enough memory to begin unrolling
    keepUnrolling = reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, memoryMode)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    def reserveAdditionalMemoryIfNecessary(): Unit = {
      if (bbos.size > unrollMemoryUsedByThisBlock) {
        val amountToRequest = bbos.size - unrollMemoryUsedByThisBlock
        keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
        if (keepUnrolling) {
          unrollMemoryUsedByThisBlock += amountToRequest
        }
      }
    }

    // Unroll this block safely, checking whether we have exceeded our threshold
    while (values.hasNext && keepUnrolling) {
      serializationStream.writeObject(values.next())(classTag)
      reserveAdditionalMemoryIfNecessary()
    }

    // Make sure that we have enough memory to store the block. By this point, it is possible that
    // the block's actual memory usage has exceeded the unroll memory by a small amount, so we
    // perform one final call to attempt to allocate additional memory if necessary.
    if (keepUnrolling) {
      serializationStream.close()
      reserveAdditionalMemoryIfNecessary()
    }

    if (keepUnrolling) {
      val entry = SerializedMemoryEntry[T](bbos.toChunkedByteBuffer, memoryMode, classTag)
      // Synchronize so that transfer is atomic
      memoryManager.synchronized {
        releaseUnrollMemoryForThisTask(memoryMode, unrollMemoryUsedByThisBlock)
        val success = memoryManager.acquireStorageMemory(blockId, entry.size, memoryMode)
        assert(success, "transferring unroll memory to storage memory failed")
      }
      entries.synchronized {
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(entry.size),
        Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      Right(entry.size)
    } else {
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, bbos.size)
      Left(
        new PartiallySerializedBlock(
          this,
          serializerManager,
          blockId,
          serializationStream,
          redirectableStream,
          unrollMemoryUsedByThisBlock,
          memoryMode,
          bbos,
          values,
          classTag))
    }
  }
  /*从内存中读取blockId对应的序列化后的block（已经封装为ChunkedByteBuffer）*/
  /*此方法只能获取序列化后的block*/
  def getBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: DeserializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getBytes on serialized blocks")
      case SerializedMemoryEntry(bytes, _, _) => Some(bytes)
    }
  }
  /*从内存中读取blockId对应的未序列化的block（已经封装为iterator）*/
  /*此方法只能获取未序列化的block*/
  def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: SerializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getValues on deserialized blocks")
      case DeserializedMemoryEntry(values, _, _) =>
        val x = Some(values)
        x.map(_.iterator) // 将Some[Array[_]]转换为Some[Iterator[_]]
    }
  }
  /*从内存中移除blockId对应的Block*/
  def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    /*【从条目中删除 此BlockId，对应的MemoryEntry引用链就断了，引用不可达，gc的时候就可以回收了】
    * 【这就是从内存中删除】*/
    val entry = entries.synchronized {
      entries.remove(blockId)
    }
    if (entry != null) {
      entry match {
          /*如果要移除的序列化后的block，还要将对应的ChunkedByteBuffer清理*/
        case SerializedMemoryEntry(buffer, _, _) => buffer.dispose()
        case _ =>
      }
      /*释放存储内存*/
      memoryManager.releaseStorageMemory(entry.size, entry.memoryMode)
      logDebug(s"Block $blockId of size ${entry.size} dropped " +
        s"from memory (free ${maxMemory - blocksMemoryUsed})")
      true
    } else {
      false
    }
  }

  /*用于清空MemoryStore*/
  def clear(): Unit = memoryManager.synchronized {
    entries.synchronized {
      entries.clear()
    }
    onHeapUnrollMemoryMap.clear()
    offHeapUnrollMemoryMap.clear()
    memoryManager.releaseAllStorageMemory()
    logInfo("MemoryStore cleared")
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   * 根据blockid返回rddid
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  /**
   * Try to evict blocks to free up a given amount of space to store a particular block.
   * Can fail if either the block is bigger than our memory or it would require replacing
   * another block from the same RDD (which leads to a wasteful cyclic replacement pattern for
   * RDDs that don't fit into memory that we want to avoid).
   * 尝试驱逐block从而释放给定大小的空间，用来存储指定的block。如果block大于我们的内存大小或者请求替换
   *（驱逐）同一个RDD中的另外一个block，（对于不适合存储到内存中的RDD，这样会导致不划算的循环替换模式，
   * 这是我们想避免的。）这两种情况下会失败。
   *
   * @param blockId the ID of the block we are freeing space for, if any // 我们为此block释放空间
   * @param space the size of this block  // block大小
   * @param memoryMode the type of memory to free (on- or off-heap) // memory类型
   * @return the amount of memory (in bytes) freed by eviction // 返回通过驱逐释放的内存大小（Byte）
   */
  /**/
  private[spark] def evictBlocksToFreeSpace(
      blockId: Option[BlockId],
      space: Long,  // 需要驱逐block所腾出的内存大小
      memoryMode: MemoryMode): Long = {
    assert(space > 0)
    memoryManager.synchronized {
      var freedMemory = 0L  // 已释放的内存大小
      val rddToAdd = blockId.flatMap(getRddId) // 获取RDDID
      val selectedBlocks = new ArrayBuffer[BlockId] // 已经选择的用于驱逐的Block的BlockId的数组
      /*判断哪些block符合驱逐条件*/
      def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
        // memoryMode一样 && （block对应的不是RDD 或者 block对应的和要存储的block不是同一个RDD）
        entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
      }
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      entries.synchronized {
        val iterator = entries.entrySet().iterator()
        /*当freedMemory小于space时，不断迭代遍历iterator*/
        while (freedMemory < space && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair.getKey
          val entry = pair.getValue
          /*帅选出符合条件的blockid们*/
          if (blockIsEvictable(blockId, entry)) {
            // We don't want to evict blocks which are currently being read, so we need to obtain
            // an exclusive write lock on blocks which are candidates for eviction. We perform a
            // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
            // 获取写锁【非阻塞获取blocking = false】，是为了不对有线程正在读或写的block进行驱逐
            if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
              /*将blockId加入待驱逐列表，更新可释放的内存freedMemory*/
              selectedBlocks += blockId
              freedMemory += pair.getValue.size
            }
          }
        }
      }
      /*删除block*/
      def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit = {
        /*判断是哪种类型的MemoryEntry*/
        val data = entry match {
          case DeserializedMemoryEntry(values, _, _) => Left(values)
          case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
        }
        /*调用BlockManager的dropFromMemory，如果storageLevel中useDisk=true，
        那就将此block写到disk，然后再从内存中删除；
        否则直接从删除此block*/
        val newEffectiveStorageLevel =
          blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
        /*根据新的StorageLevel判断是移到其他存储了，还是删除了*/
        if (newEffectiveStorageLevel.isValid) {
          // The block is still present in at least one store, so release the lock
          // but don't delete the block info
          /*此block已被已到其他store，因此释放锁，但是不删除block info*/
          blockInfoManager.unlock(blockId)
        } else {
          // The block isn't present in any store, so delete the block info so that the
          // block can be stored again
          /*此block已经被删除，同时删除block info*/
          blockInfoManager.removeBlock(blockId)
        }
      }
      /*经过遍历entries（已缓存的block），帅选出符合驱逐条件的block后，可释放内存 >= 需要的内存*/
      if (freedMemory >= space) {
        /*记录日志：释放几个block，释放掉多少内存*/
        logInfo(s"${selectedBlocks.size} blocks selected for dropping " +
          s"(${Utils.bytesToString(freedMemory)} bytes)")
        /*遍历符合驱逐条件的block，进行释放*/
        for (blockId <- selectedBlocks) {
          val entry = entries.synchronized { entries.get(blockId) }
          // This should never be null as only one task should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry != null) {
            /*删除block*/
            dropBlock(blockId, entry)
          }
        }
        logInfo(s"After dropping ${selectedBlocks.size} blocks, " +
          s"free memory is ${Utils.bytesToString(maxMemory - blocksMemoryUsed)}")
        freedMemory
      } else {
        /*可释放内存 < 需要的内存*/
        blockId.foreach { id =>
          logInfo(s"Will not store $id")
        }
        /*释放符合驱逐条件的block的锁*/
        selectedBlocks.foreach { id =>
          blockInfoManager.unlock(id)
        }
        0L
      }
    }
  }

  /*判断本地MemoryStore中是否包含给定BlockId所对应的Block文件*/
  def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Reserve memory for unrolling the given block for this task.
   * 为task展开给定的block预留内存
   * @return whether the request is granted.
   *         返回是否预留成功
   */
  def reserveUnrollMemoryForThisTask(
      blockId: BlockId,
      memory: Long,
      memoryMode: MemoryMode): Boolean = {
    memoryManager.synchronized {
      val success = memoryManager.acquireUnrollMemory(blockId, memory, memoryMode)
      // 如果申请成功，继续执行；否则返回false
      if (success) {
        /*获取当前taskAttemptId线程*/
        val taskAttemptId = currentTaskAttemptId()
        val unrollMemoryMap = memoryMode match {
          case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
          case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
        }
        /*更新当前taskAttemptId线程 所占用的展开内存*/
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      success
    }
  }

  /**
   * Release memory used by this task for unrolling blocks.
   * If the amount is not specified, remove the current task's allocation altogether.
   * 释放此task的展开内存
   * 如果没有指定数量，则完全删除当前任务所分配的
   */
  def releaseUnrollMemoryForThisTask(memoryMode: MemoryMode, memory: Long = Long.MaxValue): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      val unrollMemoryMap = memoryMode match {
        case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
        case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
      }
      if (unrollMemoryMap.contains(taskAttemptId)) {
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {  //释放展开内存
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          /*其实还是释放memoryManager中storagePool中的*/
          memoryManager.releaseUnrollMemory(memoryToRelease, memoryMode)
        }
        if (unrollMemoryMap(taskAttemptId) == 0) {
        // 如果taskAttemptId所使用的展开内存为0，从unrollMemoryMap中删除此task
          unrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
   * 用于展开block使用的内存大小。
   */
  def currentUnrollMemory: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.values.sum + offHeapUnrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this task.
   * 当前的任务尝试线程用于展开block所占用的内存。
   */
  def currentUnrollMemoryForThisTask: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L) +
      offHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
   * Return the number of tasks currently unrolling blocks.
   * 当前展开block的task数量。
   */
  private def numTasksUnrolling: Int = memoryManager.synchronized {
    (onHeapUnrollMemoryMap.keys ++ offHeapUnrollMemoryMap.keys).toSet.size
  }

  /**
   * Log information about current memory usage.
   */
  private def logMemoryUsage(): Unit = {
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
      s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  private def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsValues()]] call.
 *
 * @param memoryStore  the memoryStore, used for freeing memory.
 * @param memoryMode   the memory mode (on- or off-heap).
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param unrolled     an iterator for the partially-unrolled values.
 * @param rest         the rest of the original iterator passed to
 *                     [[MemoryStore.putIteratorAsValues()]].
 */
private[storage] class PartiallyUnrolledIterator[T](
    memoryStore: MemoryStore,
    memoryMode: MemoryMode,
    unrollMemory: Long,
    private[this] var unrolled: Iterator[T],
    rest: Iterator[T])
  extends Iterator[T] {

  private def releaseUnrollMemory(): Unit = {
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    // SPARK-17503: Garbage collects the unrolling memory before the life end of
    // PartiallyUnrolledIterator.
    unrolled = null
  }

  override def hasNext: Boolean = {
    if (unrolled == null) {
      rest.hasNext
    } else if (!unrolled.hasNext) {
      releaseUnrollMemory()
      rest.hasNext
    } else {
      true
    }
  }

  override def next(): T = {
    if (unrolled == null || !unrolled.hasNext) {
      rest.next()
    } else {
      unrolled.next()
    }
  }

  /**
   * Called to dispose of this iterator and free its memory.
   */
  def close(): Unit = {
    if (unrolled != null) {
      releaseUnrollMemory()
    }
  }
}

/**
 * A wrapper which allows an open [[OutputStream]] to be redirected to a different sink.
 */
private[storage] class RedirectableOutputStream extends OutputStream {
  private[this] var os: OutputStream = _
  def setOutputStream(s: OutputStream): Unit = { os = s }
  override def write(b: Int): Unit = os.write(b)
  override def write(b: Array[Byte]): Unit = os.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
  override def flush(): Unit = os.flush()
  override def close(): Unit = os.close()
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsBytes()]] call.
 *
 * @param memoryStore the MemoryStore, used for freeing memory.
 * @param serializerManager the SerializerManager, used for deserializing values.
 * @param blockId the block id.
 * @param serializationStream a serialization stream which writes to [[redirectableOutputStream]].
 * @param redirectableOutputStream an OutputStream which can be redirected to a different sink.
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param memoryMode whether the unroll memory is on- or off-heap
 * @param bbos byte buffer output stream containing the partially-serialized values.
 *                     [[redirectableOutputStream]] initially points to this output stream.
 * @param rest         the rest of the original iterator passed to
 *                     [[MemoryStore.putIteratorAsValues()]].
 * @param classTag the [[ClassTag]] for the block.
 */
private[storage] class PartiallySerializedBlock[T](
    memoryStore: MemoryStore,
    serializerManager: SerializerManager,
    blockId: BlockId,
    private val serializationStream: SerializationStream,
    private val redirectableOutputStream: RedirectableOutputStream,
    val unrollMemory: Long,
    memoryMode: MemoryMode,
    bbos: ChunkedByteBufferOutputStream,
    rest: Iterator[T],
    classTag: ClassTag[T]) {

  private lazy val unrolledBuffer: ChunkedByteBuffer = {
    bbos.close()
    bbos.toChunkedByteBuffer
  }

  // If the task does not fully consume `valuesIterator` or otherwise fails to consume or dispose of
  // this PartiallySerializedBlock then we risk leaking of direct buffers, so we use a task
  // completion listener here in order to ensure that `unrolled.dispose()` is called at least once.
  // The dispose() method is idempotent, so it's safe to call it unconditionally.
  Option(TaskContext.get()).foreach { taskContext =>
    taskContext.addTaskCompletionListener { _ =>
      // When a task completes, its unroll memory will automatically be freed. Thus we do not call
      // releaseUnrollMemoryForThisTask() here because we want to avoid double-freeing.
      unrolledBuffer.dispose()
    }
  }

  // Exposed for testing
  private[storage] def getUnrolledChunkedByteBuffer: ChunkedByteBuffer = unrolledBuffer

  private[this] var discarded = false
  private[this] var consumed = false

  private def verifyNotConsumedAndNotDiscarded(): Unit = {
    if (consumed) {
      throw new IllegalStateException(
        "Can only call one of finishWritingToStream() or valuesIterator() and can only call once.")
    }
    if (discarded) {
      throw new IllegalStateException("Cannot call methods on a discarded PartiallySerializedBlock")
    }
  }

  /**
   * Called to dispose of this block and free its memory.
   */
  def discard(): Unit = {
    if (!discarded) {
      try {
        // We want to close the output stream in order to free any resources associated with the
        // serializer itself (such as Kryo's internal buffers). close() might cause data to be
        // written, so redirect the output stream to discard that data.
        redirectableOutputStream.setOutputStream(ByteStreams.nullOutputStream())
        serializationStream.close()
      } finally {
        discarded = true
        unrolledBuffer.dispose()
        memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
      }
    }
  }

  /**
   * Finish writing this block to the given output stream by first writing the serialized values
   * and then serializing the values from the original input iterator.
   */
  def finishWritingToStream(os: OutputStream): Unit = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    ByteStreams.copy(unrolledBuffer.toInputStream(dispose = true), os)
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    redirectableOutputStream.setOutputStream(os)
    while (rest.hasNext) {
      serializationStream.writeObject(rest.next())(classTag)
    }
    serializationStream.close()
  }

  /**
   * Returns an iterator over the values in this block by first deserializing the serialized
   * values and then consuming the rest of the original input iterator.
   *
   * If the caller does not plan to fully consume the resulting iterator then they must call
   * `close()` on it to free its resources.
   */
  def valuesIterator: PartiallyUnrolledIterator[T] = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // Close the serialization stream so that the serializer's internal buffers are freed and any
    // "end-of-stream" markers can be written out so that `unrolled` is a valid serialized stream.
    serializationStream.close()
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    val unrolledIter = serializerManager.dataDeserializeStream(
      blockId, unrolledBuffer.toInputStream(dispose = true))(classTag)
    // The unroll memory will be freed once `unrolledIter` is fully consumed in
    // PartiallyUnrolledIterator. If the iterator is not consumed by the end of the task then any
    // extra unroll memory will automatically be freed by a `finally` block in `Task`.
    new PartiallyUnrolledIterator(
      memoryStore,
      memoryMode,
      unrollMemory,
      unrolled = unrolledIter,
      rest = rest)
  }
}
