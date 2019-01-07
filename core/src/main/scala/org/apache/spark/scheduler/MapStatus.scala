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

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on as well as the sizes of outputs for each reducer, for passing on to the reduce tasks.
 * ShuffleMapTask返回给调度器TaskScheduler的执行结果。包括了task运行的block manager的地址 和
 * 为每个reducer 输出的文件的大小，用于传递给reduce task。
 */
private[spark] sealed trait MapStatus {
  /** Location where this task was run. */
  // task运行的位置，即所在节点的BlockManager的身份标识，BlockManagerId。
  def location: BlockManagerId

  /**
   * Estimated size for the reduce block, in bytes.
   * 预测reduce需要拉取的block的大小，按Byte。
   *
   * If a block is non-empty, then this method MUST return a non-zero size.  This invariant is
   * necessary for correctness, since block fetchers are allowed to skip zero-size blocks.
   * 如果一个block不为空，那么这个方法一定返回一个非0的大小。这个不变量必须是准确的，
   * 因为block fetchers 将会跳过为0的block。
   */
  def getSizeForBlock(reduceId: Int): Long
}


private[spark] object MapStatus {

  // 如果未压缩的长度大于2000，使用HighlyCompressedMapStatus;否则使用CompressedMapStatus
  // 说明对于较大数据量使用高压缩，一般数据量使用普通压缩
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): MapStatus = {
    if (uncompressedSizes.length > 2000) {
      HighlyCompressedMapStatus(loc, uncompressedSizes)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes)
    }
  }

  private[this] val LOG_BASE = 1.1

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }
}


/**
 * A [[MapStatus]] implementation that tracks the size of each block. Size for each block is
 * represented using a single byte.
 *
 * @param loc location where the task is being executed.
 * @param compressedSizes size of the blocks, indexed by reduce partition id.
 */
private[spark] class CompressedMapStatus(
    private[this] var loc: BlockManagerId,
    private[this] var compressedSizes: Array[Byte])
  extends MapStatus with Externalizable {

  protected def this() = this(null, null.asInstanceOf[Array[Byte]])  // For deserialization only

  def this(loc: BlockManagerId, uncompressedSizes: Array[Long]) {
    this(loc, uncompressedSizes.map(MapStatus.compressSize))
  }

  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(compressedSizes(reduceId))
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    out.writeInt(compressedSizes.length)
    out.write(compressedSizes)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    val len = in.readInt()
    compressedSizes = new Array[Byte](len)
    in.readFully(compressedSizes)
  }
}

/**
 * A [[MapStatus]] implementation that only stores the average size of non-empty blocks,
 * plus a bitmap for tracking which blocks are empty.
 *
 * @param loc location where the task is being executed
 * @param numNonEmptyBlocks the number of non-empty blocks
 * @param emptyBlocks a bitmap tracking which blocks are empty
 * @param avgSize average size of the non-empty blocks
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var loc: BlockManagerId,
    private[this] var numNonEmptyBlocks: Int,
    private[this] var emptyBlocks: RoaringBitmap,
    private[this] var avgSize: Long)
  extends MapStatus with Externalizable {

  // loc could be null when the default constructor is called during deserialization
  require(loc == null || avgSize > 0 || numNonEmptyBlocks == 0,
    "Average size can only be zero for map stages that produced no output")

  protected def this() = this(null, -1, null, -1)  // For deserialization only

  override def location: BlockManagerId = loc

  override def getSizeForBlock(reduceId: Int): Long = {
    if (emptyBlocks.contains(reduceId)) {
      0
    } else {
      avgSize
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    loc.writeExternal(out)
    emptyBlocks.writeExternal(out)
    out.writeLong(avgSize)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    loc = BlockManagerId(in)
    emptyBlocks = new RoaringBitmap()
    emptyBlocks.readExternal(in)
    avgSize = in.readLong()
  }
}

private[spark] object HighlyCompressedMapStatus {
  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long]): HighlyCompressedMapStatus = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    var i = 0
    var numNonEmptyBlocks: Int = 0
    var totalSize: Long = 0
    // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
    // blocks. From a performance standpoint, we benefit from tracking empty blocks because
    // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
    val emptyBlocks = new RoaringBitmap()
    val totalNumBlocks = uncompressedSizes.length
    while (i < totalNumBlocks) {
      var size = uncompressedSizes(i)
      if (size > 0) {
        numNonEmptyBlocks += 1
        totalSize += size
      } else {
        emptyBlocks.add(i)
      }
      i += 1
    }
    val avgSize = if (numNonEmptyBlocks > 0) {
      totalSize / numNonEmptyBlocks
    } else {
      0
    }
    emptyBlocks.trim()
    emptyBlocks.runOptimize()
    new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize)
  }
}
