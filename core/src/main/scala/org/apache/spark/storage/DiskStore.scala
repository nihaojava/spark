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

import java.io.{FileOutputStream, IOException, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import com.google.common.io.Closeables

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Stores BlockManager blocks on disk.
 * 将block存储到磁盘
 */
private[spark] class DiskStore(conf: SparkConf, diskManager: DiskBlockManager) extends Logging {

  /*读取磁盘中的block时，是直接读取还是使用FileChannel的内存镜像映射方法读取的阈值，默认2M*/
  private val minMemoryMapBytes = conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")

  /*获取BlockId对应文件的大小，通过DiskBlockManager先获取File，再取length*/
  def getSize(blockId: BlockId): Long = {
    diskManager.getFile(blockId.name).length
  }

  /**
   * Invokes the provided callback function to write the specific block.
   * 调用提供的回调函数来写指定的块。
   * @throws IllegalStateException if the block already exists in the disk store.
   */
  def put(blockId: BlockId)(writeFunc: FileOutputStream => Unit): Unit = {
    /*调用contains方法判断给定blockid所对应的block文件是否存在，当不存在时进入下一步*/
    if (contains(blockId)) {
      throw new IllegalStateException(s"Block $blockId is already present in the disk store")
    }
    logDebug(s"Attempting to put block $blockId")
    val startTime = System.currentTimeMillis
    // 获取blockId所对应的文件file，并打开文件输出流
    val file = diskManager.getFile(blockId)
    val fileOutputStream = new FileOutputStream(file)
    var threwException: Boolean = true
    try {
      // 调用回调函数，对Block文件写入。
      writeFunc(fileOutputStream)
      threwException = false
    } finally {
      try {
        Closeables.close(fileOutputStream, threwException)
      } finally {
         if (threwException) {
          /*当写入失败时，还需要调用remove方法删除可能写入的部分文件*/
          remove(blockId)
        }
      }
    }
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName,
      Utils.bytesToString(file.length()),
      finishTime - startTime))
  }

  /*此方法用于将blockId所对应的Block写入磁盘，block的内容已经封装成了ChunkedByteBuffer*/
  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    put(blockId) { fileOutputStream =>
      val channel = fileOutputStream.getChannel
      Utils.tryWithSafeFinally {
        /*将ChunkedByteBuffer写到通向blockId对应文件的channel*/
        bytes.writeFully(channel)
      } {
        channel.close()
      }
    }
  }
  /*读取给定blockId所对应的Block，并封装为ChunkedByteBuffer*/
  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    val file = diskManager.getFile(blockId.name)
    val channel = new RandomAccessFile(file, "r").getChannel
    Utils.tryWithSafeFinally {
      // For small files, directly read rather than memory map
      // 对于小于minMemoryMapBytes的小文件，默认是2M，直接读取
      if (file.length < minMemoryMapBytes) {
        val buf = ByteBuffer.allocate(file.length.toInt)
        //置为0，从文件开头读
        channel.position(0)
        // 判buf是否还有剩余空间
        while (buf.remaining() != 0) {
          // 读入buf，并判断文件中的数据是否已经读完，没读完继续读
          if (channel.read(buf) == -1) {
            throw new IOException("Reached EOF before filling buffer\n" +
              s"offset=0\nfile=${file.getAbsolutePath}\nbuf.remaining=${buf.remaining}")
            /*抛出异常，文件已经读完，但是没有填充满缓冲区*/
          }
        }
        //切换写模式为度模式
        buf.flip()
        // 构造一个ChunkedByteBuffer
        new ChunkedByteBuffer(buf)
      } else {
        /*对于 >= 2M 的文件，使用内存文件映射*/
        new ChunkedByteBuffer(channel.map(MapMode.READ_ONLY, 0, file.length))
      }
    } {
      channel.close()
    }
  }

  /*用于删除给定blockId所对应的Block文件，删除成功返回true,文件不存在返回false*/
  def remove(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    if (file.exists()) {
      val ret = file.delete()
      if (!ret) {
        logWarning(s"Error deleting ${file.getPath()}")
      }
      ret
    } else {
      false
    }
  }

  /*判断blockId对应的物理文件是否存在*/
  def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    file.exists()
  }
}
