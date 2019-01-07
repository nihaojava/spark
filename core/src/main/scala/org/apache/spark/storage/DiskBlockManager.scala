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

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. One block is mapped to one file with a name given by its BlockId.
 * 创建和维护逻辑block和数据写入磁盘的物理位置之间的逻辑映射关系。
 * 一个块映射到一个文件，该文件的名称由它的BlockId给出（blockId中的name）。
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 * 块文件在spark.local.dir列出的目录中进行散列
 */
// deleteFilesOnStop:停止DiskBlockManager的时候是否删除本地目录的布尔类型标记
// 当不指定外部的ShuffleClient（即spark.shuffle.service.enabled属性为flase）或者 当前实例是Driver时，此属性为true。
private[spark] class DiskBlockManager(conf: SparkConf, deleteFilesOnStop: Boolean) extends Logging {
  // 每个新建的本地目录“blockmgr-$UUID”下的子目录的数量。默认64。
  private[spark] val subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level.
   * 为在spark.local.dir中提到的每个路径创建一个本地目录。然后，在这个目录里，创建多个子目录，我们将哈希文件放进去，
   * 为了避免在顶层有非常大的索引节点【？】。
   * */
  // 创建本地目录数组,在spark.local.dir提供的每个目录下，新建一个子目录
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  // subDirs是不可变的，但是subDirs(i)是可变的。（类似java中的final 的list，不可重新执行新的list引用，但是list内容可变）
  // subDirs(i)的内容受subDirs(i)的锁保护。

 /*创建一个subDirs: Array[localDirs.length][subDirsPerLocalDir] 本地子目录的二维数组，子目录个数为64*/
  /*Array.fill方法，返回一个Array，元素个数为localDirs.length，元素为new Array[File](subDirsPerLocalDir)*/
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  /*此属性的作用是在初始化DiskBlockManager时，调用addShutdownHook，设置关闭钩子*/
  private val shutdownHook = addShutdownHook()

  /** Looks up a file by hashing it into one of our local subdirectories. */
  /*通过将文件散列到我们的一个本地子目录中来查找它。*/
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  /*此方法根据指定的文件名获取文件*/
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    /*找出它散列到哪个本地目录，以及该目录中的哪个子目录
    * 获取文件名的非负哈希值，如果为负数取绝对值*/
    val hash = Utils.nonNegativeHash(filename)
    /*对localDirs取余，得到第一级目录id*/
    val dirId = hash % localDirs.length
    /*哈希值除以一级目录的大小获得商，然后用商数与subDirsPerLocalDir取余获取的余数作为选中的二级目录*/
    /*【二级目录为什么这样算，而不是hash%subDirsPerLocalDir？】*/
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // Create the subdirectory if it doesn't already exist
    /*获取二级目录。如果二级目录不存在，则需要创建二级目录。*/
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old
      } else {
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }
    /*返回new一个二级目录下的filename文件对象*/
    /*【此时，有可能文件存在，也有可能不存在】*/
    new File(subDir, filename)
  }

  /*根据blockId获取文件*/
  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** Check if disk block manager has a block. */
  /*检查是否包含对应的block文件*/
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /** List all the files currently stored on disk by the disk manager. */
  /*获取本地localDirs目录中的所有文件。为了线程安全采用了同步+克隆的方式*/
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirs.flatMap { dir =>
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        // 克隆dir里的内容，因为它的内容有可能被其他线程修改
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /** List all the blocks currently stored on disk by the disk manager. */
  /*获取本地localDirs目录中所有Block的BlockId*/
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().map(f => BlockId(f.getName))
  }

  /** Produces a unique block id and File suitable for storing local intermediate results. */
  /*为存储本地中间结果创建唯一的blockId 和 文件。*/
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
  /*创建唯一的blockId和文件，用来存储Shuffle中间结果*/
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   * 创建本地目录用来存储block数据。这些目录位于配置的本地目录下面，当使用外部的shuffle service时，
   * 在jvm退出的时候不会删除这些目录。
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    /*【体会flatMap的用法，因为里面元素是Some[File],所以要使用flatMap，这样的使用方式在spark中有很多】*/
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        // 在rootDir下创建一个名为“blockmgr-$UUID”子目录
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  /*添加关闭钩子*/
  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /** Cleanup local dirs and stop shuffle sender. */
  /*清空local dirs目录并且停止shuffle sender ?*/
  /*停止DiskBlockManager*/
  private[spark] def stop() {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    /*删除关闭钩子。如果我们不处理它，它会导致内存泄漏。*/
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  // 实际的stop方法
  private def doStop(): Unit = {
    if (deleteFilesOnStop) {
      localDirs.foreach { localDir =>
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            /*遍历localDirs数组中的一级目录，并调用工具类的deleteRecursively方法，递归删除*/
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}
