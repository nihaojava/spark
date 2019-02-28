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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

/* ShuffleWriter的实现之一，提供了对Shuffle数据的排序功能。
*  使用ExternalSorter作为排序器，由于ExternalSorter底层使用了PartitionedAppendOnlyMap和PartitionedPairBuffer
*  两种缓存，因此sortShuffleWriter还支持对Shuffle数据的聚合功能。
*/
private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  // ShuffleDependency
  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager
  /*排序器*/
  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  // 我们正在停止吗？
  private var stopping = false
  /*map 任务的状态*/
  private var mapStatus: MapStatus = null

  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics

  /** Write a bunch of records to this task's output
    * 写一批此task的输出数据
    *
    * */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    // 创建ExternalSorter
    sorter = if (dep.mapSideCombine) {/*是否在map端聚合，必须要求定义了聚合器*/
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      /*在这种情况下，我们既不传递聚合器也不传递排序给排序器，因为我们不关心每个分区中的键是否排序；
      * 如果正在运行的操作是sortByKey，那么这将在reduce端完成。*/
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    // 将map任务的输出记录插入到缓存中【一条一条的写，可能溢写临时文件】
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    /*获取shuffleId的输出File*/
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    /*在此路径下new一个临时文件*/
    val tmp = Utils.tempFileWith(output)
    try {
      /*new一个ShuffleBlockId*/
      val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
      /*将产生的shuffle数据，持久化【溢写的和内存中的合并成一个文件【如果设置了聚合器和排序器，合并前要进行聚合和排序】】
      * 这里的排序器是指同一个分区id内再按照key进行排序【】*/
      val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
      /*根据上一步生成的data文件，创建index文件，【以便让reduce知道取哪一段数据】*/
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
      /*new MapStatus*/
      /*shuffleServerId也就是BlockManagerId 和 此map任务为各个分区输出的长度 Array[Long]*/
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
      }
    }
  }

  /** Close this writer, passing along whether the map completed */
  /*关闭writer，返回Option[MapStatus]*/
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    /*如果需要进行mapSideCombine，则无法绕过排序。*/
    if (dep.mapSideCombine) {/*必须定义aggregator聚合器*/
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      false
    } else {/*如果不需要进行mapSideCombine 且 分区个数小于等于200，则可以绕过排序*/
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
