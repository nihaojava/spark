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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.util.Utils

@DeveloperApi
class RDDInfo(
    val id: Int,
    var name: String,
    val numPartitions: Int,
    var storageLevel: StorageLevel,
    val parentIds: Seq[Int],
    val callSite: String = "",  /*RDD的用户调用栈信息*/
    val scope: Option[RDDOperationScope] = None)
  extends Ordered[RDDInfo] {

  var numCachedPartitions = 0 /*缓存的分区个数*/
  var memSize = 0L  /*使用的内存大小*/
  var diskSize = 0L /*使用的磁盘大小*/
  var externalBlockStoreSize = 0L /*Block存储在外部的大小*/

  /*是否已经缓存了*/
  def isCached: Boolean = (memSize + diskSize > 0) && numCachedPartitions > 0

  override def toString: String = {
    import Utils.bytesToString
    ("RDD \"%s\" (%d) StorageLevel: %s; CachedPartitions: %d; TotalPartitions: %d; " +
      "MemorySize: %s; DiskSize: %s").format(
        name, id, storageLevel.toString, numCachedPartitions, numPartitions,
        bytesToString(memSize), bytesToString(diskSize))
  }

  override def compare(that: RDDInfo): Int = {
    this.id - that.id
  }
}

/*根据rdd构造一个RDDInfo*/
private[spark] object RDDInfo {
  def fromRdd(rdd: RDD[_]): RDDInfo = {
    /*rdd名称，如果没有为rdd的simple类名*/
    val rddName = Option(rdd.name).getOrElse(Utils.getFormattedClassName(rdd))
    /*父rdd的Ids*/
    val parentIds = rdd.dependencies.map(_.rdd.id)
    /*new一个RDDInfo*/
    new RDDInfo(rdd.id, rddName, rdd.partitions.length,
      rdd.getStorageLevel, parentIds, rdd.creationSite.shortForm, rdd.scope)
  }
}
