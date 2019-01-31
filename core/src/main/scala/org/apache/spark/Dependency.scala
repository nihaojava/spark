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

package org.apache.spark

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 * dependencies 的基类，只有一个方法rdd，返回当前依赖的RDD
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 * 是对那些一个子RDD的分区依赖一小部分父RDD的分区，这样的依赖的一个基类。
 * 窄依赖允许管道方式执行。
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * 根据子RDD的分区ID，获得所依赖的父RDD的分区列表。
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 * 表示对shuffle阶段输出的依赖。注意，一旦发生shuffle，此rdd就是瞬态的，因为我们在executor端不需要它。
 * @param _rdd the parent RDD 父RDD
 * @param partitioner partitioner used to partition the shuffle output 分区器
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If not set
 *                   explicitly then the default serializer, as specified by `spark.serializer`
 *                   config option, will be used. 序列化器
 * @param keyOrdering key ordering for RDD's shuffles 支持排序的Key
 * @param aggregator map/reduce-side aggregator for RDD's shuffle 聚合器
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine)
 * map端输出是否完成部分聚合
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],  /*要求RDD中的元素必须是Product2[K, V]及其子类*/
    val partitioner: Partitioner, /*分区器*/
    val serializer: Serializer = SparkEnv.get.serializer, /*java序列化器*/
    val keyOrdering: Option[Ordering[K]] = None, /*对key进行排序的排序器*/
    val aggregator: Option[Aggregator[K, V, C]] = None, /*对map任务的输出数据进行聚合的聚合器*/
    val mapSideCombine: Boolean = false)  /*是否在map端进行合并，默认为false*/
  extends Dependency[Product2[K, V]] {

  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  //key,value K，V的class类名
  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  /*结合器C的类名*/
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  /*当前ShuffleDependency的身份标识，val会在new的时候只执行一次，其他val属性也是*/
  val shuffleId: Int = _rdd.context.newShuffleId()
  // 将此ShuffleDependency注册到shuffleManager
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.length, this)

  /*将自己注册到cleaner，当自己被gc回收后，清理相关的shuffle的数据*/
  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 * 表示父RDD的分区和子RDD的分区 一对一 的依赖。
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  // 子RDD的分区依赖父RDD的对应分区，例如子RDD partition0 依赖 父RDD partition0
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD //父RDDrange的开始
 * @param outStart the start of the range in the child RDD //子RDDrange的开始
 * @param length the length of the range //range长度
 * 表示子RDD一段分区 和 父RDD一段分区是 一对一 的依赖关系。
 * 上面是 一个分区和一个分区，这里是一段分区 和 一段分区。
 * 【目前RangeDependency只用于 UnionRDD】
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
