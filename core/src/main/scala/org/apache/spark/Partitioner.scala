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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.hashing.byteswap32

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.util.random.SamplingUtils

/**
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
 * Partitioner是定义了在key-value型的RDD中，如何根据Key来划分元素的一个对象。
 * 映射每个key到一个partition ID，ID从 0 到 `分区数-1`
 */
abstract class Partitioner extends Serializable {
  def numPartitions: Int    /*分区个数，也是下个stage中task的个数*/
  def getPartition(key: Any): Int
}

object Partitioner {
  /**
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
   * 选择一个partitioner用于 在多个RDD之间类似cogroup的操作。
   *
   * If any of the RDDs already has a partitioner, choose that one.
   * 如果任何一个RDD已经有partitioner了，就选择那个。
   *
   * Otherwise, we use a default HashPartitioner. For the number of partitions, if
   * spark.default.parallelism is set, then we'll use the value from SparkContext
   * defaultParallelism, otherwise we'll use the max number of upstream partitions.
   * 否则，我们使用一个默认的HashPartitioner。对于分区的个数，如果设置了spark.default.parallelism，
   * 则我们将通过SparkContext的defaultParallelism来使用这个参数，否则我们将使用 上游RDD的最大的的分区数。
   *
   * Unless spark.default.parallelism is set, the number of partitions will be the
   * same as the number of partitions in the largest upstream RDD, as this should
   * be least likely to cause out-of-memory errors.
   * 除非设置了spark.default.parallelism，否则分区数将会和 上游RDD的最大分区数一样，这样可以最小可能的引起OOM。
   *
   * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD。
   * 我们使用两个方法参数 (rdd, others)来保证调用者至少传递一个RDD。
   */
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val rdds = (Seq(rdd) ++ others)
    // hasPartitioner：帅选出Seq[RDD]中 partitioner的numPartitions大于0的RDD（也就是说存在partitioner且numPartitions>0）
    val hasPartitioner = rdds.filter(_.partitioner.exists(_.numPartitions > 0))
    if (hasPartitioner.nonEmpty) {
      //1.如果hasPartitioner:Seq[RDD]不为空,取partitioner中分区个数最大的，返回
      hasPartitioner.maxBy(_.partitions.length).partitioner.get
    } else {
      //2. 如果Seq[RDD]中没有分区器，则new一个默认的分区器HashPartitioner
      // HashPartitioner的参数分区个数：
      // 如果设置了spark.default.parallelism则取该值;如果没有则取Seq[RDD]中最大的分区个数
      if (rdd.context.conf.contains("spark.default.parallelism")) {
        new HashPartitioner(rdd.context.defaultParallelism)
      } else {
        new HashPartitioner(rdds.map(_.partitions.length).max)
      }
    }
  }
}

/**
 * A [[org.apache.spark.Partitioner]] that implements hash-based partitioning using
 * Java's `Object.hashCode`.
 * HashPartitioner 是一个基于hash值分区的Partitioner，hash值使用了java的Object.hashCode。
 *
 * Java arrays have hashCodes that are based on the arrays' identities rather than their contents,
 * so attempting to partition an RDD[Array[_]] or RDD[(Array[_], _)] using a HashPartitioner will
 * produce an unexpected or incorrect result.
 * java数组的hashCode是基于数组的唯一标示而不是数组的内容，所以尝试使用HashPartitioner分区一个
 * 以数组作为Key的RDD的时候，会产生和预期不一样或错误的结果。
 */
class HashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions
  // 根据传入的Key进行分区,返回分区ID。
  // key==null，返回0；其他=key.hashCode%numPartitions,如果结果为负则+numPartitions
  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }
  // 比较两个HashPartitioner是否相同，算法一样，所以就是比较分区个数是否相同就可以
  /*【重要】*/
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
  // HashPartitioner的哈希值=分区数
  override def hashCode: Int = numPartitions
}

/**
 * A [[org.apache.spark.Partitioner]] that partitions sortable records by range into roughly
 * equal ranges. The ranges are determined by sampling the content of the RDD passed in.
 * RangePartitioner是一个 按照范围分区 的分区器，将【可排序的记录】划分为粗略相等的区间。
 * @note The actual number of partitions created by the RangePartitioner might not be the same
 * as the `partitions` parameter, in the case where the number of sampled records is less than
 * the value of `partitions`.
 * 注意：RangePartitioner创建的实际分区数可能和传递的参数partitions不相等，
 * 这种情况，是因为抽样的记录数小于分区数。
 */
class RangePartitioner[K : Ordering : ClassTag, V](
    partitions: Int,  /*期望得到的分区个数*/
    rdd: RDD[_ <: Product2[K, V]],  /*当前rdd*/
    private var ascending: Boolean = true)
  extends Partitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  // 通过隐式转换，获取RDD中key类型数据对应的排序器Ordering[K]
  private var ordering = implicitly[Ordering[K]]

  // An array of upper bounds for the first (partitions - 1) partitions
  // 存储每个partition的上界值
  private var rangeBounds: Array[K] = {
    if (partitions <= 1) {
      // 如果给定的分区数是一个的情况下，直接返回一个空的集合，表示数据不进行分区
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      // 给定总的数据抽样大小，最多1M的数据量(10^6)，最少20倍的RDD分区数量，也就是每个RDD分区至少抽取20条数据
      val sampleSize = math.min(20.0 * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      // 计算每个分区抽取的数据量大小， 假设输入数据每个分区分布的大致均匀，稍微多采样一点
      // * 3.0 是为了对分区数据大的多采样一点
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      // 从rdd中抽取数据，返回值:(rdd总元素个数， Array[分区id，当前分区的元素个数，当前分区抽取的数据Array[T]])
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        // 如果总的数据量为0(RDD为空)，那么直接返回一个空的数组
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        // 如果某个分区的元素个数>>远远大于分区的平均元素个数，我们会重新采样保证那个分区有足够多的采样数据。
        // 计算总样本数量和总记录数的占比，占比最大为1.0，（也就是平均每个分区的采样比例）
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        // 保存样本数据的集合buffer
        val candidates = ArrayBuffer.empty[(K, Float)]
        // 保存数据分布不均衡的分区id(数据量超过fraction比率的分区)
        val imbalancedPartitions = mutable.Set.empty[Int]
        // 遍历抽取出来的样本数据，将分布均匀的和不均匀的分区分别存入对应的集合
        sketched.foreach { case (idx, n, sample) =>
          // 如果fraction乘以当前分区中的数据量大于之前计算的每个分区的抽象数据大小，
          // 那么表示当前分区抽取的数据太少了，该分区数据分布不均衡，需要重新抽取
          // 也就是说:(fraction * n)=该partition应该采样的平均值 > 先前计算的每个分区应该采样的值
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            // 当前分区分布均匀，计算占比权重，并添加到candidates集合中
            // 权重值 = 分区元素个数 / 该分区采样元素个数
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        // 对于数据分布不均衡的RDD分区，重新进行数据抽样
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          // 获取数据分布不均衡的RDD分区，并构成RDD
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          // 随机种子
          val seed = byteswap32(-rdd.id - 1)
          // 利用rdd的sample抽样函数API进行数据抽样
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        // 将最终的抽样数据计算出rangeBounds出来
        RangePartitioner.determineBounds(candidates, partitions)
      }
    }
  }
  // 边界数组长度+1 ？ 因为，举例：一个绳子剪3刀，会分成4段。类似的道理。会分成数组长度+1个分区。
  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      // 如果分区个数小于等于128，进行简单的搜索，遍历有序的边界数组rangeBounds，找到第一个大于k的元素下标，也就是partitionID
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      // 分区个数 > 128，使用二分查找,找到分区ID
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    //默认升序，返回partition
    if (ascending) {
      partition
    } else {
      //降序，返回 分区总数-partition
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

private[spark] object RangePartitioner {

  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   * 通过对输入RDD的每个partition进行水塘采样，描绘出RDD的草图（大概的数据情况）
   *
   * @param rdd the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   * 返回 （rdd中元素总个数，Array[(分区id，分区中的元素个数，该分区的采样数组Array[T])]）
   */
  def sketch[K : ClassTag](
      rdd: RDD[K],
      sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    // 对每个分区进行采样，collect到driver端，Array[(分区id，分区中的元素个数，该分区的采样数组Array[T]])
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      // 返回一个分区的（sample:采样数组Array[T],n:分区元素个数）
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      // 每个分区返回一个Iterator（分区id，分区中的元素个数，该分区的采样数组Array[T]）
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   * 从包含权重候选数组ArrayBuffer[(候选边界K, 权重Float)]中确定range partition划分的边界。
   * 通常情况下是
   * @param candidates unordered candidates with weights //未排序的ArrayBuffer[(待定边界值K, 权重Float)]
   * @param partitions number of partitions //期望分区数
   * @return selected bounds //确定的最终边界值Array[K]
   */
  def determineBounds[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],
      partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    // 候选数组 按照K升序排序
    val ordered = candidates.sortBy(_._1)
    // 候选数组 中元素个数
    val numCandidates = ordered.size
    // 总权重=候选数组中的各个权重和
    val sumWeights = ordered.map(_._2.toDouble).sum
    // 步长 = 总权重/分区数
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    //保存上一个边界的值
    var previousBound = Option.empty[K]
    // 遍历数组，j < partitions - 1，说明bounds数组元素的个数为最大为partitions - 1个
    while ((i < numCandidates) && (j < partitions - 1)) {
      // 获取排序后的第i个数据及权重
      val (key, weight) = ordered(i)
      // 累计权重
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        // 权重已经达到一个步长的范围，当前key做为当前分区的上界
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          // 上一个边界值为空，或者当前边界key数据大于上一个边界的值，那么当前key有效，进行计算
          // 添加当前key到边界集合中
          bounds += key
          // target每次增加一个步长
          target += step
          // 分区数量加1，一个分区边界已经确定了，进行下一个分区
          j += 1
          // 上一个边界的值重置为当前边界的值
          previousBound = Some(key)
        }
      }
      // i加1，待定数组的next
      i += 1
    }
    // 返回结果
    bounds.toArray
  }
}
