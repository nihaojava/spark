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

import java.io.{FileInputStream, InputStream}
import java.util.{NoSuchElementException, Properties}

import scala.xml.{Node, XML}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.util.Utils

/**
 * An interface to build Schedulable tree
 * 用于构建可调度树的接口
 * buildPools: build the tree nodes(pools)
 * 构建池:构建树节点(池)
 * addTaskSetManager: build the leaf nodes(TaskSetManagers)
 * ddTaskSetManager:构建叶节点(tasksetmanager)
 */
private[spark] trait SchedulableBuilder {
  def rootPool: Pool  /*返回根调度池*/

  def buildPools(): Unit  /*对调度池进行构建*/

  /*向调度池添加TaskSetManager*/
  def addTaskSetManager(manager: Schedulable, properties: Properties): Unit
}

private[spark] class FIFOSchedulableBuilder(val rootPool: Pool)
  extends SchedulableBuilder with Logging {

  override def buildPools() {
    // nothing
  }
  /*重写addTaskSetManager，向rootPool中添加TaskSetManager*/
  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    rootPool.addSchedulable(manager)
  }
}

private[spark] class FairSchedulableBuilder(val rootPool: Pool, conf: SparkConf)
  extends SchedulableBuilder with Logging {

  /*用户指定的文件系统中的调度分配文件，用户自定义的*/
  val schedulerAllocFile = conf.getOption("spark.scheduler.allocation.file")
  val DEFAULT_SCHEDULER_FILE = "fairscheduler.xml"
  val FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.pool"
  val DEFAULT_POOL_NAME = "default"
  val MINIMUM_SHARES_PROPERTY = "minShare"
  val SCHEDULING_MODE_PROPERTY = "schedulingMode"
  val WEIGHT_PROPERTY = "weight"
  val POOL_NAME_PROPERTY = "@name"
  val POOLS_PROPERTY = "pool"
  val DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO
  val DEFAULT_MINIMUM_SHARE = 0 /*默认的minShare*/
  val DEFAULT_WEIGHT = 1 /*默认的weight*/

  /*重写buildPools，构建Pools*/
  override def buildPools() {
    var is: Option[InputStream] = None
    try {
      /*优先从文件系统读取schedulerAllocFile，如果用户未指定则默认从fairscheduler.xml文件读取*/
      is = Option {
        schedulerAllocFile.map { f =>
          new FileInputStream(f)
        }.getOrElse {
          Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE)
        }
      }
      /*从xml文件读取相应的配置参数，new Pool然后添加到rootPool中*/
      is.foreach { i => buildFairSchedulerPool(i) }
    } finally {
      is.foreach(_.close())
    }

    // finally create "default" pool
    /*最后，构建默认调度池并添加到rootPool*/
    buildDefaultPool()
  }
  /*构建默认调度池并添加到rootPool*/
  private def buildDefaultPool() {
    if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
      /*创建默认调度池*/
      val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      /*向根调度池的队列中添加默认的调度池*/
      rootPool.addSchedulable(pool)
      logInfo("Created default pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
    }
  }

  /*从xml文件读取相应的配置参数，new Pool然后添加到rootPool中*/
  private def buildFairSchedulerPool(is: InputStream) {
    val xml = XML.load(is)
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {

      val poolName = (poolNode \ POOL_NAME_PROPERTY).text

      val schedulingMode = getSchedulingModeValue(poolNode, poolName, DEFAULT_SCHEDULING_MODE)
      val minShare = getIntValue(poolNode, poolName, MINIMUM_SHARES_PROPERTY, DEFAULT_MINIMUM_SHARE)
      val weight = getIntValue(poolNode, poolName, WEIGHT_PROPERTY, DEFAULT_WEIGHT)

      rootPool.addSchedulable(new Pool(poolName, schedulingMode, minShare, weight))

      logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        poolName, schedulingMode, minShare, weight))
    }
  }

  private def getSchedulingModeValue(
      poolNode: Node,
      poolName: String,
      defaultValue: SchedulingMode): SchedulingMode = {

    val xmlSchedulingMode = (poolNode \ SCHEDULING_MODE_PROPERTY).text.trim.toUpperCase
    val warningMessage = s"Unsupported schedulingMode: $xmlSchedulingMode, using the default " +
      s"schedulingMode: $defaultValue for pool: $poolName"
    try {
      if (SchedulingMode.withName(xmlSchedulingMode) != SchedulingMode.NONE) {
        SchedulingMode.withName(xmlSchedulingMode)
      } else {
        logWarning(warningMessage)
        defaultValue
      }
    } catch {
      case e: NoSuchElementException =>
        logWarning(warningMessage)
        defaultValue
    }
  }

  private def getIntValue(
      poolNode: Node,
      poolName: String,
      propertyName: String, defaultValue: Int): Int = {

    val data = (poolNode \ propertyName).text.trim
    try {
      data.toInt
    } catch {
      case e: NumberFormatException =>
        logWarning(s"Error while loading scheduler allocation file. " +
          s"$propertyName is blank or invalid: $data, using the default $propertyName: " +
          s"$defaultValue for pool: $poolName")
        defaultValue
    }
  }
  /*添加TaskSetManager添加到默认的调度池default中*/
  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    var poolName = DEFAULT_POOL_NAME
    /*以名为default的调度池为TaskSetManager的父调度池*/
    var parentPool = rootPool.getSchedulableByName(poolName)
    if (properties != null) {
      /*如果指定了spark.scheduler.pool优先用该名称*/
      poolName = properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
      parentPool = rootPool.getSchedulableByName(poolName)
      if (parentPool == null) {
        // we will create a new pool that user has configured in app
        // instead of being defined in xml file
        parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
          DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
        rootPool.addSchedulable(parentPool)
        logInfo("Created pool %s, schedulingMode: %s, minShare: %d, weight: %d".format(
          poolName, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
      }
    }
    parentPool.addSchedulable(manager)
    logInfo("Added task set " + manager.name + " tasks to pool " + poolName)
  }
}
