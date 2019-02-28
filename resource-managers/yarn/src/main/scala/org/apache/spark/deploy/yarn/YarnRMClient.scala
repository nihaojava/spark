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

package org.apache.spark.deploy.yarn

import scala.collection.JavaConverters._

import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.webapp.util.WebAppUtils

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

/**
 * Handles registering and unregistering the application with the YARN ResourceManager.
 * 帮助向YARN ResourceManager 注册和取消注册 application。
 */
private[spark] class YarnRMClient extends Logging {

  private var amClient: AMRMClient[ContainerRequest] = _
  private var uiHistoryAddress: String = _
  private var registered: Boolean = false

  /**
   * Registers the application master with the RM.
   *
   * @param conf The Yarn configuration.
   * @param sparkConf The Spark configuration.
   * @param uiAddress Address of the SparkUI.
   * @param uiHistoryAddress Address of the application on the History Server.
   * @param securityMgr The security manager.
   * @param localResources Map with information about files distributed via YARN's cache.
   */
  /*向RM注册一个application master*/
  def register(
      driverUrl: String,
      driverRef: RpcEndpointRef,
      conf: YarnConfiguration,
      sparkConf: SparkConf,
      uiAddress: String,
      uiHistoryAddress: String,
      securityMgr: SecurityManager,
      localResources: Map[String, LocalResource]
    ): YarnAllocator = {
    amClient = AMRMClient.createAMRMClient()
    amClient.init(conf)
    amClient.start()
    this.uiHistoryAddress = uiHistoryAddress

    logInfo("Registering the ApplicationMaster")
    /*注册ApplicationMaster，调用amClient向RM的AMS发送registerApplicationMaster消息【主要包含host和port】*/
    synchronized {
      amClient.registerApplicationMaster(Utils.localHostName(), 0, uiAddress)
      registered = true
    }
    /*注册完成后创建YarnAllocator（ContainerAllocator）*/
    new YarnAllocator(driverUrl, driverRef, conf, sparkConf, amClient, getAttemptId(), securityMgr,
      localResources)
  }

  /**
   * Unregister the AM. Guaranteed to only be called once.
   * 取消注册AM，保证只调用一次。
   *
   * @param status The final status of the AM.
   * @param diagnostics Diagnostics message to include in the final status.
   */
  /*注销*/
  def unregister(status: FinalApplicationStatus, diagnostics: String = ""): Unit = synchronized {
    if (registered) {
      amClient.unregisterApplicationMaster(status, diagnostics, uiHistoryAddress)
    }
  }

  /** Returns the attempt ID. */
  /*返回ApplicationAttemptId*/
  def getAttemptId(): ApplicationAttemptId = {
    YarnSparkHadoopUtil.get.getContainerId.getApplicationAttemptId()
  }

  /** Returns the configuration for the AmIpFilter to add to the Spark UI. */
  def getAmIpFilterParams(conf: YarnConfiguration, proxyBase: String): Map[String, String] = {
    // Figure out which scheme Yarn is using. Note the method seems to have been added after 2.2,
    // so not all stable releases have it.
    val prefix = WebAppUtils.getHttpSchemePrefix(conf)
    val proxies = WebAppUtils.getProxyHostsAndPortsForAmFilter(conf)
    val hosts = proxies.asScala.map(_.split(":").head)
    val uriBases = proxies.asScala.map { proxy => prefix + proxy + proxyBase }
    Map("PROXY_HOSTS" -> hosts.mkString(","), "PROXY_URI_BASES" -> uriBases.mkString(","))
  }

  /** Returns the maximum number of attempts to register the AM. */
  /*如果不设置spark.yarn.maxAppAttempts，默认取yarn的设置（默认为AM尝试2次）*/
  def getMaxRegAttempts(sparkConf: SparkConf, yarnConf: YarnConfiguration): Int = {
    val sparkMaxAttempts = sparkConf.get(MAX_APP_ATTEMPTS).map(_.toInt)
    val yarnMaxAttempts = yarnConf.getInt(
      YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
    sparkMaxAttempts match {
      case Some(x) => if (x <= yarnMaxAttempts) x else yarnMaxAttempts
      case None => yarnMaxAttempts
    }
  }

}
