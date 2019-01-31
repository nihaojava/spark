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

package org.apache.spark.deploy

/*Driver的描述信息，
Master会将此发送给Worker，让worker来启动Driver*/
private[deploy] case class DriverDescription(
    jarUrl: String, //jar的Url
    mem: Int,
    cores: Int,
    supervise: Boolean, //是否监控（失败重启）
    command: Command) { //启动命令

  override def toString: String = s"DriverDescription (${command.mainClass})"
}
