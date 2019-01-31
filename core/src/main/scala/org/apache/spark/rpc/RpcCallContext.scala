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

package org.apache.spark.rpc

/**
 * A callback that [[RpcEndpoint]] can use to send back a message or failure. It's thread-safe
 * and can be called in any thread.
 * 一个回调函数，[[RpcEndpoint]]可以使用它来往回发消息或失败。线程安全的并且可以在任何线程中被调用。
 */
private[spark] trait RpcCallContext {

  /**
   * Reply a message to the sender. If the sender is [[RpcEndpoint]], its [[RpcEndpoint.receive]]
   * will be called.
   * 向发送者回复一条消息，如果sender是[[RpcEndpoint]]，它的[[RpcEndpoint.receive]]将被调用。
   */

  def reply(response: Any): Unit

  /**
   * Report a failure to the sender.
   * 向sender发送failure
   */
  def sendFailure(e: Throwable): Unit

  /**
   * The sender of this message.
   * sender的RpcAddress
   */
  def senderAddress: RpcAddress
}
