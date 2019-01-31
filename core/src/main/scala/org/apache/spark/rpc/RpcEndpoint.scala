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

import org.apache.spark.SparkException

/**
 * A factory class to create the [[RpcEnv]]. It must have an empty constructor so that it can be
 * created using Reflection.
 * 一个用于生产[[RpcEnv]]的工厂类。它一定要有一个空的构造函数，因为这样才能通过反射来创建。
 */
/*其中只包含一个方法，create*/
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 * RPC的端点，它定义了给定消息触发哪些函数。
 * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
 * 可以保证“onStart”、“receive”和“onStop”将被依次调用。
 * The life-cycle of an endpoint is:
 * 一个endpoint的生命周期：
 * constructor -> onStart -> receive* -> onStop
 *
 * Note: `receive` can be called concurrently. If you want `receive` to be thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
 * 注意： `receive`会被并发的调用。如果你想`receive`是线程安全的，请使用[[ThreadSafeRpcEndpoint]]。
 *
 * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
 * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
 * 如果从[[RpcEndpoint]]方法抛出任何错误，除了' onError '，' onError '也会被调用。
 * 如果' onError '抛出一个错误，[[RpcEnv]]将忽略它。
 */
private[spark] trait RpcEndpoint {

  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   * 此[[RpcEndpoint]]注册到的[[RpcEnv]]。
   */
  val rpcEnv: RpcEnv

  /**
   * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
   * called. And `self` will become `null` when `onStop` is called.
   * 这个[[RpcEndpointRef]]的[[RpcEndpoint]]。当调用“onStart”时，“self”将变得有效。
   * 当调用onStop时，self将变成null。
   * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
   * 注意：因为在`onStart`执行前， [[RpcEndpoint]]还没有注册也就没有有效的[[RpcEndpointRef]]。
   * 因此不可以在`onStart`前调用`self`。
   */
  /*获取RpcEndpoint的RpcEndpointRef*/
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
   * Process messages from [[RpcEndpointRef.send]] or [[RpcCallContext.reply)]]. If receiving a
   * unmatched message, [[SparkException]] will be thrown and sent to `onError`.
   */
  /*接收并处理消息，但不需要给客户端回复*/
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
   * Process messages from [[RpcEndpointRef.ask]]. If receiving a unmatched message,
   * [[SparkException]] will be thrown and sent to `onError`.
   */
  /*接收并处理消息，需要给客户端回复。回复是通过RpcCallContext来实现的*/
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
   * Invoked when any exception is thrown during handling messages.
   * 在处理消息发生异常时调用。
   */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
   * Invoked when `remoteAddress` is connected to the current node.
   * 当远端节点与当前节点连接上之后调用。
   */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when `remoteAddress` is lost.
   * 当远端节点与当前节点失去连接之后调用。
   */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
   * 当远端节点与当前节点之间出现网络问题时调用。
   */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked before [[RpcEndpoint]] starts to handle any message.
   * 在[[RpcEndpoint]]开始处理消息之前调用，可以在Rpc正式工作之前做一些准备工作。
   */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
   * use it to send or ask messages.
   * [[RpcEndpoint]]将要停止的时候被调用，可以做一些收尾工作。
   */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
   * A convenient method to stop [[RpcEndpoint]].
   * 一个stop [[RpcEndpoint]]的便捷方法。
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}

/**
 * A trait that requires RpcEnv thread-safely sending messages to it.
 * 一个特质，它要求RpcEnv线程安全向它发送消息。
 *
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
 * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
 * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
 * 线程安全意味着对于同一个ThreadSafeRpcEndpoint发送过来的消息，先来的消息的先处理。换句话说，修改
 * 一个[[ThreadSafeRpcEndpoint]]内部的字段当处理下一条消息的时候是可见的，在ThreadSafeRpcEndpoint里的字段
 * 不需要设置为volatile或者equivalent。
 *
 * However, there is no guarantee that the same thread will be executing the same
 * [[ThreadSafeRpcEndpoint]] for different messages.
 * 但是，不能保证相同的线程将执行相同的[[ThreadSafeRpcEndpoint]]用于不同的消息。？
 */
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint
