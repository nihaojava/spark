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

package org.apache.spark.rpc.netty

import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}

/*Inbox盒子内的消息*/
private[netty] sealed trait InboxMessage

private[netty] case class OneWayMessage(
    senderAddress: RpcAddress,
    content: Any) extends InboxMessage

private[netty] case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage

private[netty] case object OnStart extends InboxMessage

private[netty] case object OnStop extends InboxMessage

/** A message to tell all endpoints that a remote process has connected. */
private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a remote process has disconnected. */
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a network error has happened. */
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

/**
 * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
 * 收件箱，为[[RpcEndpoint]]存储消息并且向收件箱中发消息是线程安全的。
 */
private[netty] class Inbox(
    val endpointRef: NettyRpcEndpointRef,
    val endpoint: RpcEndpoint)
  extends Logging {

  inbox =>  // Give this an alias so we can use it more clearly in closures.
  /*起了个别名，因此我们可以在闭包中更清楚的使用它*/

  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  /** True if the inbox (and its associated endpoint) is stopped. */
  @GuardedBy("this")
  private var stopped = false

  /** Allow multiple threads to process messages at the same time. */
  /*是否允许同一时间多个线程处理同一个Inbox消息*/
  @GuardedBy("this")
  private var enableConcurrent = false

  /** The number of threads processing messages for this inbox. */
  /*处理此inbox中消息的激活线程数，正在处理messages中消息的线程数量*/
  @GuardedBy("this")
  private var numActiveThreads = 0

  // OnStart should be the first message to process
  /*启动的时候，向收件箱发送第一条消息OnStart*/
  inbox.synchronized {
    messages.add(OnStart)
  }

  /**
   * Process stored messages.
   * 处理消息
   */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    /*同步检查此inbox的并发处理线程，如果满足条件，当前线程获取message*/
    inbox.synchronized {
      /*进行线程并发检查。
      如果不允许多个线程处理同一个Inbox&&正在处理此inbox的线程不为0，
      所以当前线程不允许再去处理消息，返回，继续messageLoop，receivers下一条*/
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      /*从messages中取第一个消息*/
      message = messages.poll()
      if (message != null) {
        /*当前inbox的激活线程数+1*/
        numActiveThreads += 1
      } else {
        return
      }
    }
    /*消息匹配进行消息处理*/
    while (true) {
      safelyCall(endpoint) {
        message match {
          case RpcMessage(_sender, content, context) =>
            try {
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message $message from ${_sender}")
              })
            } catch {
              case NonFatal(e) =>
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayMessage(_sender, content) =>
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              throw new SparkException(s"Unsupported message $message from ${_sender}")
            })

          /*收到OnStart类型的消息，启动endpoint*/
          case OnStart =>
            endpoint.onStart()
            /*是否允许并发处理消息*/
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                }
              }
            }

          case OnStop =>
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            dispatcher.removeRpcEndpointRef(endpoint)
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      /*处理完inbox中一条消息后*/
      inbox.synchronized {
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        /*对激活线程数进行控制，如果不允许并发，并且激活线程数不为1，当前线程返回*/
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          /*我们不是唯一的worker，退出*/
          numActiveThreads -= 1
          return
        }
        /*再从此inbox中取一条message，如果没有消息了，那么当前线程返回，否则的话whil循环处理此消息*/
        /*也就是说，当线程处理完此inbox里面的消息，就返回，继续处理其他inbox的消息*/
        message = messages.poll()
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    } // end while
  }

  /*向inbox添加消息*/
  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      /*inbox已经停止了，删除此消息*/
      onDrop(message)
    } else {
      /*添加消息*/
      messages.add(message)
      false
    }
  }

  /*停止此inbxo，并向inbox中添加OnStop消息，用于停止响应的RpcEndpoint*/
  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    if (!stopped) {
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      /*先将enableConcurrent设为false，（考虑的是运行并发的情况）*/
      enableConcurrent = false
      /*设置当前inbox状态*/
      stopped = true
      /*messages添加OnStop消息*/
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  /**
   * Called when we are dropping a message. Test cases override this to test message dropping.
   * Exposed for testing.
   */
  /*删除message的时候记录日志*/
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(s"Drop $message because $endpointRef is stopped")
  }

  /**
   * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
   * 在处理消息发生错误的时候，当前inbox对应的RpcEndpoint的错误处理方法onError可以接收到这些错误信息。
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try action catch {
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) => logError(s"Ignoring error", ee)
        }
    }
  }

}
