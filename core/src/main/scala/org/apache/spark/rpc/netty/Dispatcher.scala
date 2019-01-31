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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

/**
 * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
 *
 */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv) extends Logging {

  /*RPC端点的数据*/
  private class EndpointData(
      val name: String,
      val endpoint: RpcEndpoint,
      val ref: NettyRpcEndpointRef) {
    val inbox = new Inbox(ref, endpoint)
  }
  /*端点实例名称与端点数据EndpointData之间的映射关系*/
  private val endpoints: ConcurrentMap[String, EndpointData] =
    new ConcurrentHashMap[String, EndpointData]
  /*端点实例RpcEndpoint和RpcEndpointRef之间的映射关系*/
  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
    new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  // Track the receivers whose inboxes may contain messages.
  /*记录Inbox中的消息来自的端点的EndpointData*/
  private val receivers = new LinkedBlockingQueue[EndpointData]

  /**
   * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
   * immediately.
   * 如果dispatcher已停止，返回treu。一旦停止，所有发布的消息将被立即弹回。
   */
  @GuardedBy("this")
  private var stopped = false

  /*注册RpcEndpoint
  * 此方法会将EndpointData放入receivers*/
  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    val addr = RpcEndpointAddress(nettyEnv.address, name)
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
    synchronized {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }
      /*创建EndpointData并放入endpoints*/
      if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
        throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
      }
      val data = endpoints.get(name)
      /*将endpoint和ref映射关系方法endpointRefs*/
      endpointRefs.put(data.endpoint, data.ref)
      /*将EndpointData放入阻塞队列receivers的队尾*/
      receivers.offer(data)  // for the OnStart message
    }
    /*返回NettyRpcEndpointRef*/
    endpointRef
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  // Should be idempotent
  /*幂等的，取消注册的RpcEndpoint*/
  private def unregisterRpcEndpoint(name: String): Unit = {
    /*从endpoints中删除为name的endpointData*/
    val data = endpoints.remove(name)
    if (data != null) {
      /*停止inbox，并向此endpointData的inbox添加onStop消息*/
      data.inbox.stop()
      /*向receivers添加endpointData*/
      receivers.offer(data)  // for the OnStop message
    }
    // Don't clean `endpointRefs` here because it's possible that some messages are being processed
    // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
    // `removeRpcEndpointRef`.
  }

  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      /*判断Dispatcher是否已经停止，如果已停止返回；未停止执行unregisterRpcEndpoint*/
      if (stopped) {
        // This endpoint will be stopped by Dispatcher.stop() method.
        return
      }
      /*执行取消RpcEndpoint注册*/
      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  /**
   * Send a message to all registered [[RpcEndpoint]]s in this process.
   * 向所有已经注册的RpcEndpoint发送一个消息。
   * This can be used to make network events known to all end points (e.g. "a new node connected").
   * 这用在产生一个网络事件让所有的end points都知道。（例如 一个新节点的连接成功）
   */
  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
      postMessage(name, message, (e) => logWarning(s"Message $message dropped. ${e.getMessage}"))
    }
  }

  /** Posts a message sent by a remote endpoint. */
  /*向远端endpoint发送一个message*/
  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    val rpcCallContext =
      new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  /** Posts a message sent by a local endpoint. */
  /*向本地endpoint发送一个message*/
  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    val rpcCallContext =
      new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  /** Posts a one-way message. */
  /*发送一个单向消息*/
  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
      (e) => throw e)
  }

  /**
   * Posts a message to a specific endpoint.
   * 将消息交给指定的endpoint。【放到它的inbox】
   *
   * @param endpointName name of the endpoint.
   * @param message the message to post
   * @param callbackIfStopped callback function if the endpoint is stopped.
   */
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      /*根据endpointName得到endpointData*/
      val data = endpoints.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (data == null) {
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        /*加入inbox的messages中*/
        data.inbox.post(message)
        /*加入receivers中*/
        receivers.offer(data)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    error.foreach(callbackIfStopped)
  }

  /*停止此Dispatcher*/
  def stop(): Unit = {
    synchronized {
      if (stopped) {
        return
      }
      stopped = true
    }
    // Stop all endpoints. This will queue all endpoints for processing by the message loops.
    /*停止所有endpoints*/
    endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
    // Enqueue a message that tells the message loops to stop.
    /*向receivers投递“毒药”，用于停止message loops线程*/
    receivers.offer(PoisonPill)
    /*关闭线程池*/
    threadpool.shutdown()
  }

  /*当使用awaitTermination时，主线程会处于一种等待的状态，等待线程池中所有的线程都运行完毕后才继续运行*/
  def awaitTermination(): Unit = {
    threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  /**
   * Return if the endpoint exists
   */
  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }

  /** Thread pool used for dispatching messages. */
  /*线程池用于分发消息*/
  private val threadpool: ThreadPoolExecutor = {
    /*获取线程池的大小，默认取运行时可用core数和2的最大值*/
    val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",
      math.max(2, Runtime.getRuntime.availableProcessors()))
    /*创建固定个数的线程池，启动的线程以后台（守护Daemon）以线程方式运行。*/
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
    /*启动numThreads个运行MessageLoop任务的线程*/
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  /** Message loop used for dispatching messages. */
  /*循环过程中对新消息进行处理*/
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            /*从receivers中获取EndpointData。EndpointData中inbox中有了新消息。
            * 由于receivers是个阻塞队列，所以当receivers没有没有EndpointData时，MessageLoop线程会阻塞*/
            val data = receivers.take()
            /*如果取到的是“毒药”，那么此MessageLoop线程将退出。*/
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              /*将”毒药“重新放回receivers，这样别的MessageLoops也能吃到*/
              receivers.offer(PoisonPill)
              return
            }
            /*如果取到的不是”毒药“，那么调用EndpointData中inbox的process方法对消息进行处理*/
            data.inbox.process(Dispatcher.this)
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  /*一个“毒”endpoint，指示MessageLoop应该退出其消息循环。*/
  private val PoisonPill = new EndpointData(null, null, null)
}
