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

package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NettyManagedBuffer;

/**
 * A generic RPC which is handled by a remote {@link org.apache.spark.network.server.RpcHandler}.
 * 一个通用的RPC，此消息类型被一个远端的服务端RpcHandler来处理。
 * This will correspond to a single
 * {@link org.apache.spark.network.protocol.ResponseMessage} (either success or failure).
 * 这将对应一个单独的ResponseMessage
 */
/*由服务端RpcHandler来处理，需要得到一个ResponseMessage回复信息*/
public final class RpcRequest extends AbstractMessage implements RequestMessage {
  /** Used to link an RPC request with its response. */
  public final long requestId;

  public RpcRequest(long requestId, ManagedBuffer message) {
    super(message, true);
    this.requestId = requestId;
  }

  @Override
  public Type type() { return Type.RpcRequest; }

  @Override
  public int encodedLength() {
    // The integer (a.k.a. the body size) is not really used, since that information is already
    // encoded in the frame length. But this maintains backwards compatibility with versions of
    // RpcRequest that use Encoders.ByteArrays.
    /*实际没有使用此方法*/
    return 8 + 4;
  }

  /*编码*/
  @Override
  public void encode(ByteBuf buf) {
    /*写request UUID，long 8个字节*/
    buf.writeLong(requestId);
    // See comment in encodedLength().
    /*写body长度，4个字节*/
    buf.writeInt((int) body().size());
  }

  /*解码*/
  public static RpcRequest decode(ByteBuf buf) {
    /*先读取requestId*/
    long requestId = buf.readLong();
    // See comment in encodedLength().
    /*读取body长度*/
    buf.readInt();
    return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RpcRequest) {
      RpcRequest o = (RpcRequest) other;
      return requestId == o.requestId && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("requestId", requestId)
      .add("body", body())
      .toString();
  }
}
