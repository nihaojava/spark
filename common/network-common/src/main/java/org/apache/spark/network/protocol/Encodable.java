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

import io.netty.buffer.ByteBuf;

/**
 * Interface for an object which can be encoded into a ByteBuf. Multiple Encodable objects are
 * stored in a single, pre-allocated ByteBuf, so Encodables must also provide their length.
 * 此接口可以将一个object编码成一个ByteBuf。多个Encodable对象可以被存在一个单独的，预先分配的ByteBuf中，
 * 因此Encodables必须提供它们的长度。
 *
 * Encodable objects should provide a static "decode(ByteBuf)" method which is invoked by
 * {@link MessageDecoder}. During decoding, if the object uses the ByteBuf as its data (rather than
 * just copying data from it), then you must retain() the ByteBuf.
 * Encodable objects应当提供一个static "decode(ByteBuf)" method，它会被MessageDecoder调用。
 * 在decoding期间，
 *
 * Additionally, when adding a new Encodable Message, add it to {@link Message.Type}.
 * 进一步，当add一个新的Encodable Message的时候也需要将其添加到Message.Type。
 */
/*实现此接口可以将一个object编码成一个ByteBuf*/
public interface Encodable {
  /** Number of bytes of the encoded form of this object. */
  /*此对象的编码形式的字节数。*/
  int encodedLength();

  /**
   * Serializes this object by writing into the given ByteBuf.
   * This method must write exactly encodedLength() bytes.
   * 序列化此object到ByteBuf。
   * 这个方法必须准确地写入encodedLength()字节
   */
  void encode(ByteBuf buf);
}
