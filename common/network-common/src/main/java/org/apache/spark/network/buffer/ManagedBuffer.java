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

package org.apache.spark.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This interface provides an immutable view for data in the form of bytes. The implementation
 * should specify how the data is provided:
 * 这个接口为字节形式的数据提供了一个不可变的视图。实现应指明如何提供数据。
 * 【不存数据，也不是数据的来源，同关系型数据库的视图类似】
 *
 * - {@link FileSegmentManagedBuffer}: data backed by part of a file 数据为文件的一部分
 * - {@link NioManagedBuffer}: data backed by a NIO ByteBuffer 数据为NIO的ByteBuffer
 * - {@link NettyManagedBuffer}: data backed by a Netty ByteBuf 数据为Netty ByteBuf
 *
 * The concrete buffer implementation might be managed outside the JVM garbage collector.
 * For example, in the case of {@link NettyManagedBuffer}, the buffers are reference counted.
 * In that case, if the buffer is going to be passed around to a different thread, retain/release
 * should be called.
 * 具体的缓冲区实现可能不受JVM垃圾收集器管理。例如，对于{@link NettyManagedBuffer}，缓冲区是引用计数的。
 * 在这种情况下，如果缓冲区将被传递到另一个线程，则retain/release应该调用。
 */
public abstract class ManagedBuffer {

  /** Number of bytes of the data. */
  public abstract long size();

  /**
   * Exposes this buffer's data as an NIO ByteBuffer. Changing the position and limit of the
   * returned ByteBuffer should not affect the content of this buffer.
   */
  // TODO: Deprecate this, usage may require expensive memory mapping or allocation.
  public abstract ByteBuffer nioByteBuffer() throws IOException;

  /**
   * Exposes this buffer's data as an InputStream. The underlying implementation does not
   * necessarily check for the length of bytes read, so the caller is responsible for making sure
   * it does not go over the limit.
   */
  public abstract InputStream createInputStream() throws IOException;

  /**
   * Increment the reference count by one if applicable.
   * 当有新的使用者使用此时图时，增加引用此视图的引用数。
   */
  public abstract ManagedBuffer retain();

  /**
   * If applicable, decrement the reference count by one and deallocates the buffer if the
   * reference count reaches zero.
   * 当使用者不再使用此时图时，减少引用此视图的引用数。当引用数为0时释放缓冲区。
   */
  public abstract ManagedBuffer release();

  /**
   * Convert the buffer into an Netty object, used to write the data out. The return value is either
   * a {@link io.netty.buffer.ByteBuf} or a {@link io.netty.channel.FileRegion}.
   *
   * If this method returns a ByteBuf, then that buffer's reference count will be incremented and
   * the caller will be responsible for releasing this new reference.
   */
  public abstract Object convertToNetty() throws IOException;
}
