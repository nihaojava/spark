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

package org.apache.spark.unsafe.memory;

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.spark.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 * 使用jvm long原始数组，最多可以分配16g
 */
public class HeapMemoryAllocator implements MemoryAllocator {

  @GuardedBy("this")
  /*按照size（以size为key）大小缓存的MemoryBlock，池化机制为了复用BlockMemory*/
  private final Map<Long, LinkedList<WeakReference<MemoryBlock>>> bufferPoolsBySize =
    new HashMap<>();

  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024; /*1M*/

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   */
  /*是否需要采用池化机制，如果要分配的对象大于等于1M*/
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    /*非常小的分配不太可能从池中获益。*/
    return size >= POOLING_THRESHOLD_BYTES; /*1M*/
  }

  /*分配指定大小的MemoryBlock，size单位为Byte*/
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    if (shouldPool(size)) { /*判断是否需要池化*/
      synchronized (this) {
        final LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool != null) {
          while (!pool.isEmpty()) {
            final WeakReference<MemoryBlock> blockReference = pool.pop();
            final MemoryBlock memory = blockReference.get();
            if (memory != null) { /*在池中找到了指定大小的MemoryBlock，出队*/
              assert (memory.size() == size);
              return memory;
            }
          }
          /*在池中没有找到指定大小的MemoryBlock，移除*/
          bufferPoolsBySize.remove(size);
        }
      }
    }
    /*指定大小的MemoryBlock不需要采用池化机制 或者 池子中没有指定大小的MemoryBlock*/
    /*(size + 7) / 8)是为了解决jvm要求对象大小为8字节的整数倍，例如申请 2个字节,也会分配8字节，浪费6字节*/
    long[] array = new long[(int) ((size + 7) / 8)];
    MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return memory;
  }

  @Override
  public void free(MemoryBlock memory) {
    final long size = memory.size();
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }
    /*size大于等于1M*/
    if (shouldPool(size)) {
      synchronized (this) {
        LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool == null) {
          pool = new LinkedList<>();
          /*如果释放的中还没有size为key的对，put*/
          bufferPoolsBySize.put(size, pool);
        }
        /*如果释放的memroyBlock大小大于等于1M，则放入缓存池*/
        pool.add(new WeakReference<>(memory));
      }
    } else {/*size小于1M，什么都不做？*/
      // Do nothing
    }
  }
}
