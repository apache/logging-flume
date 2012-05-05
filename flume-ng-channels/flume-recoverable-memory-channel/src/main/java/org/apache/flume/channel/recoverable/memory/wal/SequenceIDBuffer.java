/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.recoverable.memory.wal;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import org.apache.flume.tools.DirectMemoryUtils;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class SequenceIDBuffer implements IndexedSortable {
  private static final Logger LOG =
      LoggerFactory.getLogger(SequenceIDBuffer.class);
  private static final int SIZE_OF_LONG = 8;
  private LongBuffer buffer;
  private ByteBuffer backingBuffer;

  public SequenceIDBuffer(int size) {
    int bytesRequired = size * SIZE_OF_LONG;
    backingBuffer = DirectMemoryUtils.allocate(bytesRequired);
    buffer = backingBuffer.asLongBuffer();
  }

  @Override
  public int compare(int leftIndex, int rightIndex) {
    long left = get(leftIndex);
    long right = get(rightIndex);
    return (left < right ? -1 : (left == right ? 0 : 1));

  }

  public boolean exists(long value) {
    return binarySearch(value) >= 0;
  }

  private int binarySearch(long value) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = get(mid);

      if (midVal < value) {
        low = mid + 1;
      } else if (midVal > value) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -(low + 1); // key not found.
  }

  @Override
  public void swap(int leftIndex, int rightIndex) {
    long left = get(leftIndex);
    long right = get(rightIndex);
    put(leftIndex, right);
    put(rightIndex, left);
  }

  public long get(int index) {
    return buffer.get(index);
  }

  public void put(int index, long value) {
    buffer.put(index, value);
  }

  public int size() {
    return buffer.limit();
  }

  public void close() {
    try {
      DirectMemoryUtils.clean(backingBuffer);
    } catch (Exception e) {
      LOG.warn("Error cleaning up buffer", e);
      if (LOG.isDebugEnabled()) {
        Throwables.propagate(e);
      }
    }
  }

  public void sort() {
    QuickSort quickSort = new QuickSort();
    quickSort.sort(this, 0, size());
  }

  public static void main(String[] args) throws Exception {
    try {
      System.out.println("SequenceIDBuffer");
      SequenceIDBuffer buffer = new SequenceIDBuffer(13107200);
      buffer.close();
      System.out.println("Array");
      @SuppressWarnings("unused")
      long[] array = new long[13107200];
    } catch (Throwable t) {
      t.printStackTrace();
    } finally {
      Thread.sleep(Long.MAX_VALUE);
    }
  }
}
