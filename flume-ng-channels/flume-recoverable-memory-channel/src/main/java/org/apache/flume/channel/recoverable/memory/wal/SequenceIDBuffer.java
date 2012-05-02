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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.List;

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class SequenceIDBuffer implements IndexedSortable {
  private static final Logger LOG = LoggerFactory.getLogger(SequenceIDBuffer.class);
  private LongBuffer buffer;
  private ByteBuffer backingBuffer;

  public SequenceIDBuffer(int size) {
    long directMemorySize = getDirectMemorySize();
    int bytesRequired = size * 8;
    if((long)bytesRequired > directMemorySize) {
      LOG.warn("DirectMemorySize is " + directMemorySize +
          " and we require " + bytesRequired + ", allocate will likely faily.");
    }
    try {
      backingBuffer = ByteBuffer.allocateDirect(bytesRequired);
    } catch(OutOfMemoryError e) {
      LOG.error("DirectMemorySize is " + directMemorySize +
          " and we required " + bytesRequired +
          " via: -XX:MaxDirectMemorySize", e);
      throw e;
    }
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
      Preconditions.checkArgument(backingBuffer.isDirect(),
          "buffer isn't direct!");
      Method cleanerMethod = backingBuffer.getClass().getMethod("cleaner");
      cleanerMethod.setAccessible(true);
      Object cleaner = cleanerMethod.invoke(backingBuffer);
      Method cleanMethod = cleaner.getClass().getMethod("clean");
      cleanMethod.setAccessible(true);
      cleanMethod.invoke(cleaner);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  public void sort() {
    QuickSort quickSort = new QuickSort();
    quickSort.sort(this, 0, size());
  }

  /**
   * @return the setting of -XX:MaxDirectMemorySize as a long. Returns 0 if
   *         -XX:MaxDirectMemorySize is not set.
   */

  private static long getDirectMemorySize() {
    RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
    List<String> arguments = RuntimemxBean.getInputArguments();
    long multiplier = 1; //for the byte case.
    for (String s : arguments) {
      if (s.contains("-XX:MaxDirectMemorySize=")) {
        String memSize = s.toLowerCase()
            .replace("-xx:maxdirectmemorysize=", "").trim();

        if (memSize.contains("k")) {
          multiplier = 1024;
        }

        else if (memSize.contains("m")) {
          multiplier = 1048576;
        }

        else if (memSize.contains("g")) {
          multiplier = 1073741824;
        }
        memSize = memSize.replaceAll("[^\\d]", "");

        long retValue = Long.parseLong(memSize);
        return retValue * multiplier;
      }

    }
    // default from VM source code
    return Runtime.getRuntime().maxMemory();
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