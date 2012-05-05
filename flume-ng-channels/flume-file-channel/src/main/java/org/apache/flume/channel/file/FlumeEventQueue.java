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
package org.apache.flume.channel.file;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.tools.DirectMemoryUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Queue of events in the channel. This queue stores only
 * {@link FlumeEventPointer} objects which are represented
 * as 8 byte longs internally. Additionally the queue itself
 * of longs is stored as a {@link LongBuffer} in DirectMemory
 * (off heap).
 */
class FlumeEventQueue implements Writable {
  // XXX  We use % heavily which can be CPU intensive.
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory
  .getLogger(FlumeEventQueue.class);
  protected static final int VERSION = 1;
  protected static final int SIZE_OF_LONG = 8;
  protected static final int EMPTY = 0;
  protected final Map<Integer, AtomicInteger> fileIDCounts = Maps.newHashMap();
  protected final LongBuffer elements;
  protected final ByteBuffer backingBuffer;
  // both fields will be modified by multiple threads
  protected volatile int size;
  protected volatile int next;
  /**
   * @param capacity max event capacity of queue
   */
  FlumeEventQueue(int capacity) {
    Preconditions.checkArgument(capacity > 0, "Capacity must be greater than zero");
    backingBuffer = DirectMemoryUtils.allocate(capacity * SIZE_OF_LONG);
    elements = backingBuffer.asLongBuffer();
    for (int index = 0; index < elements.capacity(); index++) {
      elements.put(index, EMPTY);
    }
  }
  /**
   * Retrieve and remove the head of the queue.
   *
   * @return FlumeEventPointer or null if queue is empty
   */
  synchronized FlumeEventPointer removeHead() {
    if(size() == 0) {
      return null;
    }
    long value = remove(0);
    Preconditions.checkState(value != EMPTY);
    FlumeEventPointer ptr = FlumeEventPointer.fromLong(value);
    decrementFileID(ptr.getFileID());
    return ptr;
  }
  /**
   * Add a FlumeEventPointer to the head of the queue
   * @param FlumeEventPointer to be added
   * @return true if space was available and pointer was
   * added to the queue
   */
  synchronized boolean addHead(FlumeEventPointer e) {
    long value = e.toLong();
    Preconditions.checkArgument(value != EMPTY);
    if(add(0, value)) {
      incrementFileID(e.getFileID());
      return true;
    }
    return false;
  }
  /**
   * Add a FlumeEventPointer to the tail of the queue
   * this will normally be used when recovering from a
   * crash
   * @param FlumeEventPointer to be added
   * @return true if space was available and pointer
   * was added to the queue
   */
  synchronized boolean addTail(FlumeEventPointer e) {
    long value = e.toLong();
    Preconditions.checkArgument(value != EMPTY);
    if(add(size(), value)) {
      incrementFileID(e.getFileID());
      return true;
    }
    return false;
  }

  /**
   * Remove FlumeEventPointer from queue, will normally
   * only be used when recovering from a crash
   * @param FlumeEventPointer to be removed
   * @return true if the FlumeEventPointer was found
   * and removed
   */
  synchronized boolean remove(FlumeEventPointer e) {
    long value = e.toLong();
    Preconditions.checkArgument(value != EMPTY);
    for (int i = 0; i < size; i++) {
      if(get(i) == value) {
        remove(i);
        FlumeEventPointer ptr = FlumeEventPointer.fromLong(value);
        decrementFileID(ptr.getFileID());
        return true;
      }
    }
    return false;
  }
  /**
   * @return the set of fileIDs which are currently on the queue
   * will be normally be used when deciding which data files can
   * be deleted
   */
  synchronized Set<Integer> getFileIDs() {
    return new HashSet<Integer>(fileIDCounts.keySet());
  }
  /**
   * @return current size of the queue, not the capacity
   */
  synchronized int size() {
    return size;
  }
  protected void incrementFileID(int fileID) {
    AtomicInteger counter = fileIDCounts.get(fileID);
    if(counter == null) {
      counter = new AtomicInteger(0);
      fileIDCounts.put(fileID, counter);
    }
    counter.incrementAndGet();
  }

  protected void decrementFileID(int fileID) {
    AtomicInteger counter = fileIDCounts.get(fileID);
    Preconditions.checkState(counter != null);
    int count = counter.decrementAndGet();
    if(count == 0) {
      fileIDCounts.remove(fileID);
    }
  }

  protected long get(int index) {
    if (index < 0 || index > size - 1) {
      throw new IndexOutOfBoundsException(String.valueOf(index));
    }
    return elements.get(convert(index));
  }

  protected boolean add(int index, long value) {
    if (index < 0 || index > size) {
      throw new IndexOutOfBoundsException(String.valueOf(index));
    }
    if (size + 1 > elements.capacity()) {
      return false;
    }
    // shift right if element is not added at the right
    // edge of the array. the common case, add(size-1, value)
    // will result in no copy operations
    for (int k = size; k > index; k--) {
      elements.put(convert(k),
          elements.get(convert(k - 1)));
    }
    elements.put(convert(index), value);
    size++;
    return true;
  }

  protected long remove(int index) {
    if (index < 0 || index > size - 1) {
      throw new IndexOutOfBoundsException(String.valueOf(index));
    }
    long value = elements.get(convert(index));
    // shift left if element removed is not on the left
    // edge of the array. the common case, remove(0)
    // will result in no copy operations
    for (int k = index; k > 0; k--) {
      elements.put(convert(k),
          elements.get(convert(k - 1)));
    }
    elements.put(next % elements.capacity(), EMPTY);
    next = (next + 1) % elements.capacity();
    size--;
    return value;
  }

  protected int convert(int index) {
    return (next + index % elements.capacity()) % elements.capacity();
  }

  @Override
  public synchronized void readFields(DataInput input) throws IOException {
    int version = input.readInt();
    if(version != VERSION) {
      throw new IOException("Bad Version " + Integer.toHexString(version));
    }
    int length = input.readInt();
    for (int index = 0; index < length; index++) {
      long value = input.readLong();
      FlumeEventPointer ptr = FlumeEventPointer.fromLong(value);
      Preconditions.checkState(value != EMPTY);
      Preconditions.checkState(addHead(ptr), "Unable to add to queue");
    }
  }

  @Override
  public synchronized void write(DataOutput output) throws IOException {
    output.writeInt(VERSION);
    output.writeInt(size);
    for (int index = 0; index < size; index++) {
      long value = elements.get(convert(index));
      Preconditions.checkState(value != EMPTY);;
      output.writeLong(value);
    }
  }
  /**
   * @return max capacity of the queue
   */
  public int getCapacity() {
    return elements.capacity();
  }
}
