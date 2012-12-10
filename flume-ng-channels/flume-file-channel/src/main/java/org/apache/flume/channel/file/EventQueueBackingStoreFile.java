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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;


abstract class EventQueueBackingStoreFile extends EventQueueBackingStore {
  private static final Logger LOG = LoggerFactory
      .getLogger(EventQueueBackingStoreFile.class);
  private static final int MAX_ALLOC_BUFFER_SIZE = 2*1024*1024; // 2MB
  protected static final int HEADER_SIZE = 1029;
  protected static final int INDEX_VERSION = 0;
  protected static final int INDEX_WRITE_ORDER_ID = 1;
  protected static final int INDEX_CHECKPOINT_MARKER = 4;
  protected static final int CHECKPOINT_COMPLETE = 0;
  protected static final int CHECKPOINT_INCOMPLETE = 1;

  protected LongBuffer elementsBuffer;
  protected final Map<Integer, Long> overwriteMap = new HashMap<Integer, Long>();
  protected final Map<Integer, AtomicInteger> logFileIDReferenceCounts = Maps.newHashMap();
  protected final MappedByteBuffer mappedBuffer;
  protected final RandomAccessFile checkpointFileHandle;
  protected final File checkpointFile;

  protected EventQueueBackingStoreFile(int capacity, String name,
      File checkpointFile) throws IOException, BadCheckpointException {
    super(capacity, name);
    this.checkpointFile = checkpointFile;
    checkpointFileHandle = new RandomAccessFile(checkpointFile, "rw");
    int totalBytes = (capacity + HEADER_SIZE) * Serialization.SIZE_OF_LONG;
    if(checkpointFileHandle.length() == 0) {
      allocate(checkpointFile, totalBytes);
      checkpointFileHandle.seek(INDEX_VERSION * Serialization.SIZE_OF_LONG);
      checkpointFileHandle.writeLong(getVersion());
      checkpointFileHandle.getChannel().force(true);
      LOG.info("Preallocated " + checkpointFile + " to " + checkpointFileHandle.length()
          + " for capacity " + capacity);
    }
    if(checkpointFile.length() != totalBytes) {
      String msg = "Configured capacity is " + capacity + " but the "
          + " checkpoint file capacity is " +
          ((checkpointFile.length() / Serialization.SIZE_OF_LONG) - HEADER_SIZE)
          + ". See FileChannel documentation on how to change a channels" +
          " capacity.";
      throw new BadCheckpointException(msg);
    }
    mappedBuffer = checkpointFileHandle.getChannel().map(MapMode.READ_WRITE, 0,
        checkpointFile.length());
    elementsBuffer = mappedBuffer.asLongBuffer();

    int version = (int) elementsBuffer.get(INDEX_VERSION);
    if(version != getVersion()) {
      throw new BadCheckpointException("Invalid version: " + version + " " +
              name + ", expected " + getVersion());
    }
    long checkpointComplete =
        (int) elementsBuffer.get(INDEX_CHECKPOINT_MARKER);
    if(checkpointComplete != CHECKPOINT_COMPLETE) {
      throw new BadCheckpointException("Checkpoint was not completed correctly,"
              + " probably because the agent stopped while the channel was"
              + " checkpointing.");
    }
  }

  protected long getCheckpointLogWriteOrderID() {
    return elementsBuffer.get(INDEX_WRITE_ORDER_ID);
  }

  protected abstract void writeCheckpointMetaData() throws IOException;

  @Override
  void checkpoint()  throws IOException {

    LOG.info("Start checkpoint for " + checkpointFile +
        ", elements to sync = " + overwriteMap.size());

    // Start checkpoint
    elementsBuffer.put(INDEX_CHECKPOINT_MARKER, CHECKPOINT_INCOMPLETE);
    mappedBuffer.force();

    setLogWriteOrderID(WriteOrderOracle.next());
    LOG.info("Updating checkpoint metadata: logWriteOrderID: "
        + getLogWriteOrderID() + ", queueSize: " + getSize() + ", queueHead: "
          + getHead());
    elementsBuffer.put(INDEX_WRITE_ORDER_ID, getLogWriteOrderID());
    try {
      writeCheckpointMetaData();
    } catch (IOException e) {
      throw new IOException("Error writing metadata", e);
    }

    Iterator<Integer> it = overwriteMap.keySet().iterator();
    while (it.hasNext()) {
      int index = it.next();
      long value = overwriteMap.get(index);
      elementsBuffer.put(index, value);
      it.remove();
    }

    Preconditions.checkState(overwriteMap.isEmpty(),
        "concurrent update detected ");

    // Finish checkpoint
    elementsBuffer.put(INDEX_CHECKPOINT_MARKER, CHECKPOINT_COMPLETE);
    mappedBuffer.force();
  }


  @Override
  void close() {
    mappedBuffer.force();
    try {
      checkpointFileHandle.close();
    } catch (IOException e) {
      LOG.info("Error closing " + checkpointFile, e);
    }
  }

  @Override
  long get(int index) {
    int realIndex = getPhysicalIndex(index);
    long result = EMPTY;
    if (overwriteMap.containsKey(realIndex)) {
      result = overwriteMap.get(realIndex);
    } else {
      result = elementsBuffer.get(realIndex);
    }
    return result;
  }

  @Override
  ImmutableSortedSet<Integer> getReferenceCounts() {
    return ImmutableSortedSet.copyOf(logFileIDReferenceCounts.keySet());
  }

  @Override
  void put(int index, long value) {
    int realIndex = getPhysicalIndex(index);
    overwriteMap.put(realIndex, value);
  }

  @Override
  boolean syncRequired() {
    return overwriteMap.size() > 0;
  }

  @Override
  protected void incrementFileID(int fileID) {
    AtomicInteger counter = logFileIDReferenceCounts.get(fileID);
    if(counter == null) {
      counter = new AtomicInteger(0);
      logFileIDReferenceCounts.put(fileID, counter);
    }
    counter.incrementAndGet();
  }
  @Override
  protected void decrementFileID(int fileID) {
    AtomicInteger counter = logFileIDReferenceCounts.get(fileID);
    Preconditions.checkState(counter != null, "null counter ");
    int count = counter.decrementAndGet();
    if(count == 0) {
      logFileIDReferenceCounts.remove(fileID);
    }
  }

  protected int getPhysicalIndex(int index) {
    return HEADER_SIZE + (getHead() + index) % getCapacity();
  }

  protected static void allocate(File file, long totalBytes) throws IOException {
    RandomAccessFile checkpointFile = new RandomAccessFile(file, "rw");
    boolean success = false;
    try {
      if (totalBytes <= MAX_ALLOC_BUFFER_SIZE) {
        checkpointFile.write(new byte[(int)totalBytes]);
      } else {
        byte[] initBuffer = new byte[MAX_ALLOC_BUFFER_SIZE];
        long remainingBytes = totalBytes;
        while (remainingBytes >= MAX_ALLOC_BUFFER_SIZE) {
          checkpointFile.write(initBuffer);
          remainingBytes -= MAX_ALLOC_BUFFER_SIZE;
        }
        if (remainingBytes > 0) {
          checkpointFile.write(initBuffer, 0, (int)remainingBytes);
        }
      }
      success = true;
    } finally {
      try {
        checkpointFile.close();
      } catch (IOException e) {
        if(success) {
          throw e;
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    File file = new File(args[0]);
    File inflightTakesFile = new File(args[1]);
    File inflightPutsFile = new File(args[2]);
    if (!file.exists()) {
      throw new IOException("File " + file + " does not exist");
    }
    if (file.length() == 0) {
      throw new IOException("File " + file + " is empty");
    }
    int capacity = (int) ((file.length() - (HEADER_SIZE * 8L)) / 8L);
    EventQueueBackingStoreFile backingStore = (EventQueueBackingStoreFile)
        EventQueueBackingStoreFactory.get(file,capacity, "debug", false);
    System.out.println("File Reference Counts"
            + backingStore.logFileIDReferenceCounts);
    System.out.println("Queue Capacity " + backingStore.getCapacity());
    System.out.println("Queue Size " + backingStore.getSize());
    System.out.println("Queue Head " + backingStore.getHead());
    for (int index = 0; index < backingStore.getCapacity(); index++) {
      long value = backingStore.get(backingStore.getPhysicalIndex(index));
      int fileID = (int) (value >>> 32);
      int offset = (int) value;
      System.out.println(index + ":" + Long.toHexString(value) + " fileID = "
              + fileID + ", offset = " + offset);
    }
    FlumeEventQueue queue =
        new FlumeEventQueue(backingStore, inflightTakesFile, inflightPutsFile);
    SetMultimap<Long, Long> putMap = queue.deserializeInflightPuts();
    System.out.println("Inflight Puts:");

    for (Long txnID : putMap.keySet()) {
      Set<Long> puts = putMap.get(txnID);
      System.out.println("Transaction ID: " + String.valueOf(txnID));
      for (long value : puts) {
        int fileID = (int) (value >>> 32);
        int offset = (int) value;
        System.out.println(Long.toHexString(value) + " fileID = "
                + fileID + ", offset = " + offset);
      }
    }
    SetMultimap<Long, Long> takeMap = queue.deserializeInflightTakes();
    System.out.println("Inflight takes:");
    for (Long txnID : takeMap.keySet()) {
      Set<Long> takes = takeMap.get(txnID);
      System.out.println("Transaction ID: " + String.valueOf(txnID));
      for (long value : takes) {
        int fileID = (int) (value >>> 32);
        int offset = (int) value;
        System.out.println(Long.toHexString(value) + " fileID = "
                + fileID + ", offset = " + offset);
      }
    }
  }
}
