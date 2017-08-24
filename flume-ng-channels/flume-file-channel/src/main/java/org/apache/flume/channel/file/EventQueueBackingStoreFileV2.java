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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.apache.flume.channel.file.instrumentation.FileChannelCounter;

final class EventQueueBackingStoreFileV2 extends EventQueueBackingStoreFile {

  private static final int INDEX_SIZE = 2;
  private static final int INDEX_HEAD = 3;
  private static final int INDEX_ACTIVE_LOG = 5;
  private static final int MAX_ACTIVE_LOGS = 1024;

  EventQueueBackingStoreFileV2(
      File checkpointFile, int capacity, String name, FileChannelCounter counter
  ) throws IOException, BadCheckpointException {
    super(capacity, name, counter, checkpointFile);
    Preconditions.checkArgument(capacity > 0,
        "capacity must be greater than 0 " + capacity);

    setLogWriteOrderID(elementsBuffer.get(INDEX_WRITE_ORDER_ID));
    setSize((int) elementsBuffer.get(INDEX_SIZE));
    setHead((int) elementsBuffer.get(INDEX_HEAD));

    int indexMaxLog = INDEX_ACTIVE_LOG + MAX_ACTIVE_LOGS;
    for (int i = INDEX_ACTIVE_LOG; i < indexMaxLog; i++) {
      long nextFileCode = elementsBuffer.get(i);
      if (nextFileCode  != EMPTY) {
        Pair<Integer, Integer> idAndCount =
            deocodeActiveLogCounter(nextFileCode);
        logFileIDReferenceCounts.put(idAndCount.getLeft(),
            new AtomicInteger(idAndCount.getRight()));
      }
    }
  }

  @Override
  protected int getVersion() {
    return Serialization.VERSION_2;
  }

  @Override
  protected void incrementFileID(int fileID) {
    super.incrementFileID(fileID);
    Preconditions.checkState(logFileIDReferenceCounts.size() < MAX_ACTIVE_LOGS,
        "Too many active logs ");
  }


  private Pair<Integer, Integer> deocodeActiveLogCounter(long value) {
    int fileId = (int) (value >>> 32);
    int count = (int) value;
    return Pair.of(fileId, count);
  }
  private long encodeActiveLogCounter(int fileId, int count) {
    long result = fileId;
    result = (long)fileId << 32;
    result += (long) count;
    return result;
  }
  @Override
  protected void writeCheckpointMetaData() {
    elementsBuffer.put(INDEX_SIZE, getSize());
    elementsBuffer.put(INDEX_HEAD, getHead());
    List<Long> fileIdAndCountEncoded = new ArrayList<Long>();
    for (Integer fileId : logFileIDReferenceCounts.keySet()) {
      Integer count = logFileIDReferenceCounts.get(fileId).get();
      long value = encodeActiveLogCounter(fileId, count);
      fileIdAndCountEncoded.add(value);
    }

    int emptySlots = MAX_ACTIVE_LOGS - fileIdAndCountEncoded.size();
    for (int i = 0; i < emptySlots; i++)  {
      fileIdAndCountEncoded.add(0L);
    }
    for (int i = 0; i < MAX_ACTIVE_LOGS; i++) {
      elementsBuffer.put(i + INDEX_ACTIVE_LOG, fileIdAndCountEncoded.get(i));
    }
  }
}
