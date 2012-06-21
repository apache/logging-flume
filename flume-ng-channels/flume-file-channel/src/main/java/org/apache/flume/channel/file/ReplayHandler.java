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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.map.MultiValueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Processes a set of data logs, replaying said logs into the queue.
 */
class ReplayHandler {
  private static final Logger LOG = LoggerFactory
      .getLogger(ReplayHandler.class);
  private final FlumeEventQueue queue;
  private final long lastCheckpoint;
  /**
   * This data structure stores takes for which we found a commit in the log
   * files before we found a commit for the put. This can happen if the channel
   * is configured for multiple directories.
   *
   * Consider the following:
   *
   * logdir1, logdir2
   *
   * Put goes to logdir2 Commit of Put goes to logdir2 Take goes to logdir1
   * Commit of Take goes to logdir1
   *
   * When replaying we will start with log1 and find the take and commit before
   * finding the put and commit in logdir2.
   */
  private final List<Long> pendingTakes;

  ReplayHandler(FlumeEventQueue queue) {
    this.queue = queue;
    this.lastCheckpoint = queue.getTimestamp();
    pendingTakes = Lists.newArrayList();
  }

  void replayLog(List<File> logs) throws IOException {
    int total = 0;
    int count = 0;
    MultiMap transactionMap = new MultiValueMap();
    LOG.info("Starting replay of " + logs);
    for (File log : logs) {
      LOG.info("Replaying " + log);
      LogFile.SequentialReader reader = null;
      try {
        reader = new LogFile.SequentialReader(log);
        reader.skipToLastCheckpointPosition(queue.getTimestamp());
        Pair<Integer, TransactionEventRecord> entry;
        FlumeEventPointer ptr;
        // for puts the fileId is the fileID of the file they exist in
        // for takes the fileId and offset are pointers to a put
        int fileId = reader.getLogFileID();
        int readCount = 0;
        int putCount = 0;
        int takeCount = 0;
        int rollbackCount = 0;
        int commitCount = 0;
        int skipCount = 0;
        while ((entry = reader.next()) != null) {
          int offset = entry.getLeft();
          TransactionEventRecord record = entry.getRight();
          short type = record.getRecordType();
          long trans = record.getTransactionID();
          readCount++;
          if (record.getTimestamp() > lastCheckpoint) {
            if (type == TransactionEventRecord.Type.PUT.get()) {
              putCount++;
              ptr = new FlumeEventPointer(fileId, offset);
              transactionMap.put(trans, ptr);
            } else if (type == TransactionEventRecord.Type.TAKE.get()) {
              takeCount++;
              Take take = (Take) record;
              ptr = new FlumeEventPointer(take.getFileID(), take.getOffset());
              transactionMap.put(trans, ptr);
            } else if (type == TransactionEventRecord.Type.ROLLBACK.get()) {
              rollbackCount++;
              transactionMap.remove(trans);
            } else if (type == TransactionEventRecord.Type.COMMIT.get()) {
              commitCount++;
              @SuppressWarnings("unchecked")
              Collection<FlumeEventPointer> pointers =
                (Collection<FlumeEventPointer>) transactionMap.remove(trans);
              if (pointers != null && pointers.size() > 0) {
                processCommit(((Commit) record).getType(), pointers);
                count += pointers.size();
              }
            } else {
              Preconditions.checkArgument(false, "Unknown record type: "
                  + Integer.toHexString(type));
            }

          } else {
            skipCount++;
          }
        }
        LOG.info("Replayed " + count + " from " + log);
        if (LOG.isDebugEnabled()) {
          LOG.debug("read: " + readCount + ", put: " + putCount + ", take: "
            + takeCount + ", rollback: " + rollbackCount + ", commit: "
            + commitCount + ", skipp: " + skipCount);
        }
      } catch (EOFException e) {
        LOG.warn("Hit EOF on " + log);
      } finally {
        total += count;
        count = 0;
        if (reader != null) {
          reader.close();
        }
      }
    }
    int pendingTakesSize = pendingTakes.size();
    if (pendingTakesSize > 0) {
      String msg = "Pending takes " + pendingTakesSize
          + " exist after the end of replay";
      if (LOG.isDebugEnabled()) {
        for (Long pointer : pendingTakes) {
          LOG.debug("Pending take " + FlumeEventPointer.fromLong(pointer));
        }
      } else {
        LOG.error(msg + ". Duplicate messages will exist in destination.");
      }
    }
    LOG.info("Replayed " + total);
  }

  private void processCommit(short type, Collection<FlumeEventPointer> pointers) {
    if (type == TransactionEventRecord.Type.PUT.get()) {
      for (FlumeEventPointer pointer : pointers) {
        Preconditions.checkState(queue.addTail(pointer), "Unable to add "
            + pointer);
        if (pendingTakes.remove(pointer.toLong())) {
          Preconditions.checkState(queue.remove(pointer),
              "Take was pending and pointer was successfully added to the"
                  + " queue but could not be removed: " + pointer);
        }
      }
    } else if (type == TransactionEventRecord.Type.TAKE.get()) {
      for (FlumeEventPointer pointer : pointers) {
        boolean removed = queue.remove(pointer);
        if (!removed) {
          pendingTakes.add(pointer.toLong());
        }
      }
    } else {
      Preconditions.checkArgument(false,
          "Unknown record type: " + Integer.toHexString(type));
    }
  }

}
