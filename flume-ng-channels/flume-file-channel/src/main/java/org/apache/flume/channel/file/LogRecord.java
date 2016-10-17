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

import java.util.Arrays;

public class LogRecord implements Comparable<LogRecord> {
  private int fileID;
  private int offset;
  private TransactionEventRecord event;

  public LogRecord(int fileID, int offset, TransactionEventRecord event) {
    this.fileID = fileID;
    this.offset = offset;
    this.event = event;
  }

  public int getFileID() {
    return fileID;
  }
  public int getOffset() {
    return offset;
  }
  public TransactionEventRecord getEvent() {
    return event;
  }

  @Override
  public int compareTo(LogRecord o) {
    int result = new Long(event.getLogWriteOrderID()).compareTo(o.getEvent().getLogWriteOrderID());
    if (result == 0) {
      // oops we have hit a flume-1.2 bug. let's try and use the txid
      // to replay the events
      result = new Long(event.getTransactionID()).compareTo(o.getEvent().getTransactionID());
      if (result == 0) {
        // events are within the same transaction. Basically we want commit
        // and rollback to come after take and put
        Integer thisIndex = Arrays.binarySearch(replaySortOrder, event.getRecordType());
        Integer thatIndex = Arrays.binarySearch(replaySortOrder, o.getEvent().getRecordType());
        return thisIndex.compareTo(thatIndex);
      }
    }
    return result;
  }

  private static final short[] replaySortOrder = new short[] {
    TransactionEventRecord.Type.TAKE.get(),
    TransactionEventRecord.Type.PUT.get(),
    TransactionEventRecord.Type.ROLLBACK.get(),
    TransactionEventRecord.Type.COMMIT.get(),
  };
}
