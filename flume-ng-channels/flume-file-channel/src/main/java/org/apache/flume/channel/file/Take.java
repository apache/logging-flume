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

/**
 * Represents a Take on disk
 */
class Take extends TransactionEventRecord {
  private int offset;
  private int fileID;
  Take(Long transactionID) {
    super(transactionID);
  }
  Take(Long transactionID, int offset, int fileID) {
    this(transactionID);
    this.offset = offset;
    this.fileID = fileID;
  }
  int getOffset() {
    return offset;
  }

  int getFileID() {
    return fileID;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    offset = in.readInt();
    fileID = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(offset);
    out.writeInt(fileID);
  }
  @Override
  short getRecordType() {
    return Type.TAKE.get();
  }
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Take [offset=");
    builder.append(offset);
    builder.append(", fileID=");
    builder.append(fileID);
    builder.append(", getLogWriteOrderID()=");
    builder.append(getLogWriteOrderID());
    builder.append(", getTransactionID()=");
    builder.append(getTransactionID());
    builder.append("]");
    return builder.toString();
  }
}
