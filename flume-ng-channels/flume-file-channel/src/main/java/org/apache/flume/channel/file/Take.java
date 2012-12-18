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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.flume.channel.file.proto.ProtosFactory;

import com.google.common.base.Preconditions;

/**
 * Represents a Take on disk
 */
class Take extends TransactionEventRecord {
  private int offset;
  private int fileID;
  Take(Long transactionID, Long logWriteOrderID) {
    super(transactionID, logWriteOrderID);
  }
  Take(Long transactionID, Long logWriteOrderID, int offset, int fileID) {
    this(transactionID, logWriteOrderID);
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
  void writeProtos(OutputStream out) throws IOException {
    ProtosFactory.Take.Builder takeBuilder = ProtosFactory.Take.newBuilder();
    takeBuilder.setFileID(fileID);
    takeBuilder.setOffset(offset);
    takeBuilder.build().writeDelimitedTo(out);
  }
  @Override
  void readProtos(InputStream in) throws IOException {
    ProtosFactory.Take take = Preconditions.checkNotNull(ProtosFactory.
        Take.parseDelimitedFrom(in), "Take cannot be null");
    fileID = take.getFileID();
    offset = take.getOffset();
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
