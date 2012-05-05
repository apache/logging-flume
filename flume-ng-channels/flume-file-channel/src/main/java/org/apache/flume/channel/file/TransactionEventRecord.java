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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

/**
 * Base class for records in data file: Put, Take, Rollback, Commit
 */
abstract class TransactionEventRecord implements Writable {
  private final long transactionID;
  private long timestamp;

  protected TransactionEventRecord(long transactionID) {
    this.transactionID = transactionID;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    timestamp = in.readLong();
  }
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(timestamp);
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  public long getTimestamp() {
    return timestamp;
  }
  long getTransactionID() {
    return transactionID;
  }

  abstract short getRecordType();


  /**
   * Provides a minimum guarantee we are not reading complete junk
   */
  static final int MAGIC_HEADER = 0xdeadbeef;

  static enum Type {
    PUT((short)1),
    TAKE((short)2),
    ROLLBACK((short)3),
    COMMIT((short)4);

    private short id;
    Type(short id) {
      this.id = id;
    }
    public short get() {
      return id;
    }
  }
  private static final ImmutableMap<Short, Constructor<? extends TransactionEventRecord>> TYPES;

  static {
    ImmutableMap.Builder<Short, Constructor<? extends TransactionEventRecord>> builder =
        ImmutableMap.<Short, Constructor<? extends TransactionEventRecord>>builder();
    try {
      builder.put(Type.PUT.get(),
          Put.class.getDeclaredConstructor(Long.class));
      builder.put(Type.TAKE.get(),
          Take.class.getDeclaredConstructor(Long.class));
      builder.put(Type.ROLLBACK.get(),
          Rollback.class.getDeclaredConstructor(Long.class));
      builder.put(Type.COMMIT.get(),
          Commit.class.getDeclaredConstructor(Long.class));
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    TYPES = builder.build();
  }


  static ByteBuffer toByteBuffer(TransactionEventRecord record) {
    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream(512);
    DataOutputStream dataOutput = new DataOutputStream(byteOutput);
    try {
      dataOutput.writeInt(MAGIC_HEADER);
      dataOutput.writeShort(record.getRecordType());
      dataOutput.writeLong(record.getTransactionID());
      record.write(dataOutput);
      dataOutput.flush();
      // TODO toByteArray does an unneeded copy
      return ByteBuffer.wrap(byteOutput.toByteArray());
    } catch(IOException e) {
      // near impossible
      Throwables.propagate(e);
    } finally {
      if(dataOutput != null) {
        try {
          dataOutput.close();
        } catch (IOException e) {}
      }
    }
    throw new IllegalStateException(
        "Should not occur as method should return or throw an exception");
  }

  static TransactionEventRecord fromDataInput(DataInput in) throws IOException {
    int header = in.readInt();
    if(header != MAGIC_HEADER) {
      throw new IOException("Header " + Integer.toHexString(header) +
          " not expected value: " + Integer.toHexString(MAGIC_HEADER));
    }
    short type = in.readShort();
    long transactionID = in.readLong();
    TransactionEventRecord entry = newRecordForType(type, transactionID);
    entry.readFields(in);
    return entry;
  }

  static String getName(short type) {
    Constructor<? extends TransactionEventRecord> constructor = TYPES.get(type);
    Preconditions.checkNotNull(constructor, "Unknown action " +
        Integer.toHexString(type));
    return constructor.getDeclaringClass().getSimpleName();
  }

  private static TransactionEventRecord newRecordForType(short type, long transactionID) {
    Constructor<? extends TransactionEventRecord> constructor = TYPES.get(type);
    Preconditions.checkNotNull(constructor, "Unknown action " +
        Integer.toHexString(type));
    try {
      return constructor.newInstance(transactionID);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    throw new IllegalStateException(
        "Should not occur as method should return or throw an exception");
  }

}
