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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.file.proto.ProtosFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

/**
 * Base class for records in data file: Put, Take, Rollback, Commit
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class TransactionEventRecord implements Writable {
  private static final Logger LOG = LoggerFactory
      .getLogger(TransactionEventRecord.class);
  private final long transactionID;
  private long logWriteOrderID;

  protected TransactionEventRecord(long transactionID, long logWriteOrderID) {
    this.transactionID = transactionID;
    this.logWriteOrderID = logWriteOrderID;
  }

  @Override
  public void readFields(DataInput in) throws IOException {

  }
  @Override
  public void write(DataOutput out) throws IOException {

  }

  abstract void writeProtos(OutputStream out) throws IOException;

  abstract void readProtos(InputStream in) throws IOException, CorruptEventException;

  long getLogWriteOrderID() {
    return logWriteOrderID;
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
          Put.class.getDeclaredConstructor(Long.class, Long.class));
      builder.put(Type.TAKE.get(),
          Take.class.getDeclaredConstructor(Long.class, Long.class));
      builder.put(Type.ROLLBACK.get(),
          Rollback.class.getDeclaredConstructor(Long.class, Long.class));
      builder.put(Type.COMMIT.get(),
          Commit.class.getDeclaredConstructor(Long.class, Long.class));
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    TYPES = builder.build();
  }

  @Deprecated
  static ByteBuffer toByteBufferV2(TransactionEventRecord record) {
    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream(512);
    DataOutputStream dataOutput = new DataOutputStream(byteOutput);
    try {
      dataOutput.writeInt(MAGIC_HEADER);
      dataOutput.writeShort(record.getRecordType());
      dataOutput.writeLong(record.getTransactionID());
      dataOutput.writeLong(record.getLogWriteOrderID());
      record.write(dataOutput);
      dataOutput.flush();
      // TODO toByteArray does an unneeded copy
      return ByteBuffer.wrap(byteOutput.toByteArray());
    } catch(IOException e) {
      // near impossible
      throw Throwables.propagate(e);
    } finally {
      if(dataOutput != null) {
        try {
          dataOutput.close();
        } catch (IOException e) {
          LOG.warn("Error closing byte array output stream", e);
        }
      }
    }
  }

  @Deprecated
  static TransactionEventRecord fromDataInputV2(DataInput in)
      throws IOException {
    int header = in.readInt();
    if(header != MAGIC_HEADER) {
      throw new IOException("Header " + Integer.toHexString(header) +
          " is not the required value: " + Integer.toHexString(MAGIC_HEADER));
    }
    short type = in.readShort();
    long transactionID = in.readLong();
    long writeOrderID = in.readLong();
    TransactionEventRecord entry = newRecordForType(type, transactionID,
        writeOrderID);
    entry.readFields(in);
    return entry;
  }

  static ByteBuffer toByteBuffer(TransactionEventRecord record) {
    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream(512);
    try {
      ProtosFactory.TransactionEventHeader.Builder headerBuilder =
          ProtosFactory.TransactionEventHeader.newBuilder();
      headerBuilder.setType(record.getRecordType());
      headerBuilder.setTransactionID(record.getTransactionID());
      headerBuilder.setWriteOrderID(record.getLogWriteOrderID());
      headerBuilder.build().writeDelimitedTo(byteOutput);
      record.writeProtos(byteOutput);
      ProtosFactory.TransactionEventFooter footer =
          ProtosFactory.TransactionEventFooter.newBuilder().build();
      footer.writeDelimitedTo(byteOutput);
      return ByteBuffer.wrap(byteOutput.toByteArray());
    } catch(IOException e) {
      throw Throwables.propagate(e);
    } finally {
      if(byteOutput != null) {
        try {
          byteOutput.close();
        } catch (IOException e) {
          LOG.warn("Error closing byte array output stream", e);
        }
      }
    }
  }


  static TransactionEventRecord fromByteArray(byte[] buffer)
      throws IOException, CorruptEventException {
    ByteArrayInputStream in = new ByteArrayInputStream(buffer);
    try {
      ProtosFactory.TransactionEventHeader header = Preconditions.
          checkNotNull(ProtosFactory.TransactionEventHeader.
              parseDelimitedFrom(in), "Header cannot be null");
      short type = (short)header.getType();
      long transactionID = header.getTransactionID();
      long writeOrderID = header.getWriteOrderID();
      TransactionEventRecord transactionEvent =
          newRecordForType(type, transactionID, writeOrderID);
      transactionEvent.readProtos(in);
      @SuppressWarnings("unused")
      ProtosFactory.TransactionEventFooter footer = Preconditions.checkNotNull(
          ProtosFactory.TransactionEventFooter.
          parseDelimitedFrom(in), "Footer cannot be null");
      return transactionEvent;
    } catch (InvalidProtocolBufferException ex) {
      throw new CorruptEventException(
        "Could not parse event from data file.", ex);
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        LOG.warn("Error closing byte array input stream", e);
      }
    }
  }

  static String getName(short type) {
    Constructor<? extends TransactionEventRecord> constructor = TYPES.get(type);
    Preconditions.checkNotNull(constructor, "Unknown action " +
        Integer.toHexString(type));
    return constructor.getDeclaringClass().getSimpleName();
  }

  private static TransactionEventRecord newRecordForType(short type,
      long transactionID, long writeOrderID) {
    Constructor<? extends TransactionEventRecord> constructor = TYPES.get(type);
    Preconditions.checkNotNull(constructor, "Unknown action " +
        Integer.toHexString(type));
    try {
      return constructor.newInstance(transactionID, writeOrderID);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
