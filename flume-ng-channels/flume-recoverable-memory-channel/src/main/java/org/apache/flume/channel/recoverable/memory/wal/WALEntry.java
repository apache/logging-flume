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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Wraps a {@link Writable} with a sequence id so that both can
 * be written together to a file.
 */
public class WALEntry<T extends Writable> implements Writable {
  /**
   * Provides a minimum guarantee we are not reading complete junk
   */
  private static final int MAGIC_HEADER = 0xdeadbeef;

  private T data;
  private long sequenceID;
  /**
   * Only to be used when reading a wal file from disk
   */
  WALEntry(T data) {
    this(data, -1);
  }
  /**
   * Creates a WALEntry with specified payload and sequence id
   * @param data
   * @param sequenceID
   */
  public WALEntry(T data, long sequenceID) {
    this.data = data;
    this.sequenceID = sequenceID;
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    int header = in.readInt();
    if(header != MAGIC_HEADER) {
      throw new IOException("Header is " + Integer.toHexString(header) +
          " expected " + Integer.toHexString(MAGIC_HEADER));
    }
    sequenceID = in.readLong();
    data.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(MAGIC_HEADER);
    out.writeLong(sequenceID);
    data.write(out);
  }

  public T getData() {
    return data;
  }

  public long getSequenceID() {
    return sequenceID;
  }

  static int getBaseSize() {
    int base = 4 /* magic header of type int */ + 8 /* seqid of type long */;
    return base;
  }
}
