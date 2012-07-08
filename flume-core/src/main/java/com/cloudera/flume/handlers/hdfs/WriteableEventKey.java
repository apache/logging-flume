/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.flume.handlers.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.WritableComparable;

import com.cloudera.flume.core.Event;


/**
 * This abstracts what the eventual key value will into a 128-bit value (16
 * bytes). This should be sufficient to pack just about anything into in.
 * 
 * Currently I'm just using the timestamp on the message.
 */
public class WriteableEventKey implements WritableComparable<WriteableEventKey> {

  private byte[] key;

  public WriteableEventKey() {
  }

  public WriteableEventKey(Event e) {
    ByteBuffer b = ByteBuffer.allocate(16);
    b.putLong(e.getTimestamp());
    key = b.array();
    assert (key.length == 16);

  }

  public void readFields(DataInput in) throws IOException {
    key = new byte[16];
    in.readFully(key);
  }

  public void write(DataOutput out) throws IOException {
    out.write(key);
  }

  @Override
  public int compareTo(WriteableEventKey k) {
    ByteBuffer k2 = ByteBuffer.wrap(k.key);
    return ByteBuffer.wrap(key).compareTo(k2);
  }
  
  /**
   * 'Strongly recommended' that compareTo(k) == 0 == equals(k)
   */
  @Override  
  public boolean equals(Object other) {
    if (other instanceof WriteableEventKey) {
      return (compareTo((WriteableEventKey)other) == 0);
    }
    return false;
  }
  
  /**
   * Overriding equals usually means that hashCode should be overridden
   */
  @Override
  public int hashCode() {
    return ByteBuffer.wrap(key).hashCode();
  }

}
