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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventBaseImpl;
import com.cloudera.flume.core.EventImpl;

import com.google.common.base.Preconditions;

/**
 * A wrapper to make my events hadoop/hdfs writables.
 * 
 */
public class WriteableEvent extends EventBaseImpl implements Writable {
  final static long MAX_BODY_SIZE = FlumeConfiguration.get().getEventMaxSizeBytes();

  private Event e;

  /**
   * This creates an empty event. Used as a container for unmarshalling data
   * using the Writable mechanism
   */
  public WriteableEvent() {
    this(new EventImpl("".getBytes()));
  }

  public WriteableEvent(Event e) {
    assert (e != null);
    this.e = e;
  }

  public static WriteableEvent create(byte[] raw) throws IOException {
    WriteableEvent e = new WriteableEvent();
    DataInput in = new DataInputStream(new ByteArrayInputStream(raw));
    e.readFields(in);
    return e;
  }

  public Event getEvent() {
    return e;
  }

  public byte[] getBody() {
    return e.getBody();
  }

  public Priority getPriority() {
    return e.getPriority();
  }

  public long getTimestamp() {
    return e.getTimestamp();
  }

  @Override
  public long getNanos() {
    return e.getNanos();
  }

  @Override
  public String getHost() {
    return e.getHost();
  }

  /**
   * This is just a place holder structure for key. The format is TBD, currently
   * using timestamp
   */
  public WriteableEventKey getEventKey() {
    return new WriteableEventKey(e);
  }

  public void readFields(DataInput in) throws IOException {
    // NOTE: NOT using read UTF8 becuase it is limited to 2^16 bytes (not
    // characters). Char encoding will likely cause problems in edge cases.

    // String s = in.readUTF();
    int len = in.readInt();

    Preconditions.checkArgument((len >= 0) && (len <= MAX_BODY_SIZE), "byte length is %s which is not <= %s and >= 0", len, MAX_BODY_SIZE);

    // TODO (jon) Compare to java.nio implementation
    byte[] body = new byte[len];
    in.readFully(body);
    
    long time = in.readLong();
    
    int prioidx = in.readInt();
    assert (Priority.values().length > prioidx);
    Priority prio = Priority.values()[prioidx];

    long nanos = in.readLong();
    
    String host = in.readUTF();

    Map<String, byte[]> fields = unserializeMap(in);

    // this should be the only instance where constructor with fields is used.
    e = new EventImpl(body, time, prio, nanos, host, fields);
  }

  public void write(DataOutput out) throws IOException {
    byte[] utf8 = getBody(); // .getBytes();
    out.writeInt(utf8.length);
    out.write(utf8);
    out.writeLong(getTimestamp());
    out.writeInt(getPriority().ordinal());
    out.writeLong(getNanos());
    out.writeUTF(getHost());

    // # of extensible entries
    serializeMap(out, e.getAttrs());

  }

  public static WriteableEvent createWriteableEvent(byte[] bytes)
      throws IOException {
    WriteableEvent we = new WriteableEvent();
    DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
    we.readFields(in);
    return we;
  }

  public byte[] toBytes() {
    try {
      // set buffer initially to 32k
      ByteArrayOutputStream baos = new ByteArrayOutputStream(2 >> 15);
      DataOutput out = new DataOutputStream(baos);
      write(out);
      return baos.toByteArray();
    } catch (IOException ioe) {
      assert (false);
      return null;
    }

  }

  @Override
  public byte[] get(String attr) {
    return e.get(attr);
  }

  public static DataOutput serializeMap(DataOutput out, Map<String, byte[]> m)
      throws IOException {
    int sz = m.size();
    out.writeInt(sz);
    for (Entry<String, byte[]> e : m.entrySet()) {
      out.writeUTF(e.getKey());
      byte[] v = e.getValue();
      out.writeInt(v.length);
      out.write(v);
    }
    return out;
  }

  public static Map<String, byte[]> unserializeMap(DataInput in)
      throws IOException {
    // # of extensible entries
    int sz = in.readInt();
    Map<String, byte[]> fields = new HashMap<String, byte[]>();
    for (int i = 0; i < sz; i++) {
      String f = in.readUTF();
      int l = in.readInt();
      byte[] val = new byte[l];
      in.readFully(val);
      fields.put(f, val);
    }
    return fields;
  }

  public static DataOutput serializeList(DataOutput out, List<byte[]> l)
      throws IOException {
    // # of extensible entries
    int sz = l.size();
    out.writeInt(sz);
    for (byte[] v : l) {
      out.writeInt(v.length);
      out.write(v);
    }
    return out;

  }

  public static List<byte[]> unserializeList(DataInput in) throws IOException {
    int sz = in.readInt();
    List<byte[]> l = new ArrayList<byte[]>(sz);
    for (int i = 0; i < sz; i++) {
      int vsz = in.readInt();
      byte[] v = new byte[vsz];
      in.readFully(v);
      l.add(v);
    }
    return l;
  }

  @Override
  public Map<String, byte[]> getAttrs() {
    return Collections.unmodifiableMap(e.getAttrs());
  }

  @Override
  public void set(String attr, byte[] v) {
    e.set(attr, v);
  }

}
