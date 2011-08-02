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
package com.cloudera.flume.core;

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This is a singleton that keeps a mapping of attribute to a type. This is a
 * like a schema, and is for formatting used in reports.
 */
public class Attributes {
  static final Logger LOG = LoggerFactory.getLogger(Attributes.class);

  public enum Type {
    INT, LONG, DOUBLE, STRING,
    // IP, DATE, HOST, ... others.
  };

  static HashMap<String, Type> map = new HashMap<String, Type>();

  public static void register(String attr, Type t) {
    Type old = map.get(attr);
    if (map.get(attr) != null) {
      LOG.warn("Changing attribute '" + attr + "' from type " + old
          + " to type " + t);
    }
    map.put(attr, t);
  }

  public static Integer readInt(Event e, String attr) {
    Type t = map.get(attr);
    Preconditions.checkArgument(t == Type.INT || t == null);
    byte[] bytes = e.get(attr);
    if (bytes == null)
      return null;

    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return buf.asIntBuffer().get();
  }

  public static Long readLong(Event e, String attr) {
    Type t = map.get(attr);
    Preconditions.checkArgument(t == Type.LONG || t == null);
    byte[] bytes = e.get(attr);
    if (bytes == null)
      return null;

    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return buf.asLongBuffer().get();
  }

  public static Double readDouble(Event e, String attr) {
    Type t = map.get(attr);
    Preconditions.checkArgument(t == Type.DOUBLE || t == null);
    byte[] bytes = e.get(attr);
    if (bytes == null)
      return null;

    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return buf.asDoubleBuffer().get();
  }

  public static String readString(Event e, String attr) {
    Type t = map.get(attr);
    Preconditions.checkArgument(t == Type.STRING || t == null);
    byte[] bytes = e.get(attr);
    if (bytes == null)
      return null;

    return new String(bytes);

  }

  public static void setInt(Event e, String attr, int i) {
    byte[] buf = ByteBuffer.allocate(4).putInt(i).array();
    e.set(attr, buf);
  }

  public static void setLong(Event e, String attr, long l) {
    byte[] buf = ByteBuffer.allocate(8).putLong(l).array();
    e.set(attr, buf);
  }

  public static void setDouble(Event e, String attr, double d) {
    byte[] buf = ByteBuffer.allocate(8).putDouble(d).array();
    e.set(attr, buf);
  }

  public static void setString(Event e, String attr, String val) {
    e.set(attr, val.getBytes());
  }

  /**
   * This toStrng method is for human readable output. The html reports use this
   * as opposed to the other.
   */
  public static String toString(Event e, String attr) {
    Preconditions.checkNotNull(attr);
    Preconditions.checkNotNull(e);
    Type t = map.get(attr);
    if (t == null) {
      // default to binary bytes array
      byte[] bytes = e.get(attr);
      if (bytes == null) {
        return "[ ]";
      }

      // this is a hack that prints in int, string and double format when there
      // are 8 bytes.
      // TODO (jon) this gets grosser and grosser. make a final decision on how
      // these attributes are going to be
      if (bytes.length == 8) {

        return "(long)" + readLong(e, attr).toString() + "  (string) '"
            + readString(e, attr) + "'" + " (double)"
            + readDouble(e, attr).toString();
      }

      // this is a simlar hack that prints in int and string format when there
      // are 4 bytes.
      if (bytes.length == 4) {
        return readInt(e, attr).toString() + " '" + readString(e, attr) + "'";
      }

      if (bytes.length == 1) {
        return "" + (((int) bytes[0]) & 0xff);
      }

      return readString(e, attr);
    }

    byte[] data = e.get(attr);
    if (data == null)
      return "";

    switch (t) {
    case INT:
      return readInt(e, attr).toString();
    case LONG:
      return readLong(e, attr).toString();
    case DOUBLE:
      return readDouble(e, attr).toString();
    case STRING:
      return readString(e, attr);
    default:
      return "<unsupported type " + t + ">";
    }
  }

  /**
   * This toString method strictly uses the Attribute type information. When
   * there is an untyped attribute, it defaults to outputing the data as a byte
   * array.
   */
  public static String toStringStrict(Event e, String attr) {
    Preconditions.checkNotNull(attr);
    Preconditions.checkNotNull(e);
    Type t = map.get(attr);
    if (t == null) {
      // default to binary bytes array
      byte[] bytes = e.get(attr);
      if (bytes == null) {
        return "[ ]";
      }

      StringBuilder sb = new StringBuilder();
      sb.append("[");

      for (int i = 0; i < bytes.length; i++) {
        if (i != 0) {
          sb.append(", ");
        }
        sb.append(bytes[i]);
      }
      sb.append("]");
      return sb.toString();
    }

    byte[] data = e.get(attr);
    if (data == null)
      return "";

    switch (t) {
    case INT:
      return readInt(e, attr).toString();
    case LONG:
      return readLong(e, attr).toString();
    case DOUBLE:
      return readDouble(e, attr).toString();
    case STRING:
      return readString(e, attr);
    default:
      return "<unsupported type " + t + ">";
    }
  }

  public static Type getType(String attr) {
    return map.get(attr);
  }
}
