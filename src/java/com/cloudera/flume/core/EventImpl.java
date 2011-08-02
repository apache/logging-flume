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

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.lang.StringEscapeUtils;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.Clock;
import com.cloudera.util.NetUtils;
import com.google.common.base.Preconditions;

/**
 * A simple in memory implementation of an event.
 * 
 * I'm limiting a message to be at most 32k
 */
public class EventImpl extends EventBaseImpl {
  byte[] body;
  long timestamp;
  Priority pri;
  long nanos;
  String host;
  
  final static long MAX_BODY_SIZE = FlumeConfiguration.get().getEventMaxSizeBytes();

  /**
   * Reflection based tools (like avro) require a null constructor
   */
  public EventImpl() {
    this(new byte[0], 0, Priority.INFO, 0, "");
  }

  /**
   * Copy constructor for converting events into EventImpl (required for
   * reflection/avro)
   */
  public EventImpl(Event e) {
    this(e.getBody(), e.getTimestamp(), e.getPriority(), e.getNanos(), e
        .getHost(), new HashMap<String, byte[]>(e.getAttrs()));
  }

  /**
   * Constructs a new event wrapping (not copying!) the provided byte array
   */
  public EventImpl(byte[] s) {
    this(s, Clock.unixTime(), Priority.INFO, Clock.nanos(), NetUtils
        .localhost());
  }

  /**
   * Constructs a new event wrapping (not copying!) the provided byte array
   */
  public EventImpl(byte[] s, Priority pri) {
    this(s, Clock.unixTime(), pri, Clock.nanos(), NetUtils.localhost());
  }

  /**
   * Constructs a new event wrapping (not copying!) the provided byte array
   */
  public EventImpl(byte[] s, long timestamp, Priority pri, long nanoTime,
      String host) {
    this(s, timestamp, pri, nanoTime, host, new HashMap<String, byte[]>());
  }

  /**
   * Constructs a new event wrapping (not copying!) the provided byte array
   */
  public EventImpl(byte[] s, long timestamp, Priority pri, long nanoTime,
      String host, Map<String, byte[]> fields) {
    super(fields);
    Preconditions.checkNotNull(s);
    Preconditions.checkArgument(s.length <= MAX_BODY_SIZE);
    // this string construction took ~5% of exec time!
    // , "byte length is " + s.length + " which is not < " + MAX_BODY_SIZE);
    Preconditions.checkNotNull(pri);
    this.body = s;
    this.timestamp = timestamp;
    this.pri = pri;
    this.nanos = nanoTime;
    this.host = host;
  }

  /**
   * Returns reference to mutable body of event
   */
  public byte[] getBody() {
    return body;
  }

  public Priority getPriority() {
    return pri;
  }

  protected void setPriority(Priority p) {
    this.pri = p;
  }

  /**
   * Returns unix time stamp in millis
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Set unix time stamp in millis
   */
  protected void setTimestamp(long stamp) {
    this.timestamp = stamp;
  }

  public String toString() {
    String mbody = StringEscapeUtils.escapeJava(new String(getBody()));
    StringBuilder attrs = new StringBuilder();
    SortedMap<String, byte[]> sorted = new TreeMap<String, byte[]>(this.fields);
    for (Entry<String, byte[]> e : sorted.entrySet()) {
      attrs.append("{ " + e.getKey() + " : ");

      String o = Attributes.toString(this, e.getKey());
      attrs.append(o + " } ");
    }

    return getHost() + " [" + getPriority().toString() + " "
        + new Date(getTimestamp()) + "] " + attrs.toString() + mbody;
  }

  @Override
  public long getNanos() {
    return nanos;
  }

  @Override
  public String getHost() {
    return host;
  }

  /**
   * This takes an event and a list of attribute names. It returns a new event
   * that has the same core event values but *only * the attributes specified by
   * the list.
   */
  public static Event select(Event e, String... attrs) {
    Event e2 = new EventImpl(e.getBody(), e.getTimestamp(), e.getPriority(), e
        .getNanos(), e.getHost());
    for (String a : attrs) {
      byte[] data = e.get(a);
      if (data == null) {
        continue;
      }
      e2.set(a, data);
    }
    return e2;
  }

  /**
   * This takes an event and a list of attribute names. It returns a new event
   * that has the same core event values and all of the attribute/values
   * *except* for those attributes sepcified by the list.
   */
  public static Event unselect(Event e, String... attrs) {
    Event e2 = new EventImpl(e.getBody(), e.getTimestamp(), e.getPriority(), e
        .getNanos(), e.getHost());
    List<String> as = Arrays.asList(attrs);
    for (Entry<String, byte[]> ent : e.getAttrs().entrySet()) {
      String a = ent.getKey();
      if (as.contains(a)) {
        continue; // don't add it if it is in the unselect list.
      }

      byte[] data = e.get(a);
      e2.set(a, data);
    }
    return e2;

  }
}
