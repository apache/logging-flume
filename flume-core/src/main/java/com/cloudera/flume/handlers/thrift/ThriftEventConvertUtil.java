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
package com.cloudera.flume.handlers.thrift;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.core.EventImpl;
import com.google.common.base.Preconditions;

/**
 * This converts a Thrift generated ThriftFlumeEvent in to a FlumeEvent.
 * 
 * This should only be used by the ThriftEventSink, ThriftEventSource, and
 * ThriftFlumeEventServerImpl. Its constructor, and static conversion function
 * are purposely package protected.
 */
class ThriftEventConvertUtil {
  public static final Logger LOG = LoggerFactory
      .getLogger(ThriftEventConvertUtil.class);

  private ThriftEventConvertUtil() {
  }

  public static Event toFlumeEvent(ThriftFlumeEvent evt) {
    return toFlumeEvent(evt, false);
  }

  public static Event toFlumeEvent(ThriftFlumeEvent evt, boolean truncates) {
    Preconditions.checkArgument(evt != null, "ThriftFlumeEvent is null!");

    byte[] body = convertBody(evt.getBody(), truncates);
    com.cloudera.flume.handlers.thrift.Priority p = evt.getPriority();
    p = (p == null) ? com.cloudera.flume.handlers.thrift.Priority.INFO : p;
    Map<String, byte[]> attrs = getAttrs(evt);
    return new EventImpl(body, evt.getTimestamp(), toFlumePriority(p),
        evt.getNanos(), evt.getHost(), attrs);
  }

  private static byte[] convertBody(byte[] buf, boolean truncates) {
    if (buf == null) {
      LOG.warn("Thrift Event had null body! returning empty body");
      return new byte[0];
    }

    int maxSz = (int) FlumeConfiguration.get().getEventMaxSizeBytes();
    if (buf.length > maxSz) {
      Preconditions.checkArgument(truncates,
          "Unexpected too long Thrift Event body: max is " + maxSz
              + " but body was buf.length");
      byte[] trunc = Arrays.copyOf(buf, maxSz);
      return trunc;
    }
    // normal case
    return buf;
  }

  private static com.cloudera.flume.core.Event.Priority toFlumePriority(
      com.cloudera.flume.handlers.thrift.Priority p) {
    Preconditions.checkNotNull(p, "Priority argument must be valid.");

    switch (p) {
    case FATAL:
      return Priority.FATAL;
    case ERROR:
      return Priority.ERROR;
    case WARN:
      return Priority.WARN;
    case INFO:
      return Priority.INFO;
    case DEBUG:
      return Priority.DEBUG;
    case TRACE:
      return Priority.TRACE;
    default:
      throw new IllegalStateException("Unknown value " + p);
    }
  }

  private static com.cloudera.flume.handlers.thrift.Priority toThriftPriority(
      com.cloudera.flume.core.Event.Priority p) {
    Preconditions.checkNotNull(p, "Argument must not be null.");

    switch (p) {
    case FATAL:
      return com.cloudera.flume.handlers.thrift.Priority.FATAL;
    case ERROR:
      return com.cloudera.flume.handlers.thrift.Priority.ERROR;
    case WARN:
      return com.cloudera.flume.handlers.thrift.Priority.WARN;
    case INFO:
      return com.cloudera.flume.handlers.thrift.Priority.INFO;
    case DEBUG:
      return com.cloudera.flume.handlers.thrift.Priority.DEBUG;
    case TRACE:
      return com.cloudera.flume.handlers.thrift.Priority.TRACE;
    default:
      throw new IllegalStateException("Unknown value " + p);
    }
  }

  /**
   * This makes a thrift compatible copy of the event. It is here to encapsulate
   * future changes to the Event/ThriftFlumeEvent interface
   */
  public static ThriftFlumeEvent toThriftEvent(Event e) {
    ThriftFlumeEvent evt = new ThriftFlumeEvent();
    evt.timestamp = e.getTimestamp();
    evt.priority = toThriftPriority(e.getPriority());
    ByteBuffer buf = ByteBuffer.wrap(e.getBody());
    evt.body = buf;
    evt.nanos = e.getNanos();
    evt.host = e.getHost();

    Map<String, byte[]> tempMap = e.getAttrs();
    Map<String, ByteBuffer> returnMap = new HashMap<String, ByteBuffer>();
    for (String key : tempMap.keySet()) {
      buf.clear();
      buf = ByteBuffer.wrap(tempMap.get(key));
      returnMap.put(key, buf);
    }

    evt.fields = returnMap;
    return evt;
  }

  private static Map<String, byte[]> getAttrs(ThriftFlumeEvent evt) {
    if (evt.fields == null) {
      return new HashMap<String, byte[]>();
    }
    Map<String, ByteBuffer> tempMap = Collections.unmodifiableMap(evt.fields);
    Map<String, byte[]> returnMap = new HashMap<String, byte[]>();
    for (String key : tempMap.keySet()) {
      ByteBuffer buf = tempMap.get(key);
      returnMap.put(key, buf.array());
    }
    return returnMap;
  }

}
