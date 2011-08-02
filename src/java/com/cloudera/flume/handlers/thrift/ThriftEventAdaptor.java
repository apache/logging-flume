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

import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringEscapeUtils;

import com.cloudera.flume.core.Event;
import com.google.common.base.Preconditions;

/**
 * This wraps a Thrift generated ThriftFlumeEvent with a Flume Event interface.
 * 
 * This should only be used by the ThriftEventSink, ThriftEventSource, and
 * ThriftFlumeEventServerImpl. Its constructor, and static conversion function
 * are purposely package protected.
 */
class ThriftEventAdaptor extends Event {

  ThriftFlumeEvent evt;

  ThriftEventAdaptor(ThriftFlumeEvent evt) {
    super();
    this.evt = evt;
  }

  @Override
  public byte[] getBody() {
    return evt.getBody();
  }

  @Override
  public Priority getPriority() {
    return convert(evt.getPriority());
  }

  @Override
  public long getTimestamp() {
    return evt.timestamp;
  }

  public static Priority convert(com.cloudera.flume.handlers.thrift.Priority p) {
    Preconditions.checkNotNull(p, "Argument must not be null.");

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

  public static com.cloudera.flume.handlers.thrift.Priority convert(Priority p) {
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

  @Override
  public String toString() {
    String mbody = StringEscapeUtils.escapeJava(new String(getBody()));
    return "[" + getPriority().toString() + " " + new Date(getTimestamp())
        + "] " + mbody;
  }

  @Override
  public long getNanos() {
    return evt.getNanos();
  }

  @Override
  public String getHost() {
    return evt.getHost();
  }

  /**
   * This makes a thrift compatible copy of the event. It is here to encapsulate
   * future changes to the Event/ThriftFlumeEvent interface
   */
  public static ThriftFlumeEvent convert(Event e) {
    return new ThriftFlumeEvent(e.getTimestamp(), convert(e.getPriority()), e
        .getBody(), e.getNanos(), e.getHost(), e.getAttrs());
  }

  @Override
  public byte[] get(String attr) {
    return evt.fields.get(attr);
  }

  @Override
  public Map<String, byte[]> getAttrs() {
    if (evt.fields == null) {
      return Collections.<String, byte[]> emptyMap();
    }
    return Collections.unmodifiableMap(evt.fields);
  }

  @Override
  public void set(String attr, byte[] v) {
    if (evt.fields.get(attr) != null) {
      throw new IllegalArgumentException(
          "Event already had an event with attribute " + attr);
    }
    evt.fields.put(attr, v);
  }

  @Override
  public void hierarchicalMerge(String prefix, Event e) {
    throw new NotImplementedException();
  }

  @Override
  public void merge(Event e) {
    throw new NotImplementedException();
  }

}
