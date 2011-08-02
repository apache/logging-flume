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
package com.cloudera.flume.handlers.avro;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringEscapeUtils;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.google.common.base.Preconditions;

/**
 * This wraps a Avro generated AvroFlumeEvent with a Flume Event interface.
 */
public class AvroEventAdaptor extends Event {
  AvroFlumeEvent evt = null;

  AvroEventAdaptor(AvroFlumeEvent evt) {
    super();
    this.evt = evt;
  }

  @Override
  public byte[] getBody() {
    return evt.body.array();
  }

  @Override
  public Priority getPriority() {
    return convert(evt.priority);
  }

  @Override
  public long getTimestamp() {
    return evt.timestamp;
  }

  public static Priority convert(com.cloudera.flume.handlers.avro.Priority p) {
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

  public static com.cloudera.flume.handlers.avro.Priority convert(Priority p) {
    Preconditions.checkNotNull(p, "Argument must not be null.");
    switch (p) {
    case FATAL:
      return com.cloudera.flume.handlers.avro.Priority.FATAL;
    case ERROR:
      return com.cloudera.flume.handlers.avro.Priority.ERROR;
    case WARN:
      return com.cloudera.flume.handlers.avro.Priority.WARN;
    case INFO:
      return com.cloudera.flume.handlers.avro.Priority.INFO;
    case DEBUG:
      return com.cloudera.flume.handlers.avro.Priority.DEBUG;
    case TRACE:
      return com.cloudera.flume.handlers.avro.Priority.TRACE;
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
    return evt.nanos;
  }

  @Override
  public String getHost() {
    return evt.host.toString();
  }

  public static AvroFlumeEvent convert(Event e) {
    AvroFlumeEvent tempAvroEvt = new AvroFlumeEvent();

    tempAvroEvt.timestamp = e.getTimestamp();
    tempAvroEvt.priority = convert(e.getPriority());
    ByteBuffer bbuf = ByteBuffer.wrap(e.getBody());

    tempAvroEvt.body = bbuf;
    tempAvroEvt.nanos = e.getNanos();
    tempAvroEvt.host = e.getHost();

    tempAvroEvt.fields = new HashMap<CharSequence, ByteBuffer>();
    for (String s : e.getAttrs().keySet()) {
      // wrap a ByteBuffer around e.getAttrs().get(s)
      // also note that e.getAttrs().get(s) is immutable
      ByteBuffer temp = ByteBuffer.wrap(e.getAttrs().get(s));
      tempAvroEvt.fields.put(s, temp);
    }
    return tempAvroEvt;
  }

  /**
   * This returns the FlumeEvent corresponding to the AvroEvent passed in the
   * constructor of this object.
   */
  public Event toFlumeEvent() {
    Preconditions.checkNotNull(evt, "AvroFlumeEvent is not initialized");
    return new EventImpl(this.getBody(), this.getTimestamp(), this
        .getPriority(), this.getNanos(), this.getHost(), this.getAttrs());
  }

  @Override
  public byte[] get(String attr) {
    return evt.fields.get(attr).array();
  }

  @Override
  public Map<String, byte[]> getAttrs() {
    if (evt.fields == null) {
      return Collections.<String, byte[]> emptyMap();
    }
    HashMap<String, byte[]> tempMap = new HashMap<String, byte[]>();
    for (CharSequence u : evt.fields.keySet()) {
      tempMap.put(u.toString(), evt.fields.get(u).array());
    }
    return tempMap;
  }

  @Override
  public void set(String attr, byte[] value) {
    if (evt.fields.get(attr) != null) {
      throw new IllegalArgumentException(
          "Event already had an event with attribute " + attr);
    }
    ByteBuffer bbuf = ByteBuffer.wrap(value);
    evt.fields.put(attr, bbuf);
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
