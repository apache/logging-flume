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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.google.common.base.Preconditions;

/**
 * This utility class contains methods that convert Avro-generated
 * AvroFlumeEvents into an FlumeEvents and vice versa.
 */
public class AvroEventConvertUtil {

  private AvroEventConvertUtil() {
  }

  public static final Logger LOG = LoggerFactory
      .getLogger(AvroEventConvertUtil.class);

  public static Event toFlumeEvent(AvroFlumeEvent evt) {
    return toFlumeEvent(evt, false);
  }

  public static Event toFlumeEvent(AvroFlumeEvent evt, boolean truncates) {
    Preconditions.checkArgument(evt != null, "AvorFlumeEvent is null!");

    byte[] body = convertBody(evt.body, truncates);
    com.cloudera.flume.handlers.avro.Priority p = evt.priority;
    p = (p == null) ? com.cloudera.flume.handlers.avro.Priority.INFO : p;
    String host = (evt.host == null) ? "" : evt.host.toString();
    Map<String, byte[]> attrs = getAttrs(evt.fields);
    return new EventImpl(body, evt.timestamp, toFlumePriority(p), evt.nanos,
        host, attrs);
  }

  private static byte[] convertBody(ByteBuffer buf, boolean truncates) {
    if (buf == null) {
      LOG.warn("Avro Event had null body! returning empty body");
      return new byte[0];
    }
    byte[] bytes = buf.array();
    int maxSz = (int) FlumeConfiguration.get().getEventMaxSizeBytes();
    if (bytes.length > maxSz) {
      Preconditions.checkArgument(truncates,
          "Unexpected too long Avro Event body: max is " + maxSz
              + " but body was buf.length");
      byte[] trunc = Arrays.copyOf(bytes, maxSz);
      return trunc;
    }
    // normal case
    return bytes;
  }

  private static com.cloudera.flume.core.Event.Priority toFlumePriority(
      com.cloudera.flume.handlers.avro.Priority p) {
    Preconditions.checkNotNull(p, "Priority argument must be valid.");
    switch (p) {
    case FATAL:
      return com.cloudera.flume.core.Event.Priority.FATAL;
    case ERROR:
      return com.cloudera.flume.core.Event.Priority.ERROR;
    case WARN:
      return com.cloudera.flume.core.Event.Priority.WARN;
    case INFO:
      return com.cloudera.flume.core.Event.Priority.INFO;
    case DEBUG:
      return com.cloudera.flume.core.Event.Priority.DEBUG;
    case TRACE:
      return com.cloudera.flume.core.Event.Priority.TRACE;
    default:
      throw new IllegalStateException("Unknown value " + p);
    }
  }

  private static com.cloudera.flume.handlers.avro.Priority toAvroPriority(
      com.cloudera.flume.core.Event.Priority p) {
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

  public static AvroFlumeEvent toAvroEvent(Event e) {
    AvroFlumeEvent tempAvroEvt = new AvroFlumeEvent();

    tempAvroEvt.timestamp = e.getTimestamp();
    tempAvroEvt.priority = toAvroPriority(e.getPriority());
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

  private static Map<String, byte[]> getAttrs(
      Map<CharSequence, ByteBuffer> fields) {
    if (fields == null) {
      return Collections.<String, byte[]> emptyMap();
    }
    HashMap<String, byte[]> tempMap = new HashMap<String, byte[]>();
    for (CharSequence u : fields.keySet()) {
      tempMap.put(u.toString(), fields.get(u).array());
    }
    return tempMap;
  }

}
