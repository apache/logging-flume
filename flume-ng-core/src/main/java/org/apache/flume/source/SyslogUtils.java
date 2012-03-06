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

package org.apache.flume.source;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyslogUtils {

  protected String host = null;
  protected int port;
  protected Channel nettyChannel;
  private static final Logger logger = LoggerFactory
      .getLogger(SyslogUtils.class);

  final public static String SYSLOG_FACILITY = "Facility";
  final public static String SYSLOG_SEVERITY = "Severity";

  enum Mode {
    START, PRIO, DATA, ERR
  };

  // create the event from syslog data
  static Event buildEvent(StringBuilder prio,
      ByteArrayOutputStream baos) {

    int pri = Integer.parseInt(prio.toString());
    int  sev = pri % 8;
    int facility = pri - sev;
    Map <String, String> headers = new HashMap<String, String>();
    headers.put(SYSLOG_FACILITY, String.valueOf(facility));
    headers.put(SYSLOG_SEVERITY, String.valueOf(sev));
    // TODO: add hostname and timestamp if provided ...

    return EventBuilder.withBody(baos.toByteArray(), headers);
  }

  // extract relevant syslog data needed for building Flume event
  public static Event extractEvent(ChannelBuffer in)
        throws IOException {
    StringBuilder prio = new StringBuilder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte b = 0;
    Mode m = Mode.START;
    Event e = null;
    boolean doneReading = false;

    try {
      while (!doneReading) {
        b = in.readByte();
        switch (m) {
        case START:
          if (b == '<') {
            m = Mode.PRIO;
          } else {
            m = Mode.ERR;
          }
          break;
        case PRIO:
          if (b == '>') {
            m = Mode.DATA;
          } else {
            char ch = (char) b;
            if (Character.isDigit(ch)) {
              prio.append(ch); // stay in PRIO mode
            } else {
              m = Mode.ERR;
            }
          }
          break;
        case DATA:
          // TCP syslog entries are separated by '\n'
          if (b == '\n') {
            e = buildEvent(prio, baos);
            doneReading = true;
          }

          baos.write(b);
          break;
        case ERR:
          if (b == '<') {
            // check if its start of new event
            m = Mode.PRIO;
          }
          // otherwise stay in Mode.ERR;
          break;
        }
      }
      return null;
    } catch (IndexOutOfBoundsException eF) {
        e = buildEvent(prio, baos);
    }
    return e;
  }

}
