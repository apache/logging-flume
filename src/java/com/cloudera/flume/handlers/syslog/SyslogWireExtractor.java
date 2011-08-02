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
package com.cloudera.flume.handlers.syslog;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.text.EventExtractException;
import com.cloudera.flume.handlers.text.Extractor;
import com.google.common.base.Preconditions;

/**
 * This uses the Extractor interface to take a DataInputStream an extract Events
 * out. This in combination with removing syscalls to unixtime when
 * instantiating EventImpls significantly improved performance when compared to
 * the previous regex based approach.
 */
public class SyslogWireExtractor implements Extractor, SyslogConsts {
  final static Logger LOG =
      Logger.getLogger(SyslogWireExtractor.class.getName());

  static SyslogWireExtractor format = new SyslogWireExtractor();

  static int calcSyslogPrio(Event e) {
    int slPrio = 0;

    byte[] fac = e.get(SYSLOG_FACILITY);
    if (fac == null || fac.length != 1) {
      slPrio = 1 * 8; // default to syslog facility.
    } else {
      slPrio = fac[0] * 8;
    }

    byte[] sev = e.get(SYSLOG_SEVERITY);
    if (sev == null || sev.length != 1) {
      slPrio += PRIO2SEVERITY[e.getPriority().ordinal()];
    } else {
      slPrio += sev[0];
    }
    return slPrio;
  }

  /**
   * This is a version that removes unneeded character encoding and decoding
   * steps.
   */
  public byte[] toBytes(Event e) {
    try {
      int slPrio = calcSyslogPrio(e);

      ByteArrayOutputStream bais = new ByteArrayOutputStream();
      bais.write('<');
      bais.write(("" + slPrio).getBytes());
      bais.write('>');
      bais.write(e.getBody());
      bais.write('\n');
      return bais.toByteArray();
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      LOG.warn("Ran out of bytes during extraction", e1);
    }
    return null;
  }

  public static Event extractEvent(DataInputStream in)
      throws EventExtractException {
    return format.extract(in);
    // return format.extract(in);

  }

  enum Mode {
    START, PRIO, DATA, ERR
  };

  static Event buildEvent(StringBuilder prio, ByteArrayOutputStream baos) {

    int pri = Integer.parseInt(prio.toString());
    byte[] facility = { (byte) (pri / 8) };
    byte[] sev = { (byte) (pri % 8) };

    // 15.2s 14.9s 15.2s
    // Event e = new EventImpl(empty, 0,
    // SyslogWireFormat.SEVERITY[sev[0]], 0, "localhost");

    // 15.0s 15.0s 15.2s
    // Event e =
    // new EventImpl(baos.toByteArray(), 0, SEVERITY[sev[0]], 0, NetUtils
    // .localhost());

    // 15.7s 15.3s 14.9s
    // Event e = new EventImpl(baos.toByteArray(), 0,
    // SyslogWireFormat.SEVERITY[sev[0]], 0, "localhost");

    // // Pick correctness over efficiency

    // 27.1s (due to sys calls).
    Event e = new EventImpl(baos.toByteArray());

    // 24.5s 24.9s 25.6s (due to sys calls)
    // Event e = new EventImpl(empty);

    e.set(SYSLOG_FACILITY, facility);
    e.set(SYSLOG_SEVERITY, sev);

    return e;
  }

  /**
   * This is basically a state machine implementation of the extract function.
   * It uses a DataInputStream instead of a string to avoid the cost of string
   * and character encoding
   */
  public Event extract(DataInputStream in) throws EventExtractException {
    Preconditions.checkNotNull(in);
    Mode m = Mode.START;
    StringBuilder prio = new StringBuilder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte b = 0;
    long cnt = 0;
    try {
      while (true) {
        b = in.readByte();
        cnt++;
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
          if (b == '\n') {
            Event e = buildEvent(prio, baos);
            return e;
          }

          baos.write(b);
          break;
        case ERR:
          // read until we get to a \n
          if (b == '\n') {
            throw new EventExtractException(
                "Failed to extract syslog wire entry");
          }
          // stay in Mode.ERR;
          break;
        }
      }
    } catch (EOFException e) {
      switch (m) {
      case ERR:
        // end of stream but was in error state? Throw extraction exception
        throw new EventExtractException("Failed to extract syslog wire entry");
      case DATA:
        // end of stream but had data, return it.
        return buildEvent(prio, baos);
      default:
        // if not in error state just return done;
        return null;
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new EventExtractException("Failed to extract syslog wire entry");
    }
  }

  public static byte[] formatEventToBytes(Event e) {
    return format.toBytes(e);
  }

}
