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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyslogUtils {

  private Mode m = Mode.START;
  private StringBuilder prio = new StringBuilder();
  private ByteArrayOutputStream baos;
  private static final Logger logger = LoggerFactory
      .getLogger(SyslogUtils.class);

  final public static String SYSLOG_FACILITY = "Facility";
  final public static String SYSLOG_SEVERITY = "Severity";
  final public static String EVENT_STATUS = "flume.syslog.status";
  final public static Integer MIN_SIZE = 10;
  final public static Integer DEFAULT_SIZE = 2500;
  private final boolean isUdp;
  private boolean isBadEvent;
  private boolean isIncompleteEvent;
  private Integer maxSize;

  public SyslogUtils() {
    this(false);
  }

  public SyslogUtils(boolean isUdp) {
    this(DEFAULT_SIZE, isUdp);
  }

  public SyslogUtils(Integer eventSize, boolean isUdp){
    this.isUdp = isUdp;
    isBadEvent = false;
    isIncompleteEvent = false;
    maxSize = (eventSize < MIN_SIZE) ? MIN_SIZE : eventSize;
    baos = new ByteArrayOutputStream(eventSize);
  }

  enum Mode {
    START, PRIO, DATA
  };

  public enum SyslogStatus{
    OTHER("Unknown"),
    INVALID("Invalid"),
    INCOMPLETE("Incomplete");

    private final String syslogStatus;

    private SyslogStatus(String status){
      syslogStatus = status;
    }

    public String getSyslogStatus(){
      return this.syslogStatus;
    }
  }

  // create the event from syslog data
  Event buildEvent() {
    int pri = 0;
    int sev = 0;
    int facility = 0;
    if(!isBadEvent){
      pri = Integer.parseInt(prio.toString());
      sev = pri % 8;
      facility = pri - sev;
    }
    Map <String, String> headers = new HashMap<String, String>();
    headers.put(SYSLOG_FACILITY, String.valueOf(facility));
    headers.put(SYSLOG_SEVERITY, String.valueOf(sev));
    if(isBadEvent){
      logger.warn("Event created from Invalid Syslog data.");
      headers.put(EVENT_STATUS, SyslogStatus.INVALID.getSyslogStatus());
    } else if(isIncompleteEvent){
      logger.warn("Event size larger than specified event size: {}. You should " +
          "consider increasing your event size.", maxSize);
      headers.put(EVENT_STATUS, SyslogStatus.INCOMPLETE.getSyslogStatus());
    }
    // TODO: add hostname and timestamp if provided ...

    byte[] body = baos.toByteArray();
    reset();
    return EventBuilder.withBody(body, headers);
  }

  private void reset(){
    baos.reset();
    m = Mode.START;
    prio.delete(0, prio.length());
    isBadEvent = false;
    isIncompleteEvent = false;

  }

  // extract relevant syslog data needed for building Flume event
  public Event extractEvent(ChannelBuffer in){

    /* for protocol debugging
    ByteBuffer bb = in.toByteBuffer();
    int remaining = bb.remaining();
    byte[] buf = new byte[remaining];
    bb.get(buf);
    HexDump.dump(buf, 0, System.out, 0);
    */

    byte b = 0;
    Event e = null;
    boolean doneReading = false;

    try {
      while (!doneReading && in.readable()) {
        b = in.readByte();
        switch (m) {
        case START:
          if (b == '<') {
            m = Mode.PRIO;
          } else if(b == '\n'){
          //If the character is \n, it was because the last event was exactly
          //as long  as the maximum size allowed and
          //the only remaining character was the delimiter - '\n', or
          //multiple delimiters were sent in a row.
          //Just ignore it, and move forward, don't change the mode.
          //This is a no-op, just ignore it.
            logger.debug("Delimiter found while in START mode, ignoring..");

          } else {
            isBadEvent = true;
            baos.write(b);
            //Bad event, just dump everything as if it is data.
            m = Mode.DATA;
          }
          break;
        case PRIO:
          if (b == '>') {
            m = Mode.DATA;
          } else {
            char ch = (char) b;
            prio.append(ch);
            if (!Character.isDigit(ch)) {
              isBadEvent = true;
              //Append the priority to baos:
              String badPrio = "<"+ prio;
              baos.write(badPrio.getBytes());
              //If we hit a bad priority, just write as if everything is data.
              m = Mode.DATA;
            }
          }
          break;
        case DATA:
          // TCP syslog entries are separated by '\n'
          if (b == '\n') {
            e = buildEvent();
            doneReading = true;
          } else {
            baos.write(b);
          }
          if(baos.size() == this.maxSize && !doneReading){
            isIncompleteEvent = true;
            e = buildEvent();
            doneReading = true;
          }
          break;
        }

      }

      // UDP doesn't send a newline, so just use what we received
      if (e == null && isUdp) {
        doneReading = true;
        e = buildEvent();
      }
    //} catch (IndexOutOfBoundsException eF) {
    //    e = buildEvent(prio, baos);
    } catch (IOException e1) {
      //no op
    } finally {
      // no-op
    }

    return e;
  }

  public Integer getEventSize() {
    return maxSize;
  }

  public void setEventSize(Integer eventSize) {
    this.maxSize = eventSize;
  }

}
