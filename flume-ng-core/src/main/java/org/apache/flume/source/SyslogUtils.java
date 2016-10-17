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

import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SyslogUtils {
  public static final String SYSLOG_TIMESTAMP_FORMAT_RFC5424_2 = "yyyy-MM-dd'T'HH:mm:ss.SZ";
  public static final String SYSLOG_TIMESTAMP_FORMAT_RFC5424_1 = "yyyy-MM-dd'T'HH:mm:ss.S";
  public static final String SYSLOG_TIMESTAMP_FORMAT_RFC5424_3 = "yyyy-MM-dd'T'HH:mm:ssZ";
  public static final String SYSLOG_TIMESTAMP_FORMAT_RFC5424_4 = "yyyy-MM-dd'T'HH:mm:ss";
  public static final String SYSLOG_TIMESTAMP_FORMAT_RFC3164_1 = "yyyyMMM d HH:mm:ss";

  public static final String SYSLOG_MSG_RFC5424_0 =
      "(?:\\<(\\d{1,3})\\>)" + // priority
          "(?:(\\d?)\\s?)" + // version
          /* yyyy-MM-dd'T'HH:mm:ss.SZ or yyyy-MM-dd'T'HH:mm:ss.S+hh:mm or - (null stamp) */
          "(?:" +
          "(\\d{4}[-]\\d{2}[-]\\d{2}[T]\\d{2}[:]\\d{2}[:]\\d{2}" +
          "(?:\\.\\d{1,6})?(?:[+-]\\d{2}[:]\\d{2}|Z)?)|-)" + // stamp
          "\\s" + // separator
          "(?:([\\w][\\w\\d\\.@\\-]*)|-)" + // host name or - (null)
          "\\s" + // separator
          "(.*)$"; // body

  public static final String SYSLOG_MSG_RFC3164_0 =
      "(?:\\<(\\d{1,3})\\>)" +
          "(?:(\\d)?\\s?)" + // version
          // stamp MMM d HH:mm:ss, single digit date has two spaces
          "([A-Z][a-z][a-z]\\s{1,2}\\d{1,2}\\s\\d{2}[:]\\d{2}[:]\\d{2})" +
          "\\s" + // separator
          "([\\w][\\w\\d\\.@-]*)" + // host
          "\\s(.*)$";  // body

  public static final int SYSLOG_PRIORITY_POS = 1;
  public static final int SYSLOG_VERSION_POS = 2;
  public static final int SYSLOG_TIMESTAMP_POS = 3;
  public static final int SYSLOG_HOSTNAME_POS = 4;
  public static final int SYSLOG_BODY_POS = 5;

  private Mode m = Mode.START;
  private StringBuilder prio = new StringBuilder();
  private ByteArrayOutputStream baos;
  private static final Logger logger = LoggerFactory
      .getLogger(SyslogUtils.class);

  public static final String SYSLOG_FACILITY = "Facility";
  public static final String SYSLOG_SEVERITY = "Severity";
  public static final String SYSLOG_PRIORITY = "Priority";
  public static final String SYSLOG_VERSION = "Version";
  public static final String EVENT_STATUS = "flume.syslog.status";
  public static final Integer MIN_SIZE = 10;
  public static final Integer DEFAULT_SIZE = 2500;
  private final boolean isUdp;
  private boolean isBadEvent;
  private boolean isIncompleteEvent;
  private Integer maxSize;
  private Set<String> keepFields;

  private class SyslogFormatter {
    public Pattern regexPattern;
    public ArrayList<String> searchPattern = new ArrayList<String>();
    public ArrayList<String> replacePattern = new ArrayList<String>();
    public ArrayList<SimpleDateFormat> dateFormat = new ArrayList<SimpleDateFormat>();
    public boolean addYear;
  }

  private ArrayList<SyslogFormatter> formats = new ArrayList<SyslogFormatter>();

  private String priority = null;
  private String version = null;
  private String timeStamp = null;
  private String hostName = null;
  private String msgBody = null;

  private static final String[] DEFAULT_FIELDS_TO_KEEP = {
      SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_PRIORITY,
      SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_VERSION,
      SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_TIMESTAMP,
      SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_HOSTNAME
  };
  public static final String KEEP_FIELDS_ALL = "--all--";

  public static boolean keepAllFields(Set<String> keepFields) {
    if (keepFields == null) {
      return false;
    }
    return keepFields.contains(KEEP_FIELDS_ALL);
  }

  public static Set<String> chooseFieldsToKeep(String keepFields) {
    if (keepFields == null) {
      return null;
    }

    keepFields = keepFields.trim().toLowerCase(Locale.ENGLISH);

    if (keepFields.equals("false") || keepFields.equals("none")) {
      return null;
    }

    if (keepFields.equals("true") || keepFields.equals("all")) {
      Set<String> fieldsToKeep = new HashSet<String>(1);
      fieldsToKeep.add(KEEP_FIELDS_ALL);
      return fieldsToKeep;
    }

    Set<String> fieldsToKeep = new HashSet<String>(DEFAULT_FIELDS_TO_KEEP.length);

    for (String field : DEFAULT_FIELDS_TO_KEEP) {
      if (keepFields.indexOf(field) != -1) {
        fieldsToKeep.add(field);
      }
    }

    return fieldsToKeep;
  }

  public static String addFieldsToBody(Set<String> keepFields,
                                       String body,
                                       String priority,
                                       String version,
                                       String timestamp,
                                       String hostname) {
    // Prepend fields to be kept in message body.
    if (keepFields != null) {
      if (keepFields.contains(SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_HOSTNAME)) {
        body = hostname + " " + body;
      }
      if (keepFields.contains(SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_TIMESTAMP)) {
        body = timestamp + " " + body;
      }
      if (keepFields.contains(SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_VERSION)) {
        if (version != null && !version.isEmpty()) {
          body = version + " " + body;
        }
      }
      if (keepFields.contains(SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_PRIORITY)) {
        body = "<" + priority + ">" + body;
      }
    }

    return body;
  }

  public SyslogUtils() {
    this(false);
  }

  public SyslogUtils(boolean isUdp) {
    this(DEFAULT_SIZE,
        new HashSet<String>(Arrays.asList(SyslogSourceConfigurationConstants.DEFAULT_KEEP_FIELDS)),
        isUdp);
  }

  public SyslogUtils(Integer eventSize, Set<String> keepFields, boolean isUdp) {
    this.isUdp = isUdp;
    isBadEvent = false;
    isIncompleteEvent = false;
    maxSize = (eventSize < MIN_SIZE) ? MIN_SIZE : eventSize;
    baos = new ByteArrayOutputStream(eventSize);
    this.keepFields = keepFields;
    initHeaderFormats();
  }

  // extend the default header formatter
  public void addFormats(Map<String, String> formatProp) {
    if (formatProp.isEmpty() || !formatProp.containsKey(
        SyslogSourceConfigurationConstants.CONFIG_REGEX)) {
      return;
    }
    SyslogFormatter fmt1 = new SyslogFormatter();
    fmt1.regexPattern = Pattern.compile(
        formatProp.get(SyslogSourceConfigurationConstants.CONFIG_REGEX));
    if (formatProp.containsKey(SyslogSourceConfigurationConstants.CONFIG_SEARCH)) {
      fmt1.searchPattern.add(
          formatProp.get(SyslogSourceConfigurationConstants.CONFIG_SEARCH));
    }
    if (formatProp.containsKey(SyslogSourceConfigurationConstants.CONFIG_REPLACE)) {
      fmt1.replacePattern.add(
          formatProp.get(SyslogSourceConfigurationConstants.CONFIG_REPLACE));
    }
    if (formatProp.containsKey(SyslogSourceConfigurationConstants.CONFIG_DATEFORMAT)) {
      fmt1.dateFormat.add(new SimpleDateFormat(
          formatProp.get(SyslogSourceConfigurationConstants.CONFIG_DATEFORMAT)));
    }
    formats.add(0, fmt1);
  }

  // setup built-in formats
  private void initHeaderFormats() {
    // setup RFC5424 formater
    SyslogFormatter fmt1 = new SyslogFormatter();
    fmt1.regexPattern = Pattern.compile(SYSLOG_MSG_RFC5424_0);
    // 'Z' in timestamp indicates UTC zone, so replace it it with '+0000' for date formatting
    fmt1.searchPattern.add("Z");
    fmt1.replacePattern.add("+0000");
    // timezone in RFC5424 is [+-]tt:tt, so remove the ':' for java date formatting
    fmt1.searchPattern.add("([+-])(\\d{2})[:](\\d{2})");
    fmt1.replacePattern.add("$1$2$3");
    // FLUME-2497: SimpleDateFormat does not handle microseconds, Truncate after 3 digits.
    fmt1.searchPattern.add("(T\\d{2}:\\d{2}:\\d{2}\\.\\d{3})(\\d*)");
    fmt1.replacePattern.add("$1");
    fmt1.dateFormat.add(new SimpleDateFormat(SYSLOG_TIMESTAMP_FORMAT_RFC5424_1, Locale.ENGLISH));
    fmt1.dateFormat.add(new SimpleDateFormat(SYSLOG_TIMESTAMP_FORMAT_RFC5424_2, Locale.ENGLISH));
    fmt1.dateFormat.add(new SimpleDateFormat(SYSLOG_TIMESTAMP_FORMAT_RFC5424_3, Locale.ENGLISH));
    fmt1.dateFormat.add(new SimpleDateFormat(SYSLOG_TIMESTAMP_FORMAT_RFC5424_4, Locale.ENGLISH));
    fmt1.addYear = false;

    // setup RFC3164 formater
    SyslogFormatter fmt2 = new SyslogFormatter();
    fmt2.regexPattern = Pattern.compile(SYSLOG_MSG_RFC3164_0);
    // the single digit date has two spaces, so trim it
    fmt2.searchPattern.add("  ");
    fmt2.replacePattern.add(" ");
    fmt2.dateFormat.add(new SimpleDateFormat(SYSLOG_TIMESTAMP_FORMAT_RFC3164_1, Locale.ENGLISH));
    fmt2.addYear = true;

    formats.add(fmt1);
    formats.add(fmt2);
  }

  enum Mode {
    START, PRIO, DATA
  }

  ;

  public enum SyslogStatus {
    OTHER("Unknown"),
    INVALID("Invalid"),
    INCOMPLETE("Incomplete");

    private final String syslogStatus;

    private SyslogStatus(String status) {
      syslogStatus = status;
    }

    public String getSyslogStatus() {
      return this.syslogStatus;
    }
  }

  // create the event from syslog data
  Event buildEvent() {
    try {
      byte[] body;
      int pri = 0;
      int sev = 0;
      int facility = 0;

      if (!isBadEvent) {
        pri = Integer.parseInt(prio.toString());
        sev = pri % 8;
        facility = pri / 8;
        formatHeaders();
      }

      Map<String, String> headers = new HashMap<String, String>();
      headers.put(SYSLOG_FACILITY, String.valueOf(facility));
      headers.put(SYSLOG_SEVERITY, String.valueOf(sev));
      if ((priority != null) && (priority.length() > 0)) {
        headers.put("priority", priority);
      }
      if ((version != null) && (version.length() > 0)) {
        headers.put("version", version);
      }
      if ((timeStamp != null) && timeStamp.length() > 0) {
        headers.put("timestamp", timeStamp);
      }
      if ((hostName != null) && (hostName.length() > 0)) {
        headers.put("host", hostName);
      }
      if (isBadEvent) {
        logger.warn("Event created from Invalid Syslog data.");
        headers.put(EVENT_STATUS, SyslogStatus.INVALID.getSyslogStatus());
      } else if (isIncompleteEvent) {
        logger.warn("Event size larger than specified event size: {}. You should " +
            "consider increasing your event size.", maxSize);
        headers.put(EVENT_STATUS, SyslogStatus.INCOMPLETE.getSyslogStatus());
      }

      if (!keepAllFields(keepFields)) {
        if ((msgBody != null) && (msgBody.length() > 0)) {
          body = msgBody.getBytes();
        } else {
          // Parse failed.
          body = baos.toByteArray();
        }
      } else {
        body = baos.toByteArray();
      }
      // format the message
      return EventBuilder.withBody(body, headers);
    } finally {
      reset();
    }
  }

  // Apply each known pattern to message
  private void formatHeaders() {
    String eventStr = baos.toString();
    String timeStampString = null;

    for (int p = 0; p < formats.size(); p++) {
      SyslogFormatter fmt = formats.get(p);
      Pattern pattern = fmt.regexPattern;
      Matcher matcher = pattern.matcher(eventStr);
      if (!matcher.matches()) {
        continue;
      }
      MatchResult res = matcher.toMatchResult();
      for (int grp = 1; grp <= res.groupCount(); grp++) {
        String value = res.group(grp);
        if (grp == SYSLOG_TIMESTAMP_POS) {
          timeStampString = value;

          // apply available format replacements to timestamp
          if (value != null) {
            for (int sp = 0; sp < fmt.searchPattern.size(); sp++) {
              value = value.replaceAll(fmt.searchPattern.get(sp), fmt.replacePattern.get(sp));
            }
            // Add year to timestamp if needed
            if (fmt.addYear) {
              value = String.valueOf(Calendar.getInstance().get(Calendar.YEAR)) + value;
            }
            // try the available time formats to timestamp
            for (int dt = 0; dt < fmt.dateFormat.size(); dt++) {
              try {
                Date parsedDate = fmt.dateFormat.get(dt).parse(value);
                /*
                 * Some code to try and add some smarts to the year insertion.
                 * Original code just added the current year which was okay-ish, but around
                 * January 1st becomes pretty naïve.
                 * The current year is added above. This code, if the year has been added does
                 * the following:
                 * 1. Compute what the computed time, but one month in the past would be.
                 * 2. Compute what the computed time, but eleven months in the future would be.
                 * If the computed time is more than one month in the future then roll it back a
                 * year. If the computed time is more than eleven months in the past then roll it
                 * forward a year. This gives us a 12 month rolling window (11 months in the past,
                 * 1 month in the future) of timestamps.
                 */
                if (fmt.addYear) {
                  Calendar cal = Calendar.getInstance();
                  cal.setTime(parsedDate);
                  Calendar calMinusOneMonth = Calendar.getInstance();
                  calMinusOneMonth.setTime(parsedDate);
                  calMinusOneMonth.add(Calendar.MONTH, -1);

                  Calendar calPlusElevenMonths = Calendar.getInstance();
                  calPlusElevenMonths.setTime(parsedDate);
                  calPlusElevenMonths.add(Calendar.MONTH, +11);

                  if (cal.getTimeInMillis() > System.currentTimeMillis() &&
                      calMinusOneMonth.getTimeInMillis() > System.currentTimeMillis()) {
                    //Need to roll back a year
                    Calendar c1 = Calendar.getInstance();
                    c1.setTime(parsedDate);
                    c1.add(Calendar.YEAR, -1);
                    parsedDate = c1.getTime();
                  } else if (cal.getTimeInMillis() < System.currentTimeMillis() &&
                             calPlusElevenMonths.getTimeInMillis() < System.currentTimeMillis()) {
                    //Need to roll forward a year
                    Calendar c1 = Calendar.getInstance();
                    c1.setTime(parsedDate);
                    c1.add(Calendar.YEAR, -1);
                    parsedDate = c1.getTime();
                  }
                }
                timeStamp = String.valueOf(parsedDate.getTime());
                break; // done. formatted the time
              } catch (ParseException e) {
                // Error formatting the timeStamp, try next format
                continue;
              }
            }
          }
        } else if (grp == SYSLOG_HOSTNAME_POS) {
          hostName = value;
        } else if (grp == SYSLOG_PRIORITY_POS) {
          priority = value;
        } else if (grp == SYSLOG_VERSION_POS) {
          version = value;
        } else if (grp == SYSLOG_BODY_POS) {
          msgBody = addFieldsToBody(keepFields, value, priority, version,
                                    timeStampString, hostName);
        }
      }
      break; // we successfully parsed the message using this pattern
    }
  }

  private void reset() {
    baos.reset();
    m = Mode.START;
    prio.delete(0, prio.length());
    isBadEvent = false;
    isIncompleteEvent = false;
    hostName = null;
    timeStamp = null;
    msgBody = null;
  }

  // extract relevant syslog data needed for building Flume event
  public Event extractEvent(ChannelBuffer in) {

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
              baos.write(b);
              m = Mode.PRIO;
            } else if (b == '\n') {
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
            baos.write(b);
            if (b == '>') {
              if (prio.length() == 0) {
                isBadEvent = true;
              }
              m = Mode.DATA;
            } else {
              char ch = (char) b;
              prio.append(ch);
              // Priority is max 3 digits per both RFC 3164 and 5424
              // With this check there is basically no danger of
              // boas.size() exceeding this.maxSize before getting to the
              // DATA state where this is actually checked
              if (!Character.isDigit(ch) || prio.length() > 3) {
                isBadEvent = true;
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
            if (baos.size() == this.maxSize && !doneReading) {
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

  public void setKeepFields(Set<String> keepFields) {
    this.keepFields = keepFields;
  }
}


