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
package org.apache.flume.serialization;

import com.google.common.base.Charsets;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.SyslogAvroEventSerializer.SyslogEvent;
import org.apache.flume.source.SyslogUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class exists to give an idea of how to use the AvroEventWriter
 * and is not intended for inclusion in the Flume core.<br/>
 * Problems with it are:<br/>
 * (1) assumes very little parsing is done at the first hop (more TBD)<br/>
 * (2) no field has been defined for use as a UUID for deduping<br/>
 * (3) tailored to syslog messages but not specific to any application<br/>
 * (4) not efficient about data copying from an implementation perspective<br/>
 * Often, it makes more sense to parse your (meta-)data out of the message part
 * itself and then store that in an application-specific Avro schema.
 */
public class SyslogAvroEventSerializer
    extends AbstractAvroEventSerializer<SyslogEvent> {

  private static final DateTimeFormatter dateFmt1 =
      DateTimeFormat.forPattern("MMM dd HH:mm:ss").withZoneUTC();
  private static final DateTimeFormatter dateFmt2 =
      DateTimeFormat.forPattern("MMM  d HH:mm:ss").withZoneUTC();

  private static final Logger logger =
      LoggerFactory.getLogger(SyslogAvroEventSerializer.class);

  // It's usually better to embed this schema in the class as a string.
  // Avro does this for you if you generate Java classes from a schema file.
  // But since this is a test class, having the schema in an .avsc file is more
  // readable. Should probably just use the maven avro plugin to generate
  // the inner SyslogEvent class from this file.
  private static final File schemaFile =
      new File("src/test/resources/syslog_event.avsc");

  private final OutputStream out;
  private final Schema schema;

  public SyslogAvroEventSerializer(OutputStream out) throws IOException {
    this.out = out;
    this.schema = new Schema.Parser().parse(schemaFile);
  }

  @Override
  protected OutputStream getOutputStream() {
    return out;
  }

  @Override
  protected Schema getSchema() {
    return schema;
  }

  // very simple rfc3164 parser
  @Override
  protected SyslogEvent convert(Event event) {
    SyslogEvent sle = new SyslogEvent();

    // Stringify body so it's easy to parse.
    // This is a pretty inefficient way to do it.
    String msg = new String(event.getBody(), Charsets.UTF_8);

    // parser read pointer
    int seek = 0;

    // Check Flume headers to see if we came from SyslogTcp(or UDP) Source,
    // which at the time of this writing only parses the priority.
    // This is a bit schizophrenic and it should parse all the fields or none.
    Map<String, String> headers = event.getHeaders();
    boolean fromSyslogSource = false;
    if (headers.containsKey(SyslogUtils.SYSLOG_FACILITY)) {
      fromSyslogSource = true;
      int facility = Integer.parseInt(headers.get(SyslogUtils.SYSLOG_FACILITY));
      sle.setFacility(facility);
    }
    if (headers.containsKey(SyslogUtils.SYSLOG_SEVERITY)) {
      fromSyslogSource = true;
      int severity = Integer.parseInt(headers.get(SyslogUtils.SYSLOG_SEVERITY));
      sle.setSeverity(severity);
    }

    // assume the message was received raw (maybe via NetcatSource)
    // parse the priority string
    if (!fromSyslogSource) {
      if (msg.charAt(0) == '<') {
        int end = msg.indexOf(">");
        if (end > -1) {
          seek = end + 1;
          String priStr = msg.substring(1, end);
          int priority = Integer.parseInt(priStr);
          int severity = priority % 8;
          int facility = (priority - severity) / 8;
          sle.setFacility(facility);
          sle.setSeverity(severity);
        }
      }
    }

    // parse the timestamp
    String timestampStr = msg.substring(seek, seek + 15);
    long ts = parseRfc3164Date(timestampStr);
    if (ts != 0) {
      sle.setTimestamp(ts);
      seek += 15 + 1; // space after timestamp
    }

    // parse the hostname
    int nextSpace = msg.indexOf(' ', seek);
    if (nextSpace > -1) {
      String hostname = msg.substring(seek, nextSpace);
      sle.setHostname(hostname);
      seek = nextSpace + 1;
    }

    // everything else is the message
    String actualMessage = msg.substring(seek);
    sle.setMessage(actualMessage);

    logger.debug("Serialized event as: {}", sle);

    return sle;
  }

  /**
   * Returns epoch time in millis, or 0 if the string cannot be parsed.
   * We use two date formats because the date spec in rfc3164 is kind of weird.
   * <br/>
   * <b>Warning:</b> logic is used here to determine the year even though it's
   * not part of the timestamp format, and we assume that the machine running
   * Flume has a clock that is at least close to the same day as the machine
   * that generated the event. We also assume that the event was generated
   * recently.
   */
  private static long parseRfc3164Date(String in) {
    DateTime date = null;
    try {
      date = dateFmt1.parseDateTime(in);
    } catch (IllegalArgumentException e) {
      // ignore the exception, we act based on nullity of date object
      logger.debug("Date parse failed on ({}), trying single-digit date", in);
    }

    if (date == null) {
      try {
        date = dateFmt2.parseDateTime(in);
      } catch (IllegalArgumentException e) {
        // ignore the exception, we act based on nullity of date object
        logger.debug("2nd date parse failed on ({}), unknown date format", in);
      }
    }

    // hacky stuff to try and deal with boundary cases, i.e. new year's eve.
    // rfc3164 dates are really dumb.
    // NB: cannot handle replaying of old logs or going back to the future
    if (date != null) {
      DateTime now = new DateTime();
      int year = now.getYear();
      DateTime corrected = date.withYear(year);

      // flume clock is ahead or there is some latency, and the year rolled
      if (corrected.isAfter(now) && corrected.minusMonths(1).isAfter(now)) {
        corrected = date.withYear(year - 1);
      // flume clock is behind and the year rolled
      } else if (corrected.isBefore(now) && corrected.plusMonths(1).isBefore(now)) {
        corrected = date.withYear(year + 1);
      }
      date = corrected;
    }

    if (date == null) {
      return 0;
    }

    return date.getMillis();
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      SyslogAvroEventSerializer writer = null;
      try {
        writer = new SyslogAvroEventSerializer(out);
        writer.configure(context);
      } catch (IOException e) {
        logger.error("Unable to parse schema file. Exception follows.", e);
      }
      return writer;
    }

  }

  // This class would ideally be generated from the avro schema file,
  // but we are letting reflection do the work instead.
  // There's no great reason not to let Avro generate it.
  public static class SyslogEvent {
    private int facility;
    private int severity;
    private long timestamp;
    private String hostname = "";
    private String message = "";

    public void setFacility(int f) { facility = f; }
    public int getFacility() { return facility; }

    public void setSeverity(int s) { severity = s; }
    public int getSeverity() { return severity; }

    public void setTimestamp(long t) { timestamp = t; }
    public long getTimestamp() { return timestamp; }

    public void setHostname(String h) { hostname = h; }
    public String getHostname() { return hostname; }

    public void setMessage(String m) { message = m; }
    public String getMessage() { return message; }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("{ Facility: ").append(facility).append(", ");
      builder.append(" Severity: ").append(severity).append(", ");
      builder.append(" Timestamp: ").append(timestamp).append(", ");
      builder.append(" Hostname: ").append(hostname).append(", ");
      builder.append(" Message: \"").append(message).append("\" }");
      return builder.toString();
    }
  }
}
