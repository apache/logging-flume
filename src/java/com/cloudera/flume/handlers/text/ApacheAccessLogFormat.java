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
package com.cloudera.flume.handlers.text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.Event.Priority;
import com.cloudera.util.Clock;
import com.cloudera.util.NetUtils;

/**
 * Apache Access Logs are configurable to have custom formats. The default
 * (which we parse here) is :
 * 
 * Default configured to be: LogFormat "%h %l %u %t \"%r\" %>s %b" common
 * 
 * which means:
 * 
 * %h ip of remote host
 * 
 * %l identd check (usually '-' because is usually unreliable data)
 * 
 * %u user according to HTTP Authentication. '-' if no pw
 * 
 * %t time request received in apache date format.
 * 
 * %r request string. The format has it in quotes.
 * 
 * %>s status code sent back to client
 * 
 * %b size of object sent to client not including header. '-' here means 0 data.
 * 
 * Others escape sequences are explained here
 * http://httpd.apache.org/docs/2.0/logs.html
 * 
 * Another common one is the CombinedLogFormat which adds two fields:
 * 
 * LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\""
 * combined
 * 
 * TODO (jon) Add support for parsing based on Apache LogFormat directive
 * http://httpd.apache.org/docs/1.3/mod/mod_log_config.html#formats
 * 
 * TODO (jon) These links talk about the cost incurred by the Calendar class.
 * Part of it is due to the fact that it is not thread safe. The extract method
 * is very likely to run in multiple threads at some point so this will become
 * an issue.
 * 
 * http://blog.bielu.com/2008/08/javautilcalendar-confusion-is-it-safe_28.html
 * 
 * The article links to the library below -- apparently to be standard as part
 * of java 7.
 * 
 * http://joda-time.sourceforge.net/
 * 
 * TODO (jon) implement apache format outputter.
 */
public class ApacheAccessLogFormat implements InputFormat {
  static Logger LOG = Logger.getLogger(ApacheAccessLogFormat.class.getName());

  final static Pattern APACHE_PAT = Pattern
      .compile("^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \\\"(.*?)\\\" (\\S+) (\\S+)( \\\"(.*?)\\\" \\\"(.*?)\\\")?$");
  final SimpleDateFormat APACHE_DF = new SimpleDateFormat(
      "dd/MMM/yyyy:HH:mm:ss zzzzz");

  /**
   * This attempts returns null if a line fails to be parsed.
   */
  public Event extract(String s) {
    Matcher m = APACHE_PAT.matcher(s);
    if (!m.matches())
      return null;

    try {
      String service = "apache";
      String date = m.group(4);
      Date d = APACHE_DF.parse(date);
      Calendar c = Calendar.getInstance();
      c.setTime(d);
      d = c.getTime();

      String host = NetUtils.localhost(); // local host

      // TODO(jon) body should be the raw entry, and a new field should be
      // created for this instead.
      String body = m.group(5);
      String client = m.group(1);
      String res = m.group(6);
      String size = m.group(7);

      String referrer = m.group(9);
      String browser = m.group(10);

      Map<String, byte[]> fields = new HashMap<String, byte[]>();
      fields.put("service", service.getBytes());
      fields.put("client", client.getBytes());
      fields.put("req_result", res.getBytes());
      fields.put("req_size", size.getBytes());

      if (referrer != null && !referrer.equals("-")) {
        fields.put("referrer", referrer.getBytes());
      }

      if (browser != null && !browser.equals("-")) {
        fields.put("browser", browser.getBytes());
      }

      Event e = new EventImpl(body.getBytes(), d.getTime(), Priority.INFO,
          Clock.nanos(), host, fields);
      return e;
    } catch (ParseException e) {
      LOG.warn("Failed to parse apache access log line: '" + s + "'", e);
      return null;
    }

  }
}
