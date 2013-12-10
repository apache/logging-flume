/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
/**
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.apache.flume.source;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Maps;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SyslogParser {

  private static final Logger logger =
      LoggerFactory.getLogger(SyslogParser.class);

  private static final int TS_CACHE_MAX = 1000;  // timestamp cache size limit
  private static final Pattern TWO_SPACES = Pattern.compile("  ");
  private static final DateTimeFormatter rfc3164Format =
      DateTimeFormat.forPattern("MMM d HH:mm:ss").withZoneUTC();

  private static final String timePat = "yyyy-MM-dd'T'HH:mm:ss";
  private static final int RFC3164_LEN = 15;
  private static final int RFC5424_PREFIX_LEN = 19;
  private final DateTimeFormatter timeParser;

  private Cache<String, Long> timestampCache;

  public SyslogParser() {
    timeParser = DateTimeFormat.forPattern(timePat).withZoneUTC();
    timestampCache = CacheBuilder.newBuilder().maximumSize(TS_CACHE_MAX).build(
        new CacheLoader<String, Long>() {

          @Override
          public Long load(String key) throws Exception {
            return timeParser.parseMillis(key);
          }
        });
  }

  /**
   * Parses a Flume Event out of a syslog message string.
   * @param msg Syslog message, not including the newline character
   * @return Parsed Flume Event
   * @throws IllegalArgumentException if unable to successfully parse message
   */
  public Event parseMessage(String msg, Charset charset, boolean keepFields) {
    Map<String, String> headers = Maps.newHashMap();

    int msgLen = msg.length();

    int curPos = 0;

    Preconditions.checkArgument(msg.charAt(curPos) == '<',
        "Bad format: invalid priority: cannot find open bracket '<' (%s)", msg);

    int endBracketPos = msg.indexOf('>');
    Preconditions.checkArgument(endBracketPos > 0 && endBracketPos <= 6,
        "Bad format: invalid priority: cannot find end bracket '>' (%s)", msg);

    String priority = msg.substring(1, endBracketPos);
    int pri = Integer.parseInt(priority);
    int facility = pri / 8;
    int severity = pri % 8;

    // put fac / sev into header
    headers.put(SyslogUtils.SYSLOG_FACILITY, String.valueOf(facility));
    headers.put(SyslogUtils.SYSLOG_SEVERITY, String.valueOf(severity));

    Preconditions.checkArgument(msgLen > endBracketPos + 1,
        "Bad format: no data except priority (%s)", msg);

    // update parsing position
    curPos = endBracketPos + 1;

    // ignore version string
    if (msgLen > curPos + 2 && "1 ".equals(msg.substring(curPos, curPos + 2))) {
      curPos += 2;
    }

    // now parse timestamp (handle different varieties)

    long ts;
    char dateStartChar = msg.charAt(curPos);

    try {

      // no timestamp specified; use relay current time
      if (dateStartChar == '-') {
        ts = System.currentTimeMillis();
        if (msgLen <= curPos + 2) {
          throw new IllegalArgumentException(
              "bad syslog format (missing hostname)");
        }
        curPos += 2; // assume we skip past a space to get to the hostname

        // rfc3164 imestamp
      } else if (dateStartChar >= 'A' && dateStartChar <= 'Z') {
        if (msgLen <= curPos + RFC3164_LEN) {
          throw new IllegalArgumentException("bad timestamp format");
        }
        ts = parseRfc3164Time(
            msg.substring(curPos, curPos + RFC3164_LEN));
        curPos += RFC3164_LEN + 1;

        // rfc 5424 timestamp
      } else {
        int nextSpace = msg.indexOf(' ', curPos);
        if (nextSpace == -1) {
          throw new IllegalArgumentException("bad timestamp format");
        }
        ts = parseRfc5424Date(msg.substring(curPos, nextSpace));
        curPos = nextSpace + 1;
      }

    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("Unable to parse message: " + msg, ex);
    }

    headers.put("timestamp", String.valueOf(ts));

    // parse out hostname
    int nextSpace = msg.indexOf(' ', curPos);
    if (nextSpace == -1) {
      throw new IllegalArgumentException(
          "bad syslog format (missing hostname)");
    }
    // copy the host string to avoid holding the message string in memory
    // if using a memory-based queue
    String hostname = new String(msg.substring(curPos, nextSpace));
    headers.put("host", hostname);

    // EventBuilder will do a copy of its own, so no defensive copy of the body
    String data = "";
    if (msgLen > nextSpace + 1 && !keepFields) {
      curPos = nextSpace + 1;
      data = msg.substring(curPos);
    } else {
      data = msg;
    }

    Event event = EventBuilder.withBody(data, charset, headers);

    return event;
  }

  /**
   * Parse date in RFC 5424 format. Uses an LRU cache to speed up parsing for
   * multiple messages that occur in the same second.
   * @param msg
   * @return Typical (for Java) milliseconds since UNIX epoch
   */
  protected long parseRfc5424Date(String msg) {

    Long ts = null;
    int curPos = 0;

    int msgLen = msg.length();
    Preconditions.checkArgument(msgLen > RFC5424_PREFIX_LEN,
        "Bad format: Not a valid RFC5424 timestamp: %s", msg);
    String timestampPrefix = msg.substring(curPos, RFC5424_PREFIX_LEN);

    try {
      ts = timestampCache.get(timestampPrefix);
    } catch (ExecutionException ex) {
      throw new IllegalArgumentException("bad timestamp format", ex);
    }

    curPos += RFC5424_PREFIX_LEN;

    Preconditions.checkArgument(ts != null, "Parsing error: timestamp is null");

    // look for the optional fractional seconds
    if (msg.charAt(curPos) == '.') {
      // figure out how many numeric digits
      boolean foundEnd = false;
      int endMillisPos = curPos + 1;

      if (msgLen <= endMillisPos) {
        throw new IllegalArgumentException("bad timestamp format (no TZ)");
      }

      // FIXME: TODO: ensure we handle all bad formatting cases
      while (!foundEnd) {
        char curDigit = msg.charAt(endMillisPos);
        if (curDigit >= '0' && curDigit <= '9') {
          endMillisPos++;
        } else {
          foundEnd = true;
        }
      }

      // if they had a valid fractional second, append it rounded to millis
      if (endMillisPos - (curPos + 1) > 0) {
        float frac = Float.parseFloat(msg.substring(curPos, endMillisPos));
        long milliseconds = (long) (frac * 1000f);
        ts += milliseconds;
      } else {
        throw new IllegalArgumentException(
            "Bad format: Invalid timestamp (fractional portion): " + msg);
      }

      curPos = endMillisPos;
    }

    // look for timezone
    char tzFirst = msg.charAt(curPos);

    // UTC
    if (tzFirst == 'Z') {
      // no-op
    } else if (tzFirst == '+' || tzFirst == '-') {

      Preconditions.checkArgument(msgLen > curPos + 5,
          "Bad format: Invalid timezone (%s)", msg);

      int polarity;
      if (tzFirst == '+') {
        polarity = +1;
      } else {
        polarity = -1;
      }

      char[] h = new char[5];
      for (int i = 0; i < 5; i++) {
        h[i] = msg.charAt(curPos + 1 + i);
      }

      if (h[0] >= '0' && h[0] <= '9'
          && h[1] >= '0' && h[1] <= '9'
          && h[2] == ':'
          && h[3] >= '0' && h[3] <= '9'
          && h[4] >= '0' && h[4] <= '9') {
        int hourOffset = Integer.parseInt(msg.substring(curPos + 1, curPos + 3));
        int minOffset = Integer.parseInt(msg.substring(curPos + 4, curPos + 6));
        ts -= polarity * ((hourOffset * 60) + minOffset) * 60000;
      } else {
        throw new IllegalArgumentException(
            "Bad format: Invalid timezone: " + msg);
      }

    }


    return ts;
  }

  /**
   * Parse the RFC3164 date format. This is trickier than it sounds because this
   * format does not specify a year so we get weird edge cases at year
   * boundaries. This implementation tries to "do what I mean".
   * @param ts RFC3164-compatible timestamp to be parsed
   * @return Typical (for Java) milliseconds since the UNIX epoch
   */
  protected long parseRfc3164Time(String ts) {
    DateTime now = DateTime.now();
    int year = now.getYear();

    ts = TWO_SPACES.matcher(ts).replaceFirst(" ");

    DateTime date;
    try {
      date = rfc3164Format.parseDateTime(ts);
    } catch (IllegalArgumentException e) {
      logger.debug("rfc3164 date parse failed on ("+ts+"): invalid format", e);
      return 0;
    }

    // try to deal with boundary cases, i.e. new year's eve.
    // rfc3164 dates are really dumb.
    // NB: cannot handle replaying of old logs or going back to the future
    if (date != null) {
      DateTime fixed = date.withYear(year);

      // flume clock is ahead or there is some latency, and the year rolled
      if (fixed.isAfter(now) && fixed.minusMonths(1).isAfter(now)) {
        fixed = date.withYear(year - 1);
      // flume clock is behind and the year rolled
      } else if (fixed.isBefore(now) && fixed.plusMonths(1).isBefore(now)) {
        fixed = date.withYear(year + 1);
      }
      date = fixed;
    }

    if (date == null) {
      return 0;
    }

    return date.getMillis();
  }


}
