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

package org.apache.flume.source;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.flume.Event;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestSyslogParser {
    private static class Entry {
        private final String rfc5424;
        private final String rfc3164;
        private final long millis;
        private final LocalDateTime localDateTime;

        public Entry(String rfc5424, String rfc3164, long millis) {
            this.rfc5424 = rfc5424;
            this.rfc3164 = rfc3164;
            this.millis = millis;
            this.localDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(millis), ZoneOffset.UTC);
        }

        public String getRfc5424() {
            return rfc5424;
        }
        public String getRfc3164() {
            return rfc3164;
        }
        public LocalDateTime getLocalDateTime() {
            return localDateTime;
        }
        public long getMillis() {
            return millis;
        }
    }

    private static final Entry[] VALID_TIMESTAMPS = {
            new Entry("1985-04-12T23:20:50.52Z", "Apr 12 23:20:50", 482196050520L),
            new Entry("1985-04-12T19:20:50.52-04:00", "Apr 12 23:20:50", 482196050520L),
            new Entry("2003-10-11T22:14:15.003Z", "Oct 11 22:14:15", 1065910455003L),
            new Entry("2003-08-24T05:14:15.000003-07:00", "Aug 24 12:14:15", 1061727255000L),
            new Entry("2012-04-13T08:08:08.0001+00:00", "Apr 13 08:08:08", 1334304488000L),
            new Entry("2012-04-13T08:08:08.251+00:00", "Apr 13 08:08:08", 1334304488251L),
            // The same instant with 0, 3, 6 and 9 fractional-second digits:
            // anything finer than a millisecond is truncated.
            new Entry("2003-10-11T22:14:15Z", "Oct 11 22:14:15", 1065910455000L),
            new Entry("2003-10-11T22:14:15.123Z", "Oct 11 22:14:15", 1065910455123L),
            new Entry("2003-10-11T22:14:15.123456Z", "Oct 11 22:14:15", 1065910455123L),
            new Entry("2003-10-11T22:14:15.123456789Z", "Oct 11 22:14:15", 1065910455123L)
    };

    private static final String[] INVALID_RFC3164_TIMESTAMPS = {
            "",                 // empty
            "not a date",       // not a timestamp at all
            "Foo 12 23:20:50",  // invalid month name
            "Apr 12 25:61:61",  // hour/minute/second out of range
    };

    private static final String[] INVALID_RFC5424_TIMESTAMPS = {
            "",                       // empty
            "2003-10-11T22:14",       // too short to hold a full timestamp
            "2003-10-11T22:14:15.",   // fractional-second marker but no digits or TZ
            "2003-10-11T22:14:15+0",  // truncated timezone offset
            "xxxx-10-11T22:14:15Z",   // prefix is not a valid date-time
    };

  @Test
  public void testParseRfc3164Time() {
    SyslogParser parser = new SyslogParser();
    LocalDate now = LocalDate.now();

    for (Entry entry : VALID_TIMESTAMPS) {
      long time = parser.parseRfc3164Time(entry.getRfc3164());

      // RFC 3164 timestamps have no year, therefore we only assert the equality
      LocalDateTime expected = entry.getLocalDateTime();
      LocalDateTime actual = LocalDateTime.ofInstant(
          Instant.ofEpochMilli(time), ZoneOffset.UTC);

      String msg = entry.getRfc3164() + " was not parsed correctly (got " + actual + ")";
      Assert.assertEquals(msg, expected.getMonth(), actual.getMonth());
      Assert.assertEquals(msg, expected.getDayOfMonth(), actual.getDayOfMonth());
      Assert.assertEquals(msg, expected.getHour(), actual.getHour());
      Assert.assertEquals(msg, expected.getMinute(), actual.getMinute());
      Assert.assertEquals(msg, expected.getSecond(), actual.getSecond());
      Assert.assertTrue(msg, Math.abs(now.getYear() - actual.getYear()) <= 1);
    }
  }

  @Test
  public void testParseRfc3164TimeInvalid() {
    SyslogParser parser = new SyslogParser();

    // parseRfc3164Time() signals a parse failure by returning 0 rather than throwing.
    for (String input : INVALID_RFC3164_TIMESTAMPS) {
      Assert.assertEquals("Expected 0 for unparseable RFC 3164 timestamp '" + input + "'",
          0L, parser.parseRfc3164Time(input));
    }
  }

  @Test
  public void testParseRfc5424Time() {
    SyslogParser parser = new SyslogParser();

    for (Entry entry : VALID_TIMESTAMPS) {
        long time = parser.parseRfc5424Date(entry.getRfc5424());
        Assert.assertEquals(entry.millis + " is not the same as timestamp " + time, entry.getMillis(), time);
    }
  }

  @Test
  public void testParseRfc5424TimeInvalid() {
    SyslogParser parser = new SyslogParser();

    for (String input : INVALID_RFC5424_TIMESTAMPS) {
      Assert.assertThrows("Expected parsing to fail for RFC 5424 timestamp '" + input + "'",
          IllegalArgumentException.class, () -> parser.parseRfc5424Date(input));
    }
  }

  @Test
  public void testMessageParsing() {
    SyslogParser parser = new SyslogParser();
    Charset charset = Charsets.UTF_8;
    List<String> messages = Lists.newArrayList();

    // supported examples from RFC 3164
    messages.add("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for " +
        "lonvick on /dev/pts/8");
    messages.add("<13>Feb  5 17:32:18 10.0.0.99 Use the BFG!");
    messages.add("<165>Aug 24 05:34:00 CST 1987 mymachine myproc[10]: %% " +
        "It's time to make the do-nuts.  %%  Ingredients: Mix=OK, Jelly=OK # " +
         "Devices: Mixer=OK, Jelly_Injector=OK, Frier=OK # Transport: " +
         "Conveyer1=OK, Conveyer2=OK # %%");
    messages.add("<0>Oct 22 10:52:12 scapegoat 1990 Oct 22 10:52:01 TZ-6 " +
         "scapegoat.dmz.example.org 10.1.2.3 sched[0]: That's All Folks!");

    // supported examples from RFC 5424
    messages.add("<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - " +
        "ID47 - BOM'su root' failed for lonvick on /dev/pts/8");
    messages.add("<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc " +
        "8710 - - %% It's time to make the do-nuts.");

    // non-standard (but common) messages (RFC3339 dates, no version digit)
    messages.add("<13>2003-08-24T05:14:15Z localhost snarf?");
    messages.add("<13>2012-08-16T14:34:03-08:00 127.0.0.1 test shnap!");

    // test with default keepFields = false
    for (String msg : messages) {
      Set<String> keepFields = new HashSet<String>();
      Event event = parser.parseMessage(msg, charset, keepFields);
      Assert.assertNull("Failure to parse known-good syslog message",
                        event.getHeaders().get(SyslogUtils.EVENT_STATUS));
    }

    // test that priority, timestamp and hostname are preserved in event body
    for (String msg : messages) {
      Set<String> keepFields = new HashSet<String>();
      keepFields.add(SyslogUtils.KEEP_FIELDS_ALL);
      Event event = parser.parseMessage(msg, charset, keepFields);
      Assert.assertArrayEquals(event.getBody(), msg.getBytes());
      Assert.assertNull("Failure to parse known-good syslog message",
          event.getHeaders().get(SyslogUtils.EVENT_STATUS));
    }

    // test that hostname is preserved in event body
    for (String msg : messages) {
      Set<String> keepFields = new HashSet<String>();
      keepFields.add(SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS_HOSTNAME);
      Event event = parser.parseMessage(msg, charset, keepFields);
      Assert.assertTrue("Failure to persist hostname",
          new String(event.getBody()).contains(event.getHeaders().get("host")));
      Assert.assertNull("Failure to parse known-good syslog message",
          event.getHeaders().get(SyslogUtils.EVENT_STATUS));
    }
  }
}