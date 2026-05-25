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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestSyslogParser {

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static class Entry {
        private final String formattedDate;
        private final long millis;

        public Entry(String formattedDate, long millis) {
            this.formattedDate = formattedDate;
            this.millis = millis;
        }

        public String getFormattedDate() {
            return formattedDate;
        }
        public long getMillis() {
            return millis;
        }
    }

  @Test
  public void testRfc5424DateParsing() {
        final Entry[] examples = {
                new Entry("1985-04-12T23:20:50.52Z", 482196050520L),
                new Entry("1985-04-12T19:20:50.52-04:00", 482196050520L),
                new Entry("2003-10-11T22:14:15.003Z", 1065910455003L),
                new Entry("2003-08-24T05:14:15.000003-07:00", 1061727255000L),
                new Entry("2012-04-13T08:08:08.0001+00:00", 1334304488000L),
                new Entry("2012-04-13T08:08:08.251+00:00", 1334304488251L)
        };

    SyslogParser parser = new SyslogParser();
    //DateTimeFormatter timeParser = DateTimeFormatter

    for (Entry entry : examples) {
        long time = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(entry.getFormattedDate())).toEpochMilli();
        Assert.assertEquals(entry.millis + " is not the same as timestamp " + time, entry.getMillis(), time);
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