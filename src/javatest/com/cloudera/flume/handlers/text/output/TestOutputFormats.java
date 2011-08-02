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
package com.cloudera.flume.handlers.text.output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.TimeZone;

import junit.framework.TestCase;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.handlers.avro.AvroJsonOutputFormat;
import com.cloudera.flume.handlers.syslog.SyslogWireOutputFormat;
import com.cloudera.flume.handlers.text.SyslogEntryFormat;

/**
 * Testing expected output of basic events.
 * 
 * TODO (jon) These test check prefixes and suffixes. Can't actually test time
 * part (even though it is set) becuase of timezone locale issues. PST vs PDT
 * break in different times of the year, PST vs EST break if tested in other
 * timezones. Effort to force a timezone punted on.
 */
public class TestOutputFormats extends TestCase {
  Event e = new EventImpl("test".getBytes(), 0, Priority.INFO, 0, "hostname");
  TimeZone tz = TimeZone.getTimeZone("GMT");

  public void testOutput() throws IOException {
    OutputFormat format = new DebugOutputFormat();
    ByteArrayOutputStream sos = new ByteArrayOutputStream();
    format.format(sos, e);
    String s = new String(sos.toByteArray());
    System.out.print(s);
    assertTrue(s.startsWith("hostname ["));
    assertTrue(s.endsWith("] test\n"));
  }

  public void testLog4j() throws IOException {
    OutputFormat format = new Log4jOutputFormat();
    ByteArrayOutputStream sos = new ByteArrayOutputStream();
    format.format(sos, e);
    byte[] data = sos.toByteArray();
    String s = new String(data);
    System.out.print(s);
    assertTrue(s.endsWith("INFO log4j: test\n"));
  }

  public void testSyslogWire() throws IOException {
    OutputFormat format = new SyslogWireOutputFormat();
    ByteArrayOutputStream sos = new ByteArrayOutputStream();
    format.format(sos, e);
    String s = new String(sos.toByteArray());
    System.out.print(s);
    assertEquals(s, "<13>test\n");
  }

  public void testSyslogEntry() throws IOException {
    OutputFormat format = new SyslogEntryFormat();
    ByteArrayOutputStream sos = new ByteArrayOutputStream();
    format.format(sos, e);
    String s = new String(sos.toByteArray());
    System.out.print(s);
    assertTrue(s.endsWith("hostname test\n"));
  }

  public void testAvroJson() throws IOException {
    OutputFormat format = new AvroJsonOutputFormat();
    ByteArrayOutputStream sos = new ByteArrayOutputStream();
    format.format(sos, e);
    String s = new String(sos.toByteArray());
    System.out.print(s);
    // TODO (jon) not sure if this will pass on every machine the same (how does
    // avro order fields?)
    assertEquals(
        s,
        "{\"body\":\"test\",\"timestamp\":0,\"pri\":\"INFO\",\"nanos\":0,\"host\":\"hostname\",\"fields\":{}}\n");
  }
}
