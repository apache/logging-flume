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
package com.cloudera.flume.handlers.hdfs;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.TestCase;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.avro.AvroJsonOutputFormat;
import com.cloudera.flume.handlers.text.SyslogEntryFormat;
import com.cloudera.flume.handlers.text.output.Log4jOutputFormat;
import com.cloudera.util.FileUtil;

/**
 * This just checks to see if I actually do have control of output format from
 * FlumeConfiguration (a configuration file).
 */
public class TestEscapedCustomOutputDfs extends TestCase {

  public void testAvroOutputFormat() throws IOException {
    // set the output format.
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.COLLECTOR_OUTPUT_FORMAT, "avrojson");

    // build a sink that outputs to that format.
    File f = FileUtil.mktempdir();
    SinkBuilder builder = EscapedCustomDfsSink.builder();
    EventSink snk =
        builder.build(new Context(), "file:///" + f.getPath()
            + "/sub-%{service}");
    Event e = new EventImpl("this is a test message".getBytes());
    Attributes.setString(e, "service", "foo");
    snk.open();
    snk.append(e);
    snk.close();

    ByteArrayOutputStream exWriter = new ByteArrayOutputStream();
    AvroJsonOutputFormat ajof = new AvroJsonOutputFormat();
    ajof.format(exWriter, e);
    exWriter.close();
    String expected = new String(exWriter.toByteArray());

    // check the output to make sure it is what we expected.
    File fo = new File(f.getPath() + "/sub-foo");

    FileReader fr = new FileReader(fo);
    BufferedReader br = new BufferedReader(fr);
    String read = br.readLine() + "\n";
    assertEquals(expected, read);
  }

  public void testSyslogOutputFormat() throws IOException {
    // set the output format.
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.COLLECTOR_OUTPUT_FORMAT, "syslog");

    // build a sink that outputs to that format.
    File f = FileUtil.mktempdir();
    SinkBuilder builder = EscapedCustomDfsSink.builder();
    EventSink snk =
        builder.build(new Context(), "file:///" + f.getPath()
            + "/sub-%{service}");
    Event e = new EventImpl("this is a test message".getBytes());
    Attributes.setString(e, "service", "foo");
    snk.open();
    snk.append(e);
    snk.close();

    ByteArrayOutputStream exWriter = new ByteArrayOutputStream();
    SyslogEntryFormat fmt = new SyslogEntryFormat();
    fmt.format(exWriter, e);
    exWriter.close();
    String expected = new String(exWriter.toByteArray());

    // check the output to make sure it is what we expected.
    File fo = new File(f.getPath() + "/sub-foo");

    FileReader fr = new FileReader(fo);
    BufferedReader br = new BufferedReader(fr);
    String read = br.readLine() + "\n";
    assertEquals(expected, read);
  }

  public void testLog4jOutputFormat() throws IOException {
    // set the output format.
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.COLLECTOR_OUTPUT_FORMAT, "log4j");

    // build a sink that outputs to that format.
    File f = FileUtil.mktempdir();
    SinkBuilder builder = EscapedCustomDfsSink.builder();
    EventSink snk =
        builder.build(new Context(), "file:///" + f.getPath()
            + "/sub-%{service}");
    Event e = new EventImpl("this is a test message".getBytes());
    Attributes.setString(e, "service", "foo");
    snk.open();
    snk.append(e);
    snk.close();

    ByteArrayOutputStream exWriter = new ByteArrayOutputStream();
    Log4jOutputFormat fmt = new Log4jOutputFormat();
    fmt.format(exWriter, e);
    exWriter.close();
    String expected = new String(exWriter.toByteArray());

    // check the output to make sure it is what we expected.
    File fo = new File(f.getPath() + "/sub-foo");

    FileReader fr = new FileReader(fo);
    BufferedReader br = new BufferedReader(fr);
    String read = br.readLine() + "\n";
    assertEquals(expected, read);
  }
}
