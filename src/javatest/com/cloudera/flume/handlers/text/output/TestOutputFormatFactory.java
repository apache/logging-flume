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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.TestCase;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;

/**
 * This tests building of outputformats via the FormatFactory mechanism. These
 * output is just for visual inspection -- what we are trying to avoid here are
 * parser exceptions or NPEs from generating the
 */
public class TestOutputFormatFactory extends TestCase {

  final int count = 5;

  /**
   * Visual inspection
   */
  public void testSyslogConsole() throws FlumeSpecException, IOException {

    EventSink snk =
        FlumeBuilder.buildSink(new Context(), "console(\"syslog\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
  }

  /**
   * Visual inspection
   */
  public void testDefaultConsole() throws FlumeSpecException, IOException {

    EventSink snk =
        FlumeBuilder.buildSink(new Context(), "console(\"default\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
  }

  /**
   * Visual inspection
   */
  public void testLog4jConsole() throws FlumeSpecException, IOException {

    EventSink snk = FlumeBuilder.buildSink(new Context(), "console(\"log4j\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
  }

  boolean checkFile(File f) throws IOException {
    BufferedReader fr = new BufferedReader(new FileReader(f));
    @SuppressWarnings("unused")
    String l = null;
    int cnt = 0;
    while ((l = fr.readLine()) != null) {
      cnt++;
    }
    return cnt == count;
  }

  /**
   * Write out to file and check to make sure there are 5 lines.
   */
  public void testSyslogText() throws FlumeSpecException, IOException {

    File tmp = File.createTempFile("syslogText", ".txt");
    tmp.deleteOnExit();

    EventSink snk =
        FlumeBuilder.buildSink(new Context(), "text(\"" + tmp.getAbsolutePath()
            + "\",\"syslog\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
    assert (checkFile(tmp));
  }

  /**
   * Write out to file and check to make sure there are 5 lines.
   */
  public void testDefaultText() throws FlumeSpecException, IOException {

    File tmp = File.createTempFile("defaultText", ".txt");
    tmp.deleteOnExit();

    EventSink snk =
        FlumeBuilder.buildSink(new Context(), "text(\"" + tmp.getAbsolutePath()
            + "\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
    assert (checkFile(tmp));

  }

  /**
   * Write out to file and check to make sure there are 5 lines.
   */
  public void testLog4jText() throws FlumeSpecException, IOException {

    File tmp = File.createTempFile("log4jText", ".txt");
    tmp.deleteOnExit();

    EventSink snk =
        FlumeBuilder.buildSink(new Context(), "text(\"" + tmp.getAbsolutePath()
            + "\",\"log4j\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
    assert (checkFile(tmp));
  }

  /**
   * Write out to file and check to make sure there are 5 lines.
   */
  public void testSyslogDfs() throws FlumeSpecException, IOException {

    File tmp = File.createTempFile("syslogDfs", ".txt");
    tmp.deleteOnExit();

    EventSink snk =
        FlumeBuilder.buildSink(new Context(), "customdfs(\"file:///"
            + tmp.getAbsolutePath() + "\",\"syslog\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
    assert (checkFile(tmp));
  }

  /**
   * Write out to file and check to make sure there are 5 lines.
   */
  public void testDefaultDfs() throws FlumeSpecException, IOException {

    File tmp = File.createTempFile("defaultDfs", ".txt");
    tmp.deleteOnExit();

    EventSink snk =
        FlumeBuilder.buildSink(new Context(), "customdfs(\"file:///"
            + tmp.getAbsolutePath() + "\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
    assert (checkFile(tmp));

  }

  /**
   * Write out to file and check to make sure there are 5 lines.
   */
  public void testLog4jDfs() throws FlumeSpecException, IOException {

    File tmp = File.createTempFile("log4jDfs", ".txt");
    tmp.deleteOnExit();

    EventSink snk =
        FlumeBuilder.buildSink(new Context(), "customdfs(\"file:///"
            + tmp.getAbsolutePath() + "\",\"log4j\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
    assert (checkFile(tmp));
  }

}
