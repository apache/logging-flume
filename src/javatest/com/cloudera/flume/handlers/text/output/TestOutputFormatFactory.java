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

import static org.apache.commons.lang.StringEscapeUtils.escapeJava;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.handlers.text.FormatFactory.OutputFormatBuilder;

/**
 * This tests building of outputformats via the FormatFactory mechanism. These
 * output is just for visual inspection -- what we are trying to avoid here are
 * parser exceptions or NPEs from generating the
 */
public class TestOutputFormatFactory {

  private static final Logger LOG = Logger
      .getLogger(TestOutputFormatFactory.class);
  final int count = 5;

  /**
   * Visual inspection
   * 
   * @throws InterruptedException
   */
  @Test
  public void testSyslogConsole() throws FlumeSpecException, IOException,
      InterruptedException {

    EventSink snk = FlumeBuilder
        .buildSink(new Context(), "console(\"syslog\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
  }

  /**
   * Visual inspection
   * 
   * @throws InterruptedException
   */
  @Test
  public void testDefaultConsole() throws FlumeSpecException, IOException,
      InterruptedException {

    EventSink snk = FlumeBuilder.buildSink(new Context(),
        "console(\"default\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
  }

  /**
   * Visual inspection
   * 
   * @throws InterruptedException
   */
  @Test
  public void testLog4jConsole() throws FlumeSpecException, IOException,
      InterruptedException {

    EventSink snk = FlumeBuilder.buildSink(new Context(), "console(\"log4j\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
  }

  /**
   * Visual inspection
   *
   * @throws InterruptedException
   */
  @Test
  public void testJsonConsole() throws FlumeSpecException, IOException,
      InterruptedException {

    EventSink snk = FlumeBuilder.buildSink(new Context(), "console(\"json\")");
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
   * 
   * @throws InterruptedException
   */
  @Test
  public void testSyslogText() throws FlumeSpecException, IOException,
      InterruptedException {

    File tmp = File.createTempFile("syslogText", ".txt");
    tmp.deleteOnExit();

    EventSink snk = FlumeBuilder.buildSink(new Context(), "text(\""
        + escapeJava(tmp.getAbsolutePath()) + "\",\"syslog\")");
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
   * 
   * @throws InterruptedException
   */
  @Test
  public void testDefaultText() throws FlumeSpecException, IOException,
      InterruptedException {

    File tmp = File.createTempFile("defaultText", ".txt");
    tmp.deleteOnExit();

    EventSink snk = FlumeBuilder.buildSink(new Context(), "text(\""
        + escapeJava(tmp.getAbsolutePath()) + "\")");
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
   * 
   * @throws InterruptedException
   */
  @Test
  public void testLog4jText() throws FlumeSpecException, IOException,
      InterruptedException {

    File tmp = File.createTempFile("log4jText", ".txt");
    tmp.deleteOnExit();

    EventSink snk = FlumeBuilder.buildSink(new Context(), "text(\""
        + escapeJava(tmp.getAbsolutePath()) + "\",\"log4j\")");
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
   *
   * @throws InterruptedException
   */
  @Test
  public void testJsonText() throws FlumeSpecException, IOException,
      InterruptedException {

    File tmp = File.createTempFile("jsonText", ".txt");
    tmp.deleteOnExit();

    EventSink snk = FlumeBuilder.buildSink(new Context(), "text(\""
        + escapeJava(tmp.getAbsolutePath()) + "\",\"json\")");
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
   * 
   * @throws InterruptedException
   */
  @Test
  public void testSyslogDfs() throws FlumeSpecException, IOException,
      InterruptedException {

    File tmp = File.createTempFile("syslogDfs", ".txt");
    tmp.deleteOnExit();

    EventSink snk = FlumeBuilder.buildSink(new Context(),
        "customdfs(\"file:///" + escapeJava(tmp.getAbsolutePath())
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
   * 
   * @throws InterruptedException
   */
  @Test
  public void testDefaultDfs() throws FlumeSpecException, IOException,
      InterruptedException {

    File tmp = File.createTempFile("defaultDfs", ".txt");
    tmp.deleteOnExit();

    EventSink snk = FlumeBuilder.buildSink(new Context(),
        "customdfs(\"file:///" + escapeJava(tmp.getAbsolutePath()) + "\")");
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
   * 
   * @throws InterruptedException
   */
  @Test
  public void testLog4jDfs() throws FlumeSpecException, IOException,
      InterruptedException {

    File tmp = File.createTempFile("log4jDfs", ".txt");
    tmp.deleteOnExit();

    EventSink snk = FlumeBuilder.buildSink(new Context(),
        "customdfs(\"file:///" + escapeJava(tmp.getAbsolutePath())
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
   *
   * @throws InterruptedException
   */
  @Test
  public void testJsonDfs() throws FlumeSpecException, IOException,
      InterruptedException {

    File tmp = File.createTempFile("jsonDfs", ".txt");
    tmp.deleteOnExit();

    EventSink snk = FlumeBuilder.buildSink(new Context(),
        "customdfs(\"file:///" + escapeJava(tmp.getAbsolutePath())
            + "\",\"json\")");
    snk.open();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl(("simple test " + i).getBytes());
      snk.append(e);
    }
    snk.close();
    assert (checkFile(tmp));
  }

  /**
   * Write out to file and check to make sure there are 5 lines prefixed by
   * "WACKADOODLE:".
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testWackaDoodle() throws FlumeSpecException, IOException,
      InterruptedException {
    EventSink sink;
    File tmpFile;
    BufferedReader reader;
    int matchedLines;

    tmpFile = File.createTempFile("wackadoodleOutputFormatTest", ".tmp");
    tmpFile.deleteOnExit();

    FormatFactory.get().registerFormat(new OutputFormatBuilder() {

      @Override
      public String getName() {
        return "wackadoodle";
      }

      @Override
      public OutputFormat build(String... args) {
        OutputFormat format;

        format = new AbstractOutputFormat() {

          @Override
          public void format(OutputStream o, Event e) throws IOException {
            o.write(("WACKADOODLE: " + new String(e.getBody())).getBytes());
          }

        };

        format.setBuilder(this);

        return format;
      }

    });

    sink = null;

    try {
      sink = FlumeBuilder.buildSink(
          new Context(),
          "[ counter(\"count\"), text(\""
              + escapeJava(tmpFile.getAbsolutePath())
              + "\", \"wackadoodle\") ]");
    } catch (FlumeSpecException e) {
      LOG.error(
          "Caught an exception while building a test sink. Exception follows.",
          e);
      Assert
          .fail("Unable to create sink. Possible lack of output format plugin? Cause:"
              + e.getMessage());
    }

    Assert.assertNotNull(sink);

    sink.open();

    for (int i = 0; i < count; i++) {
      Event e;

      e = new EventImpl(("test line " + i + "\n").getBytes());

      sink.append(e);
    }

    sink.close();

    matchedLines = 0;
    reader = null;

    try {
      String line;

      reader = new BufferedReader(new FileReader(tmpFile));
      line = reader.readLine();

      while (line != null) {
        if (line.startsWith("WACKADOODLE:")) {
          matchedLines++;
        }

        line = reader.readLine();
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }

    Assert.assertEquals(count, matchedLines);
  }

}
