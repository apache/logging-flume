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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.avro.AvroJsonOutputFormat;
import com.cloudera.flume.handlers.text.SyslogEntryFormat;
import com.cloudera.flume.handlers.text.output.Log4jOutputFormat;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.cloudera.util.FileUtil;

/**
 * This just checks to see if I actually do have control of output format from
 * FlumeConfiguration (a configuration file).
 */
public class TestEscapedCustomOutputDfs {

  final public static Logger LOG = Logger
      .getLogger(TestEscapedCustomOutputDfs.class);

  void checkOutputFormat(String format, OutputFormat of) throws IOException,
      InterruptedException {
    checkOutputFormat(format, of, "None", null);
  }

  void checkOutputFormat(String format, OutputFormat of, String codecName,
      CompressionCodec codec) throws IOException, InterruptedException {
    // set the output format.
    FlumeConfiguration conf = FlumeConfiguration.get();
    conf.set(FlumeConfiguration.COLLECTOR_OUTPUT_FORMAT, format);
    conf.set(FlumeConfiguration.COLLECTOR_DFS_COMPRESS_CODEC, codecName);

    // build a sink that outputs to that format.
    File f = FileUtil.mktempdir();
    SinkBuilder builder = EscapedCustomDfsSink.builder();
    EventSink snk = builder.create(new Context(), "file:///" + f.getPath()
        + "/sub-%{service}");
    Event e = new EventImpl("this is a test message".getBytes());
    Attributes.setString(e, "service", "foo");
    snk.open();
    snk.append(e);
    snk.close();

    ByteArrayOutputStream exWriter = new ByteArrayOutputStream();
    of.format(exWriter, e);
    exWriter.close();
    String expected = new String(exWriter.toByteArray());

    // check the output to make sure it is what we expected.

    // handle compression codec / extensions when checking.
    String ext = ""; // file extension
    if (codec != null) {
      ext = codec.getDefaultExtension();
    }
    InputStream in = new FileInputStream(f.getPath() + "/sub-foo" + ext);
    if (codec != null) {
      in = codec.createInputStream(in);
    }
    byte[] buf = new byte[1];
    StringBuilder output = new StringBuilder();
    // read the file
    while ((in.read(buf)) > 0) {
      output.append(new String(buf));
    }
    in.close(); // Must close for windows to delete
    assertEquals(expected, output.toString());

    // This doesn't get deleted in windows but the core test succeeds
    assertTrue("temp folder successfully deleted", FileUtil.rmr(f));
  }

  @Test
  public void testAvroOutputFormat() throws IOException, InterruptedException {
    checkOutputFormat("avrojson", new AvroJsonOutputFormat());
  }

  @Test
  public void testSyslogOutputFormat() throws IOException, InterruptedException {
    checkOutputFormat("syslog", new SyslogEntryFormat());
  }

  @Test
  public void testLog4jOutputFormat() throws IOException, InterruptedException {
    checkOutputFormat("log4j", new Log4jOutputFormat());
  }

  /**
   * Test to write few log lines, compress using gzip, write to disk, read back
   * the compressed file and verify the written lines. This test alone doesn't
   * test GZipCodec with its Native Libs. java.library.path must contain the
   * path to the hadoop native libs for this to happen.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testGZipCodec() throws IOException, InterruptedException {
    GzipCodec codec = new GzipCodec();
    codec.setConf(FlumeConfiguration.get());
    checkOutputFormat("syslog", new SyslogEntryFormat(), "GzipCodec",
        codec);
  }

  /**
   * Test to write few log lines, compress using bzip2, write to disk, read back
   * the compressed file and verify the written lines.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testBZip2Codec() throws IOException, InterruptedException {
    checkOutputFormat("syslog", new SyslogEntryFormat(), "BZip2Codec",
        new BZip2Codec());
  }

  /**
   * Test to write few log lines, compress using bzip2, write to disk, read back
   * the compressed file and verify the written lines.
   *
   * This test uses the wrong case for the codec name.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testBZip2CodecWrongCase() throws IOException,
      InterruptedException {
    checkOutputFormat("syslog", new SyslogEntryFormat(), "bzip2Codec",
        new BZip2Codec());
  }

  /**
   * Test to write few log lines, compress using default, write to disk, read
   * back the compressed file and verify the written lines.
   *
   * @throws InterruptedException
   */
  @Test
  public void testDefaultCodec() throws IOException, InterruptedException {
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(FlumeConfiguration.get()); // default needs conf
    checkOutputFormat("syslog", new SyslogEntryFormat(), "DefaultCodec", codec);
  }

  public void testCodecs() {
    LOG.info(CompressionCodecFactory.getCodecClasses(FlumeConfiguration.get()));
  }

  @Test
  public void testOutputFormats() throws FlumeSpecException {
    // format
    String src = "escapedFormatDfs(\"file:///tmp/test/testfilename\",\"\", avro)";
    FlumeBuilder.buildSink(new Context(), src);

    // format
    src = "escapedFormatDfs(\"file:///tmp/test/testfilename\",\"\", seqfile)";
    FlumeBuilder.buildSink(new Context(), src);

    // format
    src = "escapedFormatDfs(\"file:///tmp/test/testfilename\",\"\", seqfile(\"bzip2\"))";
    FlumeBuilder.buildSink(new Context(), src);
  }

  /**
   * Some output formats cache an output stream and each hdfs file thus needs to
   * make sure it has its own copy of the outputStream.
   * 
   * @throws IOException
   * @throws FlumeSpecException
   * @throws InterruptedException
   */
  @Test
  public void testNoOutputFormatSharingProblem() throws IOException,
      FlumeSpecException, InterruptedException {
    File f = FileUtil.mktempdir("newFileOutputFormatPer");
    String snk = "escapedFormatDfs(\"file://" + f.getAbsoluteFile()
        + "\", \"%{nanos}\", seqfile)";

    Event e1 = new EventImpl("e1".getBytes());
    Event e2 = new EventImpl("e2".getBytes());

    EventSink evtSnk = FlumeBuilder.buildSink(new Context(), snk);

    try {
      evtSnk.open();
      evtSnk.append(e1);
      evtSnk.append(e2);
      evtSnk.close();
    } finally {
      FileUtil.rmr(f);
    }
  }
}
