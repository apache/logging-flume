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
package com.cloudera.flume.handlers.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.cloudera.util.FileUtil;

/**
 * This test takes a small canned batch of events, writes them to a avro data
 * file, and then reads them back checking the values.
 */
public class TestAvroDataFile {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestAvroDataFile.class);

  @Test
  public void testAvroDataFileWriteRead() throws IOException,
      FlumeSpecException, InterruptedException {

    MemorySinkSource mem = MemorySinkSource.cannedData("test ", 5);

    // setup sink.
    File f = FileUtil.createTempFile("avrodata", ".avro");
    f.deleteOnExit();
    LOG.info("filename before escaping: " + f.getAbsolutePath());
    String custom = "text(\""
        + StringEscapeUtils.escapeJava(f.getAbsolutePath())
        + "\", \"avrodata\")";
    LOG.info("sink to parse: " + custom);
    EventSink snk = FlumeBuilder.buildSink(new Context(), custom);
    snk.open();
    mem.open();
    EventUtil.dumpAll(mem, snk);
    snk.close();

    mem.open();
    DatumReader<EventImpl> dtm = new ReflectDatumReader<EventImpl>(
        EventImpl.class);
    DataFileReader<EventImpl> dr = new DataFileReader<EventImpl>(f, dtm);

    EventImpl eout = null;
    for (Object o : dr) {
      eout = (EventImpl) o; // TODO (jon) fix AVRO -- this is gross
      Event expected = mem.next();
      Assert.assertTrue(Arrays.equals(eout.getBody(), expected.getBody()));
    }
  }

  /**
   * This is a helper method that is a lot like the above, except that it
   * directly creates the output format so that we can configure it, since
   * this isn't possible via the configuration language currently.
   */
  public void avroDataFileWriteReadHelper(String... args) throws IOException,
      FlumeSpecException, InterruptedException {

    MemorySinkSource mem = MemorySinkSource.cannedData("test ", 5);

    // setup sink.
    File f = FileUtil.createTempFile("avrodata", ".avro");
    f.deleteOnExit();
    FileOutputStream fos = new FileOutputStream(f);
    LOG.info("filename before escaping: " + f.getAbsolutePath());
    OutputFormat out = FormatFactory.get().getOutputFormat("avrodata", args);
    mem.open();
    Event e = mem.next();
    while (e != null) {
      out.format(fos, e);
      e = mem.next();
    }

    mem.open();
    DatumReader<EventImpl> dtm = new ReflectDatumReader<EventImpl>(
        EventImpl.class);
    DataFileReader<EventImpl> dr = new DataFileReader<EventImpl>(f, dtm);

    EventImpl eout = null;
    for (Object o : dr) {
      eout = (EventImpl) o; // TODO (jon) fix AVRO -- this is gross
      Event expected = mem.next();
      Assert.assertTrue(Arrays.equals(eout.getBody(), expected.getBody()));
    }
  }

  @Test
  public void testAvroDataFileFormatDefault() throws IOException,
      FlumeSpecException, InterruptedException {
    avroDataFileWriteReadHelper();
  }

  @Test
  public void testAvroDataFileFormatNullCodec() throws IOException,
      FlumeSpecException, InterruptedException {
    avroDataFileWriteReadHelper("null");
  }

  @Test
  public void testAvroDataFileFormatDeflateCodec() throws IOException,
      FlumeSpecException, InterruptedException {
    avroDataFileWriteReadHelper("deflate");
  }

  @Test
  public void testAvroDataFileFormatDeflateConfiguredCodec() throws IOException,
      FlumeSpecException, InterruptedException {
    avroDataFileWriteReadHelper("deflate", "9");
  }

  @Test(expected=IllegalArgumentException.class)
  public void testAvroDataFileFormatInvalidCodec() throws IOException,
      FlumeSpecException, InterruptedException {
    avroDataFileWriteReadHelper("invalid");
  }
}
