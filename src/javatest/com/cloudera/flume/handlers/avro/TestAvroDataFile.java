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

import java.io.File;
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

/**
 * This test takes a small canned batch of events, writes them to a avro data
 * file, and then reads them back checking the values.
 */
public class TestAvroDataFile {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestAvroDataFile.class);

  @Test
  public void testAvroDataFileWriteRead() throws IOException,
      FlumeSpecException {

    MemorySinkSource mem = MemorySinkSource.cannedData("test ", 5);

    // setup sink.
    File f = File.createTempFile("avrodata", ".avro");
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
}
