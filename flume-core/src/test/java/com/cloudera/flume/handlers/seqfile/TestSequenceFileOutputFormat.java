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
package com.cloudera.flume.handlers.seqfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.cloudera.flume.handlers.hdfs.WriteableEventKey;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.cloudera.util.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test takes a small canned batch of events, writes them to a sequence
 * file, and then reads them back checking the values.
 */
public class TestSequenceFileOutputFormat {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestSequenceFileOutputFormat.class);

  /**
   * This is a helper method that is a lot like the above, except that it
   * directly creates the output format so that we can configure it, since
   * this isn't possible via the configuration language currently.
   */
  public void sequenceFileWriteReadHelper(String... args) throws IOException,
      FlumeSpecException, InterruptedException {

    MemorySinkSource mem = MemorySinkSource.cannedData("test ", 5);

    // setup sink.
    File f = FileUtil.createTempFile("sequencefile", ".seq");
    f.deleteOnExit();
    FileOutputStream fos = new FileOutputStream(f);
    LOG.info("filename before escaping: " + f.getAbsolutePath());
    OutputFormat out = FormatFactory.get().getOutputFormat("seqfile", args);
    mem.open();
    Event e = mem.next();
    while (e != null) {
      out.format(fos, e);
      e = mem.next();
    }

    mem.open();
    
    FlumeConfiguration conf = FlumeConfiguration.get();
    FileSystem fs = FileSystem.getLocal(conf);
    SequenceFile.Reader r = new SequenceFile.Reader(fs, new Path(f.toURI()), conf);
    WriteableEventKey k = new WriteableEventKey();
    WriteableEvent evt = new WriteableEvent();
    while (r.next(k, evt)) {
      Event expected = mem.next();
      assertEquals(evt.getTimestamp(), expected.getTimestamp());
      assertEquals(evt.getNanos(), expected.getNanos());
      assertEquals(evt.getPriority(), expected.getPriority());
      assertTrue(Arrays.equals(evt.getBody(), expected.getBody()));
    }

  }

  @Test
  public void testSequenceFileFormatDefault() throws IOException,
      FlumeSpecException, InterruptedException {
      sequenceFileWriteReadHelper();
  }

  @Test
  public void testSequenceFileFormatDeflateCodec() throws IOException,
      FlumeSpecException, InterruptedException {
      sequenceFileWriteReadHelper("BZip2Codec");
  }

  @Test(expected=IllegalArgumentException.class)
  public void testSequenceFileFormatInvalidCodec() throws IOException,
      FlumeSpecException, InterruptedException {
      sequenceFileWriteReadHelper("invalid");
  }
}
