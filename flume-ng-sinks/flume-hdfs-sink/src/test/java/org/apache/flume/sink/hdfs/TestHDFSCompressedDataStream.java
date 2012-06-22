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

package org.apache.flume.sink.hdfs;

import com.google.common.base.Charsets;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHDFSCompressedDataStream {

  private static final Logger logger =
      LoggerFactory.getLogger(TestHDFSCompressedDataStream.class);

  // make sure the data makes it to disk if we sync() the data stream
  @Test
  public void testGzipDurability() throws IOException {
    File file = new File("target/test/data/foo.gz");
    String fileURI = file.getAbsoluteFile().toURI().toString();
    logger.info("File URI: {}", fileURI);

    Configuration conf = new Configuration();
    // local FS must be raw in order to be Syncable
    conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    Path path = new Path(fileURI);
    FileSystem fs = path.getFileSystem(conf); // get FS with our conf cached
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);

    HDFSCompressedDataStream writer = new HDFSCompressedDataStream();
    FlumeFormatter fmt = new HDFSTextFormatter();
    writer.open(fileURI, factory.getCodec(new Path(fileURI)),
        SequenceFile.CompressionType.BLOCK, fmt);
    String body = "yarf!";
    Event evt = EventBuilder.withBody(body, Charsets.UTF_8);
    writer.append(evt, fmt);
    writer.sync();

    byte[] buf = new byte[256];
    GZIPInputStream cmpIn = new GZIPInputStream(new FileInputStream(file));
    int len = cmpIn.read(buf);
    String result = new String(buf, 0, len, Charsets.UTF_8);
    result = result.trim(); // HDFSTextFormatter adds a newline

    Assert.assertEquals("input and output must match", body, result);
  }

}
