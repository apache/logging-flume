/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.hdfs;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestUseRawLocalFileSystem {

  private static Logger logger =
      LoggerFactory.getLogger(TestUseRawLocalFileSystem.class);
  private Context context;

  private File baseDir;
  private File testFile;
  private Event event;

  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
    testFile = new File(baseDir.getAbsoluteFile(), "test");
    context = new Context();
    event = EventBuilder.withBody("test", Charsets.UTF_8);
  }

  @After
  public void teardown() throws Exception {
    FileUtils.deleteQuietly(baseDir);
  }

  @Test
  public void testTestFile() throws Exception {
    String file = testFile.getCanonicalPath();
    HDFSDataStream stream = new HDFSDataStream();
    context.put("hdfs.useRawLocalFileSystem", "true");
    stream.configure(context);
    stream.open(file);
    stream.append(event);
    stream.sync();
    Assert.assertTrue(testFile.length() > 0);
  }
  @Test
  public void testCompressedFile() throws Exception {
    String file = testFile.getCanonicalPath();
    HDFSCompressedDataStream stream = new HDFSCompressedDataStream();
    context.put("hdfs.useRawLocalFileSystem", "true");
    stream.configure(context);
    stream.open(file, new GzipCodec(), CompressionType.RECORD);
    stream.append(event);
    stream.sync();
    Assert.assertTrue(testFile.length() > 0);
  }
  @Test
  public void testSequenceFile() throws Exception {
    String file = testFile.getCanonicalPath();
    HDFSSequenceFile stream = new HDFSSequenceFile();
    context.put("hdfs.useRawLocalFileSystem", "true");
    stream.configure(context);
    stream.open(file);
    stream.append(event);
    stream.sync();
    Assert.assertTrue(testFile.length() > 0);
  }

}