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

package org.apache.flume.client.avro;

import static org.apache.flume.client.avro.TestSpoolingFileLineReader.bodyAsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.flume.client.avro.TestSpoolingFileLineReader
    .bodiesAsStrings;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestBufferedLineReader {
  private File tmpDir;

  @Before
  public void before() {
    tmpDir = Files.createTempDir();
  }

  @After
  public void after() {
    for (File f : tmpDir.listFiles()) {
      f.delete();
    }
    tmpDir.delete();
  }

  @Test
  public void testSimpleRead() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    SimpleTextLineEventReader reader = new SimpleTextLineEventReader(new FileReader(f1));

    assertEquals("file1line1", bodyAsString(reader.readEvent()));
    assertEquals("file1line2", bodyAsString(reader.readEvent()));
    assertEquals("file1line3", bodyAsString(reader.readEvent()));
    assertEquals("file1line4", bodyAsString(reader.readEvent()));
    assertEquals("file1line5", bodyAsString(reader.readEvent()));
    assertEquals("file1line6", bodyAsString(reader.readEvent()));
    assertEquals("file1line7", bodyAsString(reader.readEvent()));
    assertEquals("file1line8", bodyAsString(reader.readEvent()));
    assertEquals(null, reader.readEvent());
  }

  @Test
  public void testBatchedReadsWithinAFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    SimpleTextLineEventReader reader = new SimpleTextLineEventReader(new FileReader(f1));

    List<String> out = bodiesAsStrings(reader.readEvents(5));

    // Make sure we got every line
    assertEquals(5, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file1line3"));
    assertTrue(out.contains("file1line4"));
    assertTrue(out.contains("file1line5"));
  }

  @Test
  public void testBatchedReadsAtFileBoundary() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    SimpleTextLineEventReader reader = new SimpleTextLineEventReader(new FileReader(f1));

    List<String> out = bodiesAsStrings(reader.readEvents(10));

    // Make sure we got exactly 8 lines
    assertEquals(8, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file1line3"));
    assertTrue(out.contains("file1line4"));
    assertTrue(out.contains("file1line5"));
    assertTrue(out.contains("file1line6"));
    assertTrue(out.contains("file1line7"));
    assertTrue(out.contains("file1line8"));
  }


}
