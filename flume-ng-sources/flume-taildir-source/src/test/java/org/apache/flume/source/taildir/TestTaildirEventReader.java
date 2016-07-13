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

package org.apache.flume.source.taildir;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.Files;
import org.apache.flume.Event;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants
                  .BYTE_OFFSET_HEADER_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestTaildirEventReader {
  private File tmpDir;
  private String posFilePath;

  public static String bodyAsString(Event event) {
    return new String(event.getBody());
  }

  static List<String> bodiesAsStrings(List<Event> events) {
    List<String> bodies = Lists.newArrayListWithCapacity(events.size());
    for (Event event : events) {
      bodies.add(new String(event.getBody()));
    }
    return bodies;
  }

  static List<String> headersAsStrings(List<Event> events, String headerKey) {
    List<String> headers = Lists.newArrayListWithCapacity(events.size());
    for (Event event : events) {
      headers.add(new String(event.getHeaders().get(headerKey)));
    }
    return headers;
  }

  private ReliableTaildirEventReader getReader(Map<String, String> filePaths,
      Table<String, String, String> headerTable, boolean addByteOffset) {
    ReliableTaildirEventReader reader;
    try {
      reader = new ReliableTaildirEventReader.Builder()
          .filePaths(filePaths)
          .headerTable(headerTable)
          .positionFilePath(posFilePath)
          .skipToEnd(false)
          .addByteOffset(addByteOffset)
          .build();
      reader.updateTailFiles();
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
    return reader;
  }

  private ReliableTaildirEventReader getReader(boolean addByteOffset) {
    Map<String, String> filePaths = ImmutableMap.of("testFiles",
                                                    tmpDir.getAbsolutePath() + "/file.*");
    Table<String, String, String> headerTable = HashBasedTable.create();
    return getReader(filePaths, headerTable, addByteOffset);
  }

  private ReliableTaildirEventReader getReader() {
    return getReader(false);
  }

  @Before
  public void setUp() {
    tmpDir = Files.createTempDir();
    posFilePath = tmpDir.getAbsolutePath() + "/taildir_position_test.json";
  }

  @After
  public void tearDown() {
    for (File f : tmpDir.listFiles()) {
      if (f.isDirectory()) {
        for (File sdf : f.listFiles()) {
          sdf.delete();
        }
      }
      f.delete();
    }
    tmpDir.delete();
  }

  @Test
  // Create three multi-line files then read them back out. Ensures that
  // lines and appended ones are read correctly from files.
  public void testBasicReadFiles() throws IOException {
    File f1 = new File(tmpDir, "file1");
    File f2 = new File(tmpDir, "file2");
    File f3 = new File(tmpDir, "file3");
    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);
    Files.write("file3line1\nfile3line2\n", f3, Charsets.UTF_8);

    ReliableTaildirEventReader reader = getReader();
    List<String> out = Lists.newArrayList();
    for (TailFile tf : reader.getTailFiles().values()) {
      List<String> bodies = bodiesAsStrings(reader.readEvents(tf, 2));
      out.addAll(bodies);
      reader.commit();
    }
    assertEquals(6, out.size());
    // Make sure we got every line
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file2line1"));
    assertTrue(out.contains("file2line2"));
    assertTrue(out.contains("file3line1"));
    assertTrue(out.contains("file3line2"));

    Files.append("file3line3\nfile3line4\n", f3, Charsets.UTF_8);

    reader.updateTailFiles();
    for (TailFile tf : reader.getTailFiles().values()) {
      List<String> bodies = bodiesAsStrings(reader.readEvents(tf, 2));
      out.addAll(bodies);
      reader.commit();
    }
    assertEquals(8, out.size());
    assertTrue(out.contains("file3line3"));
    assertTrue(out.contains("file3line4"));
  }

  @Test
  // Make sure this works when there are initially no files
  // and we finish reading all files and fully commit.
  public void testInitiallyEmptyDirAndBehaviorAfterReadingAll() throws IOException {
    ReliableTaildirEventReader reader = getReader();

    List<Long> fileInodes = reader.updateTailFiles();
    assertEquals(0, fileInodes.size());

    File f1 = new File(tmpDir, "file1");
    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

    reader.updateTailFiles();
    List<String> out = null;
    for (TailFile tf : reader.getTailFiles().values()) {
      out = bodiesAsStrings(reader.readEvents(tf, 2));
      reader.commit();
    }
    assertEquals(2, out.size());
    // Make sure we got every line
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));

    reader.updateTailFiles();
    List<String> empty = null;
    for (TailFile tf : reader.getTailFiles().values()) {
      empty = bodiesAsStrings(reader.readEvents(tf, 15));
      reader.commit();
    }
    assertEquals(0, empty.size());
  }

  @Test
  // Test a basic case where a commit is missed.
  public void testBasicCommitFailure() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n" +
                "file1line9\nfile1line10\nfile1line11\nfile1line12\n",
                f1, Charsets.UTF_8);

    ReliableTaildirEventReader reader = getReader();
    List<String> out1 = null;
    for (TailFile tf : reader.getTailFiles().values()) {
      out1 = bodiesAsStrings(reader.readEvents(tf, 4));
    }
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));

    List<String> out2 = bodiesAsStrings(reader.readEvents(4));
    assertTrue(out2.contains("file1line1"));
    assertTrue(out2.contains("file1line2"));
    assertTrue(out2.contains("file1line3"));
    assertTrue(out2.contains("file1line4"));

    reader.commit();
    List<String> out3 = bodiesAsStrings(reader.readEvents(4));
    assertTrue(out3.contains("file1line5"));
    assertTrue(out3.contains("file1line6"));
    assertTrue(out3.contains("file1line7"));
    assertTrue(out3.contains("file1line8"));

    reader.commit();
    List<String> out4 = bodiesAsStrings(reader.readEvents(4));
    assertEquals(4, out4.size());
    assertTrue(out4.contains("file1line9"));
    assertTrue(out4.contains("file1line10"));
    assertTrue(out4.contains("file1line11"));
    assertTrue(out4.contains("file1line12"));
  }

  @Test
  // Test a case where a commit is missed and the batch size changes.
  public void testBasicCommitFailureAndBatchSizeChanges() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    ReliableTaildirEventReader reader = getReader();
    List<String> out1 = null;
    for (TailFile tf : reader.getTailFiles().values()) {
      out1 = bodiesAsStrings(reader.readEvents(tf, 5));
    }
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));
    assertTrue(out1.contains("file1line5"));

    List<String> out2 = bodiesAsStrings(reader.readEvents(2));
    assertTrue(out2.contains("file1line1"));
    assertTrue(out2.contains("file1line2"));

    reader.commit();
    List<String> out3 = bodiesAsStrings(reader.readEvents(2));
    assertTrue(out3.contains("file1line3"));
    assertTrue(out3.contains("file1line4"));

    reader.commit();
    List<String> out4 = bodiesAsStrings(reader.readEvents(15));
    assertTrue(out4.contains("file1line5"));
    assertTrue(out4.contains("file1line6"));
    assertTrue(out4.contains("file1line7"));
    assertTrue(out4.contains("file1line8"));
  }

  @Test
  public void testIncludeEmptyFile() throws IOException {
    File f1 = new File(tmpDir, "file1");
    File f2 = new File(tmpDir, "file2");
    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.touch(f2);

    ReliableTaildirEventReader reader = getReader();
    // Expect to read nothing from empty file
    List<String> out = Lists.newArrayList();
    for (TailFile tf : reader.getTailFiles().values()) {
      out.addAll(bodiesAsStrings(reader.readEvents(tf, 5)));
      reader.commit();
    }
    assertEquals(2, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertNull(reader.readEvent());
  }

  @Test
  public void testBackoffWithoutNewLine() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("file1line1\nfile1", f1, Charsets.UTF_8);

    ReliableTaildirEventReader reader = getReader();
    List<String> out = Lists.newArrayList();
    // Expect to read only the line with newline
    for (TailFile tf : reader.getTailFiles().values()) {
      out.addAll(bodiesAsStrings(reader.readEvents(tf, 5)));
      reader.commit();
    }
    assertEquals(1, out.size());
    assertTrue(out.contains("file1line1"));

    Files.append("line2\nfile1line3\nfile1line4", f1, Charsets.UTF_8);

    for (TailFile tf : reader.getTailFiles().values()) {
      out.addAll(bodiesAsStrings(reader.readEvents(tf, 5)));
      reader.commit();
    }
    assertEquals(3, out.size());
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file1line3"));

    // Should read the last line if it finally has no newline
    out.addAll(bodiesAsStrings(reader.readEvents(5, false)));
    reader.commit();
    assertEquals(4, out.size());
    assertTrue(out.contains("file1line4"));
  }

  @Test
  public void testBatchedReadsAcrossFileBoundary() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    ReliableTaildirEventReader reader = getReader();
    List<String> out1 = Lists.newArrayList();
    for (TailFile tf : reader.getTailFiles().values()) {
      out1.addAll(bodiesAsStrings(reader.readEvents(tf, 5)));
      reader.commit();
    }

    File f2 = new File(tmpDir, "file2");
    Files.write("file2line1\nfile2line2\nfile2line3\nfile2line4\n" +
                "file2line5\nfile2line6\nfile2line7\nfile2line8\n",
                f2, Charsets.UTF_8);

    List<String> out2 = bodiesAsStrings(reader.readEvents(5));
    reader.commit();

    reader.updateTailFiles();
    List<String> out3 = Lists.newArrayList();
    for (TailFile tf : reader.getTailFiles().values()) {
      out3.addAll(bodiesAsStrings(reader.readEvents(tf, 5)));
      reader.commit();
    }

    // Should have first 5 lines of file1
    assertEquals(5, out1.size());
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));
    assertTrue(out1.contains("file1line5"));

    // Should have 3 remaining lines of file1
    assertEquals(3, out2.size());
    assertTrue(out2.contains("file1line6"));
    assertTrue(out2.contains("file1line7"));
    assertTrue(out2.contains("file1line8"));

    // Should have first 5 lines of file2
    assertEquals(5, out3.size());
    assertTrue(out3.contains("file2line1"));
    assertTrue(out3.contains("file2line2"));
    assertTrue(out3.contains("file2line3"));
    assertTrue(out3.contains("file2line4"));
    assertTrue(out3.contains("file2line5"));
  }

  @Test
  public void testLargeNumberOfFiles() throws IOException {
    int fileNum = 1000;
    Set<String> expected = Sets.newHashSet();

    for (int i = 0; i < fileNum; i++) {
      String data = "data" + i;
      File f = new File(tmpDir, "file" + i);
      Files.write(data + "\n", f, Charsets.UTF_8);
      expected.add(data);
    }

    ReliableTaildirEventReader reader = getReader();
    for (TailFile tf : reader.getTailFiles().values()) {
      List<Event> events = reader.readEvents(tf, 10);
      for (Event e : events) {
        expected.remove(new String(e.getBody()));
      }
      reader.commit();
    }
    assertEquals(0, expected.size());
  }

  @Test
  public void testLoadPositionFile() throws IOException {
    File f1 = new File(tmpDir, "file1");
    File f2 = new File(tmpDir, "file2");
    File f3 = new File(tmpDir, "file3");

    Files.write("file1line1\nfile1line2\nfile1line3\n", f1, Charsets.UTF_8);
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);
    Files.write("file3line1\n", f3, Charsets.UTF_8);

    ReliableTaildirEventReader reader = getReader();
    Map<Long, TailFile> tailFiles = reader.getTailFiles();

    long pos = f2.length();
    int i = 1;
    File posFile = new File(posFilePath);
    for (TailFile tf : tailFiles.values()) {
      Files.append(i == 1 ? "[" : "", posFile, Charsets.UTF_8);
      Files.append(String.format("{\"inode\":%s,\"pos\":%s,\"file\":\"%s\"}",
          tf.getInode(), pos, tf.getPath()), posFile, Charsets.UTF_8);
      Files.append(i == 3 ? "]" : ",", posFile, Charsets.UTF_8);
      i++;
    }
    reader.loadPositionFile(posFilePath);

    for (TailFile tf : tailFiles.values()) {
      if (tf.getPath().equals(tmpDir + "file3")) {
        // when given position is larger than file size
        assertEquals(0, tf.getPos());
      } else {
        assertEquals(pos, tf.getPos());
      }
    }
  }

  @Test
  public void testSkipToEndPosition() throws IOException {
    ReliableTaildirEventReader reader = getReader();
    File f1 = new File(tmpDir, "file1");
    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

    reader.updateTailFiles();
    for (TailFile tf : reader.getTailFiles().values()) {
      if (tf.getPath().equals(tmpDir + "file1")) {
        assertEquals(0, tf.getPos());
      }
    }

    File f2 = new File(tmpDir, "file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);
    // Expect to skip to EOF the read position when skipToEnd option is true
    reader.updateTailFiles(true);
    for (TailFile tf : reader.getTailFiles().values()) {
      if (tf.getPath().equals(tmpDir + "file2")) {
        assertEquals(f2.length(), tf.getPos());
      }
    }
  }

  @Test
  public void testByteOffsetHeader() throws IOException {
    File f1 = new File(tmpDir, "file1");
    String line1 = "file1line1\n";
    String line2 = "file1line2\n";
    String line3 = "file1line3\n";
    Files.write(line1 + line2 + line3, f1, Charsets.UTF_8);

    ReliableTaildirEventReader reader = getReader(true);
    List<String> headers = null;
    for (TailFile tf : reader.getTailFiles().values()) {
      headers = headersAsStrings(reader.readEvents(tf, 5), BYTE_OFFSET_HEADER_KEY);
      reader.commit();
    }
    assertEquals(3, headers.size());
    // Make sure we got byte offset position
    assertTrue(headers.contains(String.valueOf(0)));
    assertTrue(headers.contains(String.valueOf(line1.length())));
    assertTrue(headers.contains(String.valueOf((line1 + line2).length())));
  }

  @Test
  public void testNewLineBoundaries() throws IOException {
    File f1 = new File(tmpDir, "file1");
    Files.write("file1line1\nfile1line2\rfile1line2\nfile1line3\r\nfile1line4\n",
                f1, Charsets.UTF_8);

    ReliableTaildirEventReader reader = getReader();
    List<String> out = Lists.newArrayList();
    for (TailFile tf : reader.getTailFiles().values()) {
      out.addAll(bodiesAsStrings(reader.readEvents(tf, 5)));
      reader.commit();
    }
    assertEquals(4, out.size());
    //Should treat \n as line boundary
    assertTrue(out.contains("file1line1"));
    //Should not treat \r as line boundary
    assertTrue(out.contains("file1line2\rfile1line2"));
    //Should treat \r\n as line boundary
    assertTrue(out.contains("file1line3"));
    assertTrue(out.contains("file1line4"));
  }
}
