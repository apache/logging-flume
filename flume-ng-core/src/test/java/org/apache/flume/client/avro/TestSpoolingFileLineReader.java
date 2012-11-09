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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.flume.FlumeException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestSpoolingFileLineReader {
  private static String completedSuffix = ".COMPLETE";
  private static int bufferMaxLineLength = 500;
  private static int bufferMaxLines = 30;

  private File tmpDir;

  @Before
  public void setUp() {
    tmpDir = Files.createTempDir();
  }

  @After
  public void tearDown() {
    for (File f : tmpDir.listFiles()) {
      f.delete();
    }
    tmpDir.delete();
  }

  @Test
  /** Create three multi-line files then read them back out. Ensures that
   * files are accessed in correct order and that lines are read correctly
   * from files. */
  public void testBasicSpooling() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    File f3 = new File(tmpDir.getAbsolutePath() + "/file3");

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);
    Files.write("file3line1\nfile3line2\n", f3, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out = Lists.newArrayList();
    for (int i = 0; i < 6; i++) {
      out.add(reader.readLine());
      reader.commit();
    }
    // Make sure we got every line
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file2line1"));
    assertTrue(out.contains("file2line2"));
    assertTrue(out.contains("file3line1"));
    assertTrue(out.contains("file3line2"));

    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());

    assertEquals(3, outFiles.size());

    // Make sure files 1 and 2 have been processed and file 3 is still open
    assertTrue(outFiles.contains(new File(tmpDir + "/file1" + completedSuffix)));
    assertTrue(outFiles.contains(new File(tmpDir + "/file2" + completedSuffix)));
    assertTrue(outFiles.contains(new File(tmpDir + "/file3")));
  }

  @Test
  /** Make sure this works when there are initially no files */
  public void testInitiallyEmptyDirectory() throws IOException {

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    assertNull(reader.readLine());
    assertEquals(0, reader.readLines(10).size());

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

    List<String> out = reader.readLines(2);
    reader.commit();

    // Make sure we got all of the first file
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);

    reader.readLine(); // force roll
    reader.commit();

    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());

    assertEquals(2, outFiles.size());
    assertTrue(
        outFiles.contains(new File(tmpDir + "/file1" + completedSuffix)));
    assertTrue(outFiles.contains(new File(tmpDir + "/file2")));
  }

  @Test(expected = IllegalStateException.class)
  /** Ensures that file immutability is enforced. */
  public void testFileChangesDuringRead() throws IOException {
    File tmpDir1 = Files.createTempDir();
    File f1 = new File(tmpDir1.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

    SpoolingFileLineReader reader1 =
        new SpoolingFileLineReader(tmpDir1, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out = Lists.newArrayList();
    out.addAll(reader1.readLines(2));
    reader1.commit();

    assertEquals(2, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));

    Files.append("file1line3\n", f1, Charsets.UTF_8);

    out.add(reader1.readLine());
    reader1.commit();
    out.add(reader1.readLine());
    reader1.commit();
  }


  /** Test when a competing destination file is found, but it matches,
   *  and we are on a Windows machine. */
  @Test
  public void testDestinationExistsAndSameFileWindows() throws IOException {
    System.setProperty("os.name", "Some version of Windows");

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f1Completed = new File(tmpDir.getAbsolutePath() + "/file1" +
        completedSuffix);

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file1line1\nfile1line2\n", f1Completed, Charsets.UTF_8);

    SpoolingFileLineReader reader =
        new SpoolingFileLineReader(tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out = Lists.newArrayList();

    for (int i = 0; i < 2; i++) {
      out.add(reader.readLine());
      reader.commit();
    }

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);

    for (int i = 0; i < 2; i++) {
      out.add(reader.readLine());
      reader.commit();
    }

    // Make sure we got every line
    assertEquals(4, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file2line1"));
    assertTrue(out.contains("file2line2"));

    // Make sure original is deleted
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
    assertEquals(2, outFiles.size());
    assertTrue(outFiles.contains(new File(tmpDir + "/file2")));
    assertTrue(outFiles.contains(
        new File(tmpDir + "/file1" + completedSuffix)));
  }

  /** Test when a competing destination file is found, but it matches,
   *  and we are not on a Windows machine. */
  @Test(expected = IllegalStateException.class)
  public void testDestinationExistsAndSameFileNotOnWindows() throws IOException {
    System.setProperty("os.name", "Some version of Linux");

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f1Completed = new File(tmpDir.getAbsolutePath() + "/file1" +
        completedSuffix);

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file1line1\nfile1line2\n", f1Completed, Charsets.UTF_8);

    SpoolingFileLineReader reader =
        new SpoolingFileLineReader(tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out = Lists.newArrayList();

    for (int i = 0; i < 2; i++) {
      out.add(reader.readLine());
      reader.commit();
    }

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);

    for (int i = 0; i < 2; i++) {
      out.add(reader.readLine());
      reader.commit();
    }

    // Not reached
  }

  @Test
  /** Test a basic case where a commit is missed. */
  public void testBasicCommitFailure() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n" +
                "file1line9\nfile1line10\nfile1line11\nfile1line12\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out1 = reader.readLines(4);
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));

    List<String> out2 = reader.readLines(4);
    assertTrue(out2.contains("file1line1"));
    assertTrue(out2.contains("file1line2"));
    assertTrue(out2.contains("file1line3"));
    assertTrue(out2.contains("file1line4"));

    reader.commit();

    List<String> out3 = reader.readLines(4);
    assertTrue(out3.contains("file1line5"));
    assertTrue(out3.contains("file1line6"));
    assertTrue(out3.contains("file1line7"));
    assertTrue(out3.contains("file1line8"));

    reader.commit();
    List<String> out4 = reader.readLines(10);
    assertEquals(4, out4.size());
    assertTrue(out4.contains("file1line9"));
    assertTrue(out4.contains("file1line10"));
    assertTrue(out4.contains("file1line11"));
    assertTrue(out4.contains("file1line12"));
  }

  @Test
  /** Test a case where a commit is missed and the buffer size shrinks. */
  public void testBasicCommitFailureAndBufferSizeChanges() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n" +
                "file1line9\nfile1line10\nfile1line11\nfile1line12\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out1 = reader.readLines(5);
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));
    assertTrue(out1.contains("file1line5"));

    List<String> out2 = reader.readLines(2);
    assertTrue(out2.contains("file1line1"));
    assertTrue(out2.contains("file1line2"));

    reader.commit();
    List<String> out3 = reader.readLines(2);
    assertTrue(out3.contains("file1line3"));
    assertTrue(out3.contains("file1line4"));

    reader.commit();
    List<String> out4 = reader.readLines(2);
    assertTrue(out4.contains("file1line5"));
    assertTrue(out4.contains("file1line6"));

    reader.commit();
    List<String> out5 = reader.readLines(2);
    assertTrue(out5.contains("file1line7"));
    assertTrue(out5.contains("file1line8"));

    reader.commit();

    List<String> out6 = reader.readLines(15);
    assertTrue(out6.contains("file1line9"));
    assertTrue(out6.contains("file1line10"));
    assertTrue(out6.contains("file1line11"));
    assertTrue(out6.contains("file1line12"));
  }

  /** Test when a competing destination file is found and it does not match. */
  @Test(expected = IllegalStateException.class)
  public void testDestinationExistsAndDifferentFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f1Completed =
        new File(tmpDir.getAbsolutePath() + "/file1" + completedSuffix);

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file1line1\nfile1XXXe2\n", f1Completed, Charsets.UTF_8);

    SpoolingFileLineReader reader =
        new SpoolingFileLineReader(tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out = Lists.newArrayList();

    for (int i = 0; i < 2; i++) {
      out.add(reader.readLine());
      reader.commit();
    }

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);

    for (int i = 0; i < 2; i++) {
      out.add(reader.readLine());
      reader.commit();
    }
    // Not reached
  }


  /**
   * Empty files should be treated the same as other files and rolled.
   */
  @Test
  public void testBehaviorWithEmptyFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.touch(f1);

    SpoolingFileLineReader reader =
        new SpoolingFileLineReader(tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f2, Charsets.UTF_8);

    List<String> out = reader.readLines(8); // Expect to skip over first file
    reader.commit();
    assertEquals(8, out.size());

    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file1line3"));
    assertTrue(out.contains("file1line4"));
    assertTrue(out.contains("file1line5"));
    assertTrue(out.contains("file1line6"));
    assertTrue(out.contains("file1line7"));
    assertTrue(out.contains("file1line8"));

    assertNull(reader.readLine());

    // Make sure original is deleted
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
    assertEquals(2, outFiles.size());
    assertTrue(outFiles.contains(
        new File(tmpDir + "/file1" + completedSuffix)));
    assertTrue(outFiles.contains(
        new File(tmpDir + "/file2" + completedSuffix)));
  }

  @Test
  public void testBatchedReadsWithinAFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir,
        completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> out = reader.readLines(5);
    reader.commit();

    // Make sure we got every line
    assertEquals(5, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file1line3"));
    assertTrue(out.contains("file1line4"));
    assertTrue(out.contains("file1line5"));
  }

  @Test
  public void testBatchedReadsAcrossFileBoundary() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir,
        completedSuffix, bufferMaxLines, bufferMaxLineLength);
    List<String> out1 = reader.readLines(5);
    reader.commit();

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\nfile2line3\nfile2line4\n" +
        "file2line5\nfile2line6\nfile2line7\nfile2line8\n",
        f2, Charsets.UTF_8);

    List<String> out2 = reader.readLines(5);
    reader.commit();
    List<String> out3 = reader.readLines(5);
    reader.commit();

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

    // file1 should be moved now
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
    assertEquals(2, outFiles.size());
    assertTrue(outFiles.contains(
        new File(tmpDir + "/file1" + completedSuffix)));
    assertTrue(outFiles.contains(new File(tmpDir + "/file2")));
  }

  @Test
  /** Test the case where we read finish reading and fully commit a file, then
   *  the directory is empty. */
  public void testEmptyDirectoryAfterCommittingFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, bufferMaxLineLength);

    List<String> allLines = reader.readLines(2);
    assertEquals(2, allLines.size());
    reader.commit();

    List<String> empty = reader.readLines(10);
    assertEquals(0, empty.size());
  }

  @Test(expected = FlumeException.class)
  /** When a line violates the character limit, we should throw an exception,
   * even if the buffer limit is not actually exceeded. */
  public void testLineExceedsMaxLineLengthButNotBufferSize() throws IOException {
    final int maxLineLength = 15;
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n" +
                "reallyreallyreallyreallyreallyLongfile1line9\n" +
                "file1line10\nfile1line11\nfile1line12\nfile1line13\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, maxLineLength);

    List<String> out1 = reader.readLines(5);
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));
    assertTrue(out1.contains("file1line5"));
    reader.commit();

    reader.readLines(5);
  }

  @Test(expected = FlumeException.class)
  /** When a line is larger than the entire buffer, we should definitely throw
   * an exception. */
  public void testLineExceedsBufferSize() throws IOException {
    final int maxLineLength = 15;
    final int bufferMaxLines = 10;

    // String slightly longer than buffer
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < (bufferMaxLines + 1) * maxLineLength; i++) {
      sb.append('x');
    }

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write(sb.toString() + "\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, maxLineLength);

    reader.readLines(5);
  }

  @Test
  /** When a line is larger than the entire buffer, we should definitely throw
   * an exception. */
  public void testCallsFailWhenReaderDisabled() throws IOException {
    final int maxLineLength = 15;
    final int bufferMaxLines = 10;

    // String slightly longer than buffer
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < (bufferMaxLines + 1) * maxLineLength; i++) {
      sb.append('x');
    }

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write(sb.toString() + "\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(
        tmpDir, completedSuffix, bufferMaxLines, maxLineLength);

    boolean exceptionOnRead = false;
    boolean illegalStateOnReadLine = false;
    boolean illegalStateOnReadLines = false;
    boolean illegalStateOnCommit = false;
    try {
      reader.readLines(5);
    } catch (FlumeException e) {
      exceptionOnRead = true;
    }
    try {
      reader.readLine();
    } catch (IllegalStateException e) {
      illegalStateOnReadLine = true;
    }
    try {
      reader.readLines(5);
    } catch (IllegalStateException e) {
      illegalStateOnReadLines = true;
    }
    try {
      reader.commit();
    } catch (IllegalStateException e) {
      illegalStateOnCommit = true;
    }

    Assert.assertTrue("Got FlumeException when reading long line.",
        exceptionOnRead);
    Assert.assertTrue("Got IllegalStateException when reading line.",
        illegalStateOnReadLine);
    Assert.assertTrue("Got IllegalStateException when reading lines.",
        illegalStateOnReadLines);
    Assert.assertTrue("Got IllegalStateException when commiting",
        illegalStateOnCommit);

  }



  @Test
  public void testNameCorrespondsToLatestRead() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir,
        completedSuffix, bufferMaxLines, bufferMaxLineLength);
    reader.readLines(5);
    reader.commit();

    assertNotNull(reader.getLastFileRead());
    assertTrue(reader.getLastFileRead().endsWith("file1"));

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\nfile2line3\nfile2line4\n" +
        "file2line5\nfile2line6\nfile2line7\nfile2line8\n",
        f2, Charsets.UTF_8);

    reader.readLines(5);
    reader.commit();
    assertNotNull(reader.getLastFileRead());
    assertTrue(reader.getLastFileRead().endsWith("file1"));

    reader.readLines(5);
    reader.commit();
    assertNotNull(reader.getLastFileRead());
    assertTrue(reader.getLastFileRead().endsWith("file2"));

    reader.readLines(5);
    assertTrue(reader.getLastFileRead().endsWith("file2"));

    reader.readLines(5);
    assertTrue(reader.getLastFileRead().endsWith("file2"));
  }
}
