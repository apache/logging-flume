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
package org.apache.flume.serialization;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;
import java.util.List;

public class TestResettableFileInputStream {

  private static final boolean CLEANUP = true;
  private static final File WORK_DIR =
      new File("target/test/work").getAbsoluteFile();
  private static final Logger logger = LoggerFactory.getLogger
      (TestResettableFileInputStream.class);

  private File file, meta;

  @Before
  public void setup() throws Exception {
    Files.createParentDirs(new File(WORK_DIR, "dummy"));
    file = File.createTempFile(getClass().getSimpleName(), ".txt", WORK_DIR);
    logger.info("Data file: {}", file);
    meta = File.createTempFile(getClass().getSimpleName(), ".avro", WORK_DIR);
    logger.info("PositionTracker meta file: {}", meta);
    meta.delete(); // We want the filename but not the empty file
  }

  @After
  public void tearDown() throws Exception {
    if (CLEANUP) {
      meta.delete();
      file.delete();
    }
  }

  /**
   * Ensure that we can simply read bytes from a file.
   * @throws IOException
   */
  @Test
  public void testBasicRead() throws IOException {
    String output = singleLineFileInit(file, Charsets.UTF_8);

    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file,  tracker);

    String result = readLine(in, output.length());
    assertEquals(output, result);

    String afterEOF = readLine(in, output.length());
    assertNull(afterEOF);

    in.close();
  }

  /**
   * Ensure that we can process lines that contain multi byte characters in weird places
   * such as at the end of a buffer.
   * @throws IOException
   */
  @Test
  public void testWideCharRead() throws IOException {
    String output = wideCharFileInit(file, Charsets.UTF_8);

    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file,  tracker);

    String result = readLine(in, output.length());
    assertEquals(output, result);

    String afterEOF = readLine(in, output.length());
    assertNull(afterEOF);

    in.close();
  }

  @Test(expected = MalformedInputException.class)
  public void testUtf8DecodeErrorHandlingFailMalformed() throws IOException {
    ResettableInputStream in = initUtf8DecodeTest(DecodeErrorPolicy.FAIL);
    while (in.readChar() != -1) {
      // Do nothing... read the whole file and throw away the bytes.
    }
    fail("Expected MalformedInputException!");
  }


  @Test
  public void testUtf8DecodeErrorHandlingIgnore() throws IOException {
    ResettableInputStream in = initUtf8DecodeTest(DecodeErrorPolicy.IGNORE);
    int c;
    StringBuilder sb = new StringBuilder();
    while ((c = in.readChar()) != -1) {
      sb.append((char)c);
    }
    assertEquals("Latin1: ()\nLong: ()\nNonUnicode: ()\n", sb.toString());
  }

  @Test
  public void testUtf8DecodeErrorHandlingReplace() throws IOException {
    ResettableInputStream in = initUtf8DecodeTest(DecodeErrorPolicy.REPLACE);
    int c;
    StringBuilder sb = new StringBuilder();
    while ((c = in.readChar()) != -1) {
      sb.append((char)c);
    }
    assertEquals("Latin1: (X)\nLong: (XXX)\nNonUnicode: (X)\n"
        .replaceAll("X", "\ufffd"), sb.toString());
  }

  @Test(expected = MalformedInputException.class)
  public void testLatin1DecodeErrorHandlingFailMalformed() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    generateLatin1InvalidSequence(out);
    Files.write(out.toByteArray(), file);
    ResettableInputStream in = initInputStream(DecodeErrorPolicy.FAIL);
    while (in.readChar() != -1) {
      // Do nothing... read the whole file and throw away the bytes.
    }
    fail("Expected MalformedInputException!");
  }

  @Test
  public void testLatin1DecodeErrorHandlingReplace() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    generateLatin1InvalidSequence(out);
    Files.write(out.toByteArray(), file);
    ResettableInputStream in = initInputStream(DecodeErrorPolicy.REPLACE);

    int c;
    StringBuilder sb = new StringBuilder();
    while ((c = in.readChar()) != -1) {
      sb.append((char)c);
    }
    assertEquals("Invalid: (X)\n".replaceAll("X", "\ufffd"), sb.toString());
  }

  private ResettableInputStream initUtf8DecodeTest(DecodeErrorPolicy policy)
      throws IOException {
    writeBigBadUtf8Sequence(file);
    return initInputStream(policy);
  }

  private ResettableInputStream initInputStream(DecodeErrorPolicy policy)
      throws IOException {
    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file, tracker,
        2048, Charsets.UTF_8, policy);
    return in;
  }

  private void writeBigBadUtf8Sequence(File file) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    generateUtf8Latin1Sequence(out);
    generateUtf8OverlyLongSequence(out);
    generateUtf8NonUnicodeSequence(out);
    Files.write(out.toByteArray(), file);
  }

  private void generateUtf8OverlyLongSequence(OutputStream out)
      throws IOException {
    out.write("Long: (".getBytes(Charsets.UTF_8));
    // Overly-long slash character should not be accepted.
    out.write(new byte[] { (byte)0xe0, (byte)0x80, (byte)0xaf });
    out.write(")\n".getBytes(Charsets.UTF_8));
  }

  private void generateUtf8NonUnicodeSequence(OutputStream out)
      throws IOException {
    out.write("NonUnicode: (".getBytes(Charsets.UTF_8));
    // This is a valid 5-octet sequence but is not Unicode
    out.write(new byte[] { (byte)0xf8, (byte)0xa1, (byte)0xa1, (byte)0xa1,
        (byte)0xa1 } );
    out.write(")\n".getBytes(Charsets.UTF_8));
  }

  private void generateUtf8Latin1Sequence(OutputStream out) throws IOException {
    out.write("Latin1: (".getBytes(Charsets.UTF_8));
    // This is "e" with an accent in Latin-1
    out.write(new byte[] { (byte)0xe9 } );
    out.write(")\n".getBytes(Charsets.UTF_8));
  }

  private void generateLatin1InvalidSequence(OutputStream out)
      throws IOException {
    out.write("Invalid: (".getBytes(Charsets.UTF_8));
    // Not a valid character in Latin 1.
    out.write(new byte[] { (byte)0x81 } );
    out.write(")\n".getBytes(Charsets.UTF_8));
  }

  /**
   * Ensure a reset() brings us back to the default mark (beginning of file)
   * @throws IOException
   */
  @Test
  public void testReset() throws IOException {
    String output = singleLineFileInit(file, Charsets.UTF_8);

    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file, tracker);

    String result1 = readLine(in, output.length());
    assertEquals(output, result1);

    in.reset();
    String result2 = readLine(in, output.length());
    assertEquals(output, result2);

    String result3 = readLine(in, output.length());
    assertNull("Should be null: " + result3, result3);

    in.close();
  }

  /**
   * Ensure that marking and resetting works.
   * @throws IOException
   */
  @Test
  public void testMarkReset() throws IOException {
    List<String> expected = multiLineFileInit(file, Charsets.UTF_8);

    int MAX_LEN = 100;

    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file, tracker);

    String result0 = readLine(in, MAX_LEN);
    assertEquals(expected.get(0), result0);

    in.reset();

    String result0a = readLine(in, MAX_LEN);
    assertEquals(expected.get(0), result0a);

    in.mark();

    String result1 = readLine(in, MAX_LEN);
    assertEquals(expected.get(1), result1);

    in.reset();

    String result1a = readLine(in, MAX_LEN);
    assertEquals(expected.get(1), result1a);

    in.mark();
    in.close();
  }

  @Test
  public void testResume() throws IOException {
    List<String> expected = multiLineFileInit(file, Charsets.UTF_8);

    int MAX_LEN = 100;

    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file, tracker);

    String result0 = readLine(in, MAX_LEN);
    String result1 = readLine(in, MAX_LEN);
    in.mark();

    String result2 = readLine(in, MAX_LEN);
    Assert.assertEquals(expected.get(2), result2);
    String result3 = readLine(in, MAX_LEN);
    Assert.assertEquals(expected.get(3), result3);
    in.close();
    tracker.close(); // redundant

    // create new Tracker & RIS
    tracker = new DurablePositionTracker(meta, file.getPath());
    in = new ResettableFileInputStream(file, tracker);

    String result2a = readLine(in, MAX_LEN);
    String result3a = readLine(in, MAX_LEN);

    Assert.assertEquals(result2, result2a);
    Assert.assertEquals(result3, result3a);
  }

  @Test
  public void testSeek() throws IOException {
    int NUM_LINES = 1000;
    int LINE_LEN = 1000;
    generateData(file, Charsets.UTF_8, NUM_LINES, LINE_LEN);

    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file, tracker,
        10 * LINE_LEN, Charsets.UTF_8, DecodeErrorPolicy.FAIL);

    String line = "";
    for (int i = 0; i < 9; i++) {
      line = readLine(in, LINE_LEN);
    }
    int lineNum = Integer.parseInt(line.substring(0, 10));
    assertEquals(8, lineNum);

    // seek back within our buffer
    long pos = in.tell();
    in.seek(pos - 2 * LINE_LEN); // jump back 2 lines

    line = readLine(in, LINE_LEN);
    lineNum = Integer.parseInt(line.substring(0, 10));
    assertEquals(7, lineNum);

    // seek forward within our buffer
    in.seek(in.tell() + LINE_LEN);
    line = readLine(in, LINE_LEN);
    lineNum = Integer.parseInt(line.substring(0, 10));
    assertEquals(9, lineNum);

    // seek forward outside our buffer
    in.seek(in.tell() + 20 * LINE_LEN);
    line = readLine(in, LINE_LEN);
    lineNum = Integer.parseInt(line.substring(0, 10));
    assertEquals(30, lineNum);

    // seek backward outside our buffer
    in.seek(in.tell() - 25 * LINE_LEN);
    line = readLine(in, LINE_LEN);
    lineNum = Integer.parseInt(line.substring(0, 10));
    assertEquals(6, lineNum);

    // test a corner-case seek which requires a buffer refill
    in.seek(100 * LINE_LEN);
    in.seek(0); // reset buffer

    in.seek(9 * LINE_LEN);
    assertEquals(9, Integer.parseInt(readLine(in, LINE_LEN).substring(0, 10)));
    assertEquals(10, Integer.parseInt(readLine(in, LINE_LEN).substring(0, 10)));
    assertEquals(11, Integer.parseInt(readLine(in, LINE_LEN).substring(0, 10)));
  }

  /**
   * Helper method that generates a line to test if parts of multi-byte characters on the
   * edge of a buffer are handled properly.
   */
  private static String generateWideCharLine(){
    String s = "éllo Wörld!\n";
    int size = (ResettableFileInputStream.DEFAULT_BUF_SIZE - 1) + s.length();
    return Strings.padStart(s, size , 'H');
  }

  /**
   * Creates a file that contains a line that contains wide characters
   * @param file
   * @param charset
   * @return
   * @throws IOException
   */
  private static String wideCharFileInit(File file, Charset charset)
      throws IOException {
    String output = generateWideCharLine();
    Files.write(output.getBytes(charset), file);
    return output;
  }

  /**
   * Helper function to read a line from a character stream.
   * @param in
   * @param maxLength
   * @return
   * @throws IOException
   */
  private static String readLine(ResettableInputStream in, int maxLength)
      throws IOException {

    StringBuilder s = new StringBuilder();
    int c;
    int i = 1;
    while ((c = in.readChar()) != -1) {
      // FIXME: support \r\n
      if (c == '\n') {
        break;
      }
      //System.out.printf("seen char val: %c\n", (char)c);
      s.append((char)c);

      if (i++ > maxLength) {
        System.out.println("Output: >" + s + "<");
        throw new RuntimeException("Too far!");
      }
    }
    if (s.length() > 0) {
      s.append('\n');
      return s.toString();
    } else {
      return null;
    }
  }

  private static String singleLineFileInit(File file, Charset charset)
      throws IOException {
    String output = "This is gonna be great!\n";
    Files.write(output.getBytes(charset), file);
    return output;
  }

  private static List<String> multiLineFileInit(File file, Charset charset)
      throws IOException {
    List<String> lines = Lists.newArrayList();
    lines.add("1. On the planet of Mars\n");
    lines.add("2. They have clothes just like ours,\n");
    lines.add("3. And they have the same shoes and same laces,\n");
    lines.add("4. And they have the same charms and same graces...\n");
    StringBuilder sb = new StringBuilder();
    for (String line : lines) {
      sb.append(line);
    }
    Files.write(sb.toString().getBytes(charset), file);
    return lines;
  }

  private static void generateData(File file, Charset charset,
      int numLines, int lineLen) throws IOException {

    OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
    StringBuilder junk = new StringBuilder();
    for (int x = 0; x < lineLen - 13; x++) {
      junk.append('x');
    }
    String payload = junk.toString();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < numLines; i++) {
      builder.append(String.format("%010d: %s\n", i, payload));
      if (i % 1000 == 0 && i != 0) {
        out.write(builder.toString().getBytes(charset));
        builder.setLength(0);
      }
    }

    out.write(builder.toString().getBytes(charset));
    out.close();

    Assert.assertEquals(lineLen * numLines, file.length());
  }

}
