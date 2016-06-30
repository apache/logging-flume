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
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestResettableFileInputStream {

  private static final boolean CLEANUP = true;
  private static final File WORK_DIR =
      new File("target/test/work").getAbsoluteFile();
  private static final Logger logger =
      LoggerFactory.getLogger(TestResettableFileInputStream.class);

  private File file;
  private File meta;

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
   * Ensure that we can simply read bytes from a file using InputStream.read() method.
   * @throws IOException
   */
  @Test
  public void testReadByte() throws IOException {
    byte[] bytes = new byte[255];
    for (int i = 0; i < 255; i++) {
      bytes[i] = (byte) i;
    }

    Files.write(bytes, file);

    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file, tracker);

    for (int i = 0; i < 255; i++) {
      assertEquals(i, in.read());
    }
    assertEquals(-1, in.read());

    in.close();
  }

  /**
   * Ensure that we can process lines that contain multi byte characters in weird places
   * such as at the end of a buffer.
   * @throws IOException
   */
  @Test
  public void testMultiByteCharRead() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write("1234567".getBytes(Charsets.UTF_8));
    // write a multi byte char encompassing buffer boundaries
    generateUtf83ByteSequence(out);
    // buffer now contains 8 chars and 10 bytes total
    Files.write(out.toByteArray(), file);
    ResettableInputStream in = initInputStream(8, Charsets.UTF_8, DecodeErrorPolicy.FAIL);
    String result = readLine(in, 8);
    assertEquals("1234567\u0A93\n", result);
  }

  /**
   * Ensure that we can process UTF-8 lines that contain surrogate pairs
   * even if they appear astride buffer boundaries.
   * @throws IOException
   */
  @Test
  public void testUtf8SurrogatePairRead() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write("1234567".getBytes(Charsets.UTF_8));
    generateUtf8SurrogatePairSequence(out);
    // buffer now contains 9 chars (7 "normal" and 2 surrogates) and 11 bytes total
    // surrogate pair will encompass buffer boundaries
    Files.write(out.toByteArray(), file);
    ResettableInputStream in = initInputStream(8, Charsets.UTF_8, DecodeErrorPolicy.FAIL);
    String result = readLine(in, 9);
    assertEquals("1234567\uD83D\uDE18\n", result);
  }

  /**
   * Ensure that we can process UTF-16 lines that contain surrogate pairs, even
   * preceded by a Byte Order Mark (BOM).
   * @throws IOException
   */
  @Test
  public void testUtf16BOMAndSurrogatePairRead() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    generateUtf16SurrogatePairSequence(out);
    // buffer now contains 1 BOM and 2 chars (1 surrogate pair) and 6 bytes total
    // (including 2-byte BOM)
    Files.write(out.toByteArray(), file);
    ResettableInputStream in = initInputStream(8, Charsets.UTF_16, DecodeErrorPolicy.FAIL);
    String result = readLine(in, 2);
    assertEquals("\uD83D\uDE18\n", result);
  }

  /**
   * Ensure that we can process Shift_JIS lines that contain multi byte Japanese chars
   * even if they appear astride buffer boundaries.
   * @throws IOException
   */
  @Test
  public void testShiftJisSurrogateCharRead() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write("1234567".getBytes(Charset.forName("Shift_JIS")));
    // write a multi byte char encompassing buffer boundaries
    generateShiftJis2ByteSequence(out);
    // buffer now contains 8 chars and 10 bytes total
    Files.write(out.toByteArray(), file);
    ResettableInputStream in = initInputStream(8, Charset.forName("Shift_JIS"),
                                               DecodeErrorPolicy.FAIL);
    String result = readLine(in, 8);
    assertEquals("1234567\u4E9C\n", result);
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
    String preJdk8ExpectedStr = "Latin1: (X)\nLong: (XXX)\nNonUnicode: (X)\n";
    String expectedStr = "Latin1: (X)\nLong: (XXX)\nNonUnicode: (XXXXX)\n";
    String javaVersionStr = System.getProperty("java.version");
    double javaVersion = Double.parseDouble(javaVersionStr.substring(0, 3));

    if (javaVersion < 1.8) {
      assertTrue(preJdk8ExpectedStr.replaceAll("X", "\ufffd").equals(sb.toString()));
    } else {
      assertTrue(expectedStr.replaceAll("X", "\ufffd").equals(sb.toString()));
    }
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

  /**
   * Ensure that surrogate pairs work well with mark/reset.
   * @throws IOException
   */
  @Test
  public void testMarkResetWithSurrogatePairs() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write("foo".getBytes(Charsets.UTF_8));
    generateUtf8SurrogatePairSequence(out);
    out.write("bar".getBytes(Charsets.UTF_8));
    Files.write(out.toByteArray(), file);
    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file, tracker);
    Assert.assertEquals('f', in.readChar());
    Assert.assertEquals('o', in.readChar());
    in.mark();
    Assert.assertEquals('o', in.readChar());
    // read high surrogate
    Assert.assertEquals('\ud83d', in.readChar());
    // call reset in the middle of a surrogate pair
    in.reset();
    // will read low surrogate *before* reverting back to mark, to ensure
    // surrogate pair is properly read
    Assert.assertEquals('\ude18', in.readChar());
    // now back to marked position
    Assert.assertEquals('o', in.readChar());
    // read high surrogate again
    Assert.assertEquals('\ud83d', in.readChar());
    // call mark in the middle of a surrogate pair:
    // will mark the position *after* the pair, *not* low surrogate's position
    in.mark();
    // will reset to the position *after* the pair
    in.reset();
    // read low surrogate normally despite of reset being called
    // so that the pair is entirely read
    Assert.assertEquals('\ude18', in.readChar());
    Assert.assertEquals('b', in.readChar());
    Assert.assertEquals('a', in.readChar());
    // will reset to the position *after* the pair
    in.reset();
    Assert.assertEquals('b', in.readChar());
    Assert.assertEquals('a', in.readChar());
    Assert.assertEquals('r', in.readChar());
    Assert.assertEquals(-1, in.readChar());
    in.close();
    tracker.close(); // redundant
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

  /**
   * Ensure that surrogate pairs work well when resuming
   * reading. Specifically, this test brings up special situations
   * where a surrogate pair cannot be correctly decoded because
   * the second character is lost.
   *
   * @throws IOException
   */
  @Test
  public void testResumeWithSurrogatePairs() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write("foo".getBytes(Charsets.UTF_8));
    generateUtf8SurrogatePairSequence(out);
    out.write("bar".getBytes(Charsets.UTF_8));
    Files.write(out.toByteArray(), file);
    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file, tracker);
    Assert.assertEquals('f', in.readChar());
    Assert.assertEquals('o', in.readChar());
    in.mark();
    Assert.assertEquals('o', in.readChar());
    // read high surrogate
    Assert.assertEquals('\ud83d', in.readChar());
    // call reset in the middle of a surrogate pair
    in.reset();
    // close RIS - this will cause the low surrogate char
    // stored in-memory to be lost
    in.close();
    tracker.close(); // redundant
    // create new Tracker & RIS
    tracker = new DurablePositionTracker(meta, file.getPath());
    in = new ResettableFileInputStream(file, tracker);
    // low surrogate char is now lost - resume from marked position
    Assert.assertEquals('o', in.readChar());
    // read high surrogate again
    Assert.assertEquals('\ud83d', in.readChar());
    // call mark in the middle of a surrogate pair:
    // will mark the position *after* the pair, *not* low surrogate's position
    in.mark();
    // close RIS - this will cause the low surrogate char
    // stored in-memory to be lost
    in.close();
    tracker.close(); // redundant
    // create new Tracker & RIS
    tracker = new DurablePositionTracker(meta, file.getPath());
    in = new ResettableFileInputStream(file, tracker);
    // low surrogate char is now lost - resume from marked position
    Assert.assertEquals('b', in.readChar());
    Assert.assertEquals('a', in.readChar());
    Assert.assertEquals('r', in.readChar());
    Assert.assertEquals(-1, in.readChar());
    in.close();
    tracker.close(); // redundant
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

  private ResettableInputStream initUtf8DecodeTest(DecodeErrorPolicy policy)
      throws IOException {
    writeBigBadUtf8Sequence(file);
    return initInputStream(policy);
  }

  private ResettableInputStream initInputStream(DecodeErrorPolicy policy)
      throws IOException {
    return initInputStream(2048, Charsets.UTF_8, policy);
  }

  private ResettableInputStream initInputStream(int bufferSize, Charset charset,
                                                DecodeErrorPolicy policy) throws IOException {
    PositionTracker tracker = new DurablePositionTracker(meta, file.getPath());
    ResettableInputStream in = new ResettableFileInputStream(file, tracker,
        bufferSize, charset, policy);
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
    out.write(new byte[]{(byte) 0xf8, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1,
        (byte) 0xa1});
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

  private void generateUtf8SurrogatePairSequence(OutputStream out) throws IOException {
    // U+1F618 (UTF-8: f0 9f 98 98) FACE THROWING A KISS
    out.write(new byte[]{(byte) 0xF0, (byte) 0x9F, (byte) 0x98, (byte) 0x98});
  }

  private void generateUtf16SurrogatePairSequence(OutputStream out) throws IOException {
    // BOM
    out.write(new byte[]{(byte) 0xFE, (byte) 0xFF});
    // U+1F618 (UTF-16: d83d de18) FACE THROWING A KISS
    out.write(new byte[]{(byte) 0xD8, (byte) 0x3D, (byte) 0xDE, (byte) 0x18});
  }

  private void generateUtf83ByteSequence(OutputStream out) throws IOException {
    // U+0A93 (UTF-8: e0 aa 93) GUJARATI LETTER O
    out.write(new byte[]{(byte) 0xe0, (byte) 0xaa, (byte) 0x93});
  }

  private void generateShiftJis2ByteSequence(OutputStream out) throws IOException {
    //U+4E9C (Shift JIS: 88 9f) CJK UNIFIED IDEOGRAPH
    out.write(new byte[]{(byte) 0x88, (byte) 0x9f});
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
