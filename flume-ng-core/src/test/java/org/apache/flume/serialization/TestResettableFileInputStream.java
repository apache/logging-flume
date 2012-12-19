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

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
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
    assertNull(result3);

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

}
