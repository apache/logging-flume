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
package com.cloudera.util;

import java.nio.charset.Charset;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

/**
 * Experiments to see how different charsets work.
 */
public class TestCharset  {

  // Wow, hudson defaults to US-ASCII!
  @Test
  public void testDefaultCharset() {
    System.out.println("Default: " + Charset.defaultCharset());

    String charset = Charset.defaultCharset().displayName();
    System.out.println(charset);
    // Ubuntu: assertEquals("UTF-8", charset); // ubuntu
    // Hudson : assertEquals("US-ASCII", charset); // hudson
  }

  @Test
  public void testDumpCharsets() {
    System.out.println("== charsets");
    for (String s : Charset.availableCharsets().keySet()) {
      System.out.println(s);
    }
  }

  public byte[] allbytes() {
    byte[] b = new byte[256];
    for (int i = 0; i < 256; i++) {
      b[i] = (byte) i;
    }
    return b;
  }

  public String dumpHex(byte[] b) {
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < b.length; i++) {
      s.append(String.format("%02x ", b[i]));
    }
    return s.toString();
  }

  /**
   * Basically, ISO-8859-1 is "raw" -- to and from, one byte to one char
   */
  @Test
  public void testDefaultStringCharset() {
    byte[] all = allbytes();
    String auto = new String(all);
    String utf8 = new String(all, Charset.forName("UTF-8"));
    String ascii = new String(all, Charset.forName("US-ASCII"));
    String latin1 = new String(all, Charset.forName("ISO-8859-1"));

    System.out.printf("lengths: auto: %d utf8: %d ascii: %d latin1: %d\n", auto
        .length(), utf8.length(), ascii.length(), latin1.length());
    System.out.printf("bytelen: auto: %d utf8: %d ascii: %d latin1: %d\n", auto
        .getBytes().length, utf8.getBytes(Charset.forName("UTF-8")).length,
        ascii.getBytes(Charset.forName("US-ASCII")).length, latin1
            .getBytes(Charset.forName("ISO-8859-1")).length);
    System.out.printf("original : %s\n", dumpHex(all));
    System.out.printf("auto     : %s\n", dumpHex(auto.getBytes()));
    System.out.printf("utf-8    : %s\n", dumpHex(utf8.getBytes(Charset
        .forName("UTF-8"))));
    System.out.printf("ascii    : %s\n", dumpHex(ascii.getBytes(Charset
        .forName("US-ASCII"))));
    System.out.printf("latin1   : %s\n", dumpHex(latin1.getBytes(Charset
        .forName("ISO-8859-1"))));

    Assert.assertTrue(Arrays.equals(all, latin1
        .getBytes(Charset.forName("ISO-8859-1"))));
  }
}
