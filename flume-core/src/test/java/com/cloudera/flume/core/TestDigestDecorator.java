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
package com.cloudera.flume.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;

import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.handlers.debug.MemorySinkSource;

/**
 * This tests the DigestDecorator.
 */
public class TestDigestDecorator {

  /**
   * A helper function used by most of the tests here to reduce
   * boilerplate.
   */
  private void helpTestDigest(String algorithm, boolean encodeBase64) throws IOException, InterruptedException {
    MemorySinkSource mem = new MemorySinkSource();
    EventSink dig = new DigestDecorator<EventSink>(mem, algorithm, "digest", encodeBase64);

    Event e = new EventImpl("content".getBytes());

    dig.open();
    dig.append(e);
    dig.close();

    mem.open();
    Event e2 = mem.next();
    byte digest[] = e2.get("digest");
    if (encodeBase64) {
      digest = Base64.decodeBase64(digest);
    }
    try {
      MessageDigest stomach = MessageDigest.getInstance(algorithm);
      stomach.update("content".getBytes());
      assertArrayEquals(stomach.digest(), digest);
    } catch (NoSuchAlgorithmException ex) {
      fail("Invalid algorithm supplied: " + algorithm);
    }
  }

  @Test
  public void testDigestMD5() throws IOException, InterruptedException {
    helpTestDigest("MD5", false);
  }

  @Test
  public void testDigestMD5Base64() throws IOException, InterruptedException {
    helpTestDigest("MD5", true);
  }

  @Test
  public void testDigestSHA1() throws IOException, InterruptedException {
    helpTestDigest("SHA-1", false);
  }

  @Test
  public void testDigestSHA1Base64() throws IOException, InterruptedException {
    helpTestDigest("SHA-1", true);
  }

  @Test
  public void testDigestSHA512() throws IOException, InterruptedException {
    helpTestDigest("SHA-512", false);
  }

  @Test
  public void testDigestSHA512Base64() throws IOException, InterruptedException {
    helpTestDigest("SHA-512", true);
  }

  /**
   * Successful if no exceptions thrown.
   */
  @Test
  public void testDigestBuilder() throws IOException, FlumeSpecException, InterruptedException {
    EventSink snk =
        new CompositeSink(new Context(),
            "{ digest(\"MD5\", \"digest\") => counter(\"count\") }");
    snk.open();
    Event e = new EventImpl("content".getBytes());
    snk.append(e);
    snk.close();
  }

  /**
   * Successful if no exceptions thrown.
   */
  @Test
  public void testDigestBuilderBase64() throws IOException, FlumeSpecException, InterruptedException {
    EventSink snk =
        new CompositeSink(new Context(),
            "{ digest(\"MD5\", \"digest\", base64=\"true\") => counter(\"count\") }");
    snk.open();
    Event e = new EventImpl("content".getBytes());
    snk.append(e);
    snk.close();
  }

  /**
   * Successful if no exceptions thrown.
   */
  @Test(expected=FlumeSpecException.class)
  public void testDigestBuilderInvalidAlgorithm() throws IOException, FlumeSpecException, InterruptedException {
    EventSink snk =
        new CompositeSink(new Context(),
            "{ digest(\"INVALID\", \"digest\") => counter(\"count\") }");
  }
}
