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

import java.io.IOException;

public class TestRegexLineInputStream extends ResettableInputStream {

  private String str;
  int markPos = 0;
  int curPos = 0;

  /**
   * Warning: This test class does not handle character/byte conversion at all!
   * @param str String to use for testing
   */
  public TestRegexLineInputStream(String str) {
    this.str = str;
  }

  @Override
  public int readChar() throws IOException {
    if (curPos >= str.length()) {
      return -1;
    }
    return str.charAt(curPos++);
  }

  @Override
  public void mark() throws IOException {
    markPos = curPos;
  }

  @Override
  public void reset() throws IOException {
    curPos = markPos;
  }

  @Override
  public void seek(long newPos) throws IOException {
    curPos = Integer.valueOf(newPos + "");
  }

  @Override
  public long tell() throws IOException {
    return curPos;
  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException("This test class doesn't return " +
        "bytes!");
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException("This test class doesn't return " +
        "bytes!");
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
