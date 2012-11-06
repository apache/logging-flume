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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * A {@link LineReader} implementation which delegates to a
 * {@link BufferedReader}.
 */
public class BufferedLineReader implements LineReader {
  private BufferedReader reader;

  public BufferedLineReader(Reader in) {
    reader = new BufferedReader(in);
  }

  @Override
  public String readLine() throws IOException {
    return reader.readLine();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public List<String> readLines(int n) throws IOException {
    List<String> out = Lists.newLinkedList();
    String line;
    while((line = readLine()) != null && out.size() < n) {
      out.add(line);
    }
    return out;
  }

  public void mark(int readAheadLimit) throws IOException {
    reader.mark(readAheadLimit);
  }

  public void reset() throws IOException {
    reader.reset();
  }
}
