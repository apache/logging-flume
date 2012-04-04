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
package org.apache.flume.sink.hdfs;

import java.io.IOException;
import org.apache.flume.Event;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;

public class MockHDFSWriter implements HDFSWriter {

  private int filesOpened = 0;
  private int bytesWritten = 0;
  private int eventsWritten = 0;

  public int getFilesOpened() {
    return filesOpened;
  }

  public int getBytesWritten() {
    return bytesWritten;
  }

  public int getEventsWritten() {
    return eventsWritten;
  }

  public void clear() {
    filesOpened = 0;
    bytesWritten = 0;
    eventsWritten = 0;
  }

  @Override
  public void open(String filePath, FlumeFormatter fmt) throws IOException {
    filesOpened++;
  }

  @Override
  public void open(String filePath, CompressionCodec codec, CompressionType cType, FlumeFormatter fmt) throws IOException {
    filesOpened++;
  }

  @Override
  public void append(Event e, FlumeFormatter fmt) throws IOException {
    eventsWritten++;
    bytesWritten += e.getBody().length;
  }

  @Override
  public void sync() throws IOException {
    // does nothing
  }

  @Override
  public void close() throws IOException {
    // does nothing
  }

}
