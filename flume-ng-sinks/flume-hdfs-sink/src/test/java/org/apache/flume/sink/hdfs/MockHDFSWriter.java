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
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;

public class MockHDFSWriter implements HDFSWriter {

  private int filesOpened = 0;
  private int filesClosed = 0;
  private int bytesWritten = 0;
  private int eventsWritten = 0;
  private String filePath = null;

  public int getFilesOpened() {
    return filesOpened;
  }

  public int getFilesClosed() {
    return filesClosed;
  }

  public int getBytesWritten() {
    return bytesWritten;
  }

  public int getEventsWritten() {
    return eventsWritten;
  }

  public String getOpenedFilePath() {
    return filePath;
  }

  public void clear() {
    filesOpened = 0;
    filesClosed = 0;
    bytesWritten = 0;
    eventsWritten = 0;
  }

  public void configure(Context context) {
    // no-op
  }

  public void open(String filePath) throws IOException {
    this.filePath = filePath;
    filesOpened++;
  }

  public void open(String filePath, CompressionCodec codec, CompressionType cType) throws IOException {
    this.filePath = filePath;
    filesOpened++;
  }

  public void append(Event e) throws IOException {
    eventsWritten++;
    bytesWritten += e.getBody().length;
  }

  public void sync() throws IOException {
    // does nothing
  }

  public void close() throws IOException {
    filesClosed++;
  }

  @Override
  public boolean isUnderReplicated() {
    return false;
  }

}
