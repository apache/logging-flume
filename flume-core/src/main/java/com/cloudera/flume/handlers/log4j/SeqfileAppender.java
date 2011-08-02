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
package com.cloudera.flume.handlers.log4j;

import java.io.File;
import java.io.IOException;

import com.cloudera.flume.handlers.hdfs.SeqfileEventSink;

/**
 * This is a extension that allows log4j configuration via log4j.properties
 * files and the class's bean interface (get/setFile, get/setBufferIO).
 */
public class SeqfileAppender extends Log4JAppenderEventSink {

  String path;
  boolean bufferedIO = true;

  /**
   * Default constructor does nothing but is needed for log4jproperties files.
   * Set values and then call activateOptions();
   */
  public SeqfileAppender() {
    super();
    path = null;
  }

  public SeqfileAppender(File path) throws IOException {
    super(new SeqfileEventSink(path));
    this.path = path.getCanonicalPath();
  }

  public void activateOptions() {
    try {
      SeqfileEventSink s = new SeqfileEventSink(new File(path));
      s.setBufferIO(bufferedIO);
      setEventSink(s);
    } catch (IOException e) {
      e.printStackTrace();
      // this will fail, but allow the program to continue.
    }
  }

  public void finalize() {
    super.finalize();
    close();
  }

  // //
  // Getter and setters for log4j.properties configuration (via Java Beans
  // interface)
  public void setFile(String file) {
    this.path = file;
  }

  public String getFile() {
    return path;
  }

  public void setBufferIO(boolean b) {
    bufferedIO = b;
  }

  public boolean getBufferIO() {
    return bufferedIO;
  }

}
