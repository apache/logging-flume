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
package com.cloudera.flume.faults;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * This eventually exhausts the drive logs are stored in
 */
public class DiskHog extends ResourceHog {

  File path;

  public DiskHog(File path, int delay, boolean random) {
    super(delay, random);
    this.path = path;
  }

  /**
   * Write a bunch of 100MB files. Eventually will run out of disk space.
   */
  @Override
  public void increment() {
    try {
      File tmp = File.createTempFile("junk", ".tmp", path);
      tmp.deleteOnExit();

      FileOutputStream out = new FileOutputStream(tmp);
      byte[] data = new byte[50 * 1024 * 1024]; // make a 1MB buffer
      // write out 100MB = 2 * 50MB -- (some JVMs default to 64mb heap)
      out.write(data);
      out.write(data);

      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
