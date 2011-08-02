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
import java.util.ArrayList;
import java.util.List;

/**
 * This consumes file handler until it exhausts the # of handlers the os
 * provides.
 */
public class FileHandleHog extends ResourceHog {

  public FileHandleHog(int delay, boolean random) {
    super(delay, random);
  }

  List<FileOutputStream> fos = new ArrayList<FileOutputStream>();

  /**
   * Open a lot of files and don't close them. Eventually will run out of
   * handles.
   */
  @Override
  public void increment() {
    File tmp;

    for (int i = 0; i < 150; i++) {
      try {
        tmp = File.createTempFile("junk", ".tmp");
        tmp.deleteOnExit();
        // prevent the handles from being GC'ed/finalized
        fos.add(new FileOutputStream(tmp));
      } catch (IOException e) {
        // this is inside the loop because I want it to keep opening more file
        // handles.
        e.printStackTrace();
      }
    }

  }

}
