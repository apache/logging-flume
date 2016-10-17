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
package org.apache.flume.channel.file;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

public class LogUtils {

  private static final Pattern pattern =
          Pattern.compile("^" + Log.PREFIX + "\\d+$");

  /**
   * Sort a list of files by the number after Log.PREFIX.
   */
  static void sort(List<File> logs) {
    Collections.sort(logs, new Comparator<File>() {
      @Override
      public int compare(File file1, File file2) {
        int id1 = getIDForFile(file1);
        int id2 = getIDForFile(file2);
        if (id1 > id2) {
          return 1;
        } else if (id1 == id2) {
          return 0;
        }
        return -1;
      }
    });
  }
  /**
   * Get the id after the Log.PREFIX
   */
  static int getIDForFile(File file) {
    return Integer.parseInt(file.getName().substring(Log.PREFIX.length()));
  }
  /**
   * Find all log files within a directory
   *
   * @param logDir directory to search
   * @return List of data files within logDir
   */
  static List<File> getLogs(File logDir) {
    List<File> result = Lists.newArrayList();
    File[] files = logDir.listFiles();
    if (files == null) {
      String msg = logDir + ".listFiles() returned null: ";
      msg += "File = " + logDir.isFile() + ", ";
      msg += "Exists = " + logDir.exists() + ", ";
      msg += "Writable = " + logDir.canWrite();
      throw new IllegalStateException(msg);
    }
    for (File file : files) {
      String name = file.getName();
      if (pattern.matcher(name).matches()) {
        result.add(file);
      }
    }
    return result;
  }
}
