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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.google.common.base.Preconditions;

/**
 * File system utility functions.
 */
public class FileUtil {

  /**
   * Makes a unique temporary directory. It is the responsibility of the caller
   * to remove this directory.
   */
  public static File mktempdir() throws IOException {
    // setup a directory with a bad entry
    File tmpdir = File.createTempFile("dir", ".tmp");
    boolean status = tmpdir.delete();
    if (!status) {
      throw new IOException("failed to delete tmp file");
    }
    status = tmpdir.mkdir();
    if (!status) {
      throw new IOException("failed to made tmp dir " + tmpdir);
    }
    return tmpdir;
  }

  /**
   * rm -r equivalent -- remove file/dir recursively.
   * 
   * Note that this function is extremely simplistic, and will traverse symlink
   * dirs, and happily delete its contents.
   * 
   * TODO (jon) make it more like 'rm -r' unix behavoir:
   * http://joust.kano.net/weblog/archives/000071.html
   */
  static public boolean rmr(File path) throws IOException {
    Preconditions.checkArgument(!path.getCanonicalPath().equals("/"));

    if (path.exists() && path.isDirectory()) {
      File[] files = path.listFiles();
      for (int i = 0; i < files.length; i++) {
        rmr(files[i]);
      }
    }
    return path.delete();
  }

  /**
   * Copies a file.
   */
  public static void dumbfilecopy(File source, File dest) throws IOException {
    FileOutputStream out = new FileOutputStream(dest);
    FileInputStream in = new FileInputStream(source);
    byte[] buf = new byte[(int) source.length()];
    in.read(buf);
    out.write(buf);
    in.close();
    out.close();
  }
  
  /**
   * Make a directory and its parents, returning true if and only if the dir
   * exists.
   * Does not guarantee that we can write to that directory!
   */
  public static boolean makeDirs(File f) {
    if (f.exists()) {
      if (!f.isDirectory()) {
        return false;
      }
      return true;
    }
    return f.mkdirs();
  }
}
