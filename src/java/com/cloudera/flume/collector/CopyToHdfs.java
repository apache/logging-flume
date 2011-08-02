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
package com.cloudera.flume.collector;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class CopyToHdfs {
  public static void main(String[] argv) throws IOException {
    if (argv.length < 2) {
      System.out.println("Need to specify arguments <src> <dst>");
      System.exit(-1);
    }
    Configuration conf = new Configuration();

    Path srcPath = new Path(argv[0]);
    FileSystem srcFs = srcPath.getFileSystem(conf);

    Path dstPath = new Path(argv[1]);
    FileSystem dstFs = dstPath.getFileSystem(conf);

    // dfs.copyFromLocalFile(false, psrc, pdst);
    FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, conf);

  }
}
