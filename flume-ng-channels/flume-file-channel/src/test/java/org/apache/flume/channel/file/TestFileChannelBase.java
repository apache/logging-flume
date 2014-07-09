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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.google.common.io.Files;

public class TestFileChannelBase {

  protected FileChannel channel;
  protected File baseDir;
  protected File checkpointDir;
  protected File[] dataDirs;
  protected String dataDir;
  protected File backupDir;
  protected File uncompressedBackupCheckpoint;
  protected File compressedBackupCheckpoint;

  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
    checkpointDir = new File(baseDir, "chkpt");
    backupDir = new File(baseDir, "backup");
    uncompressedBackupCheckpoint = new File(backupDir, "checkpoint");
    compressedBackupCheckpoint = new File(backupDir,
      "checkpoint.snappy");
    Assert.assertTrue(checkpointDir.mkdirs() || checkpointDir.isDirectory());
    Assert.assertTrue(backupDir.mkdirs() || backupDir.isDirectory());
    dataDirs = new File[3];
    dataDir = "";
    for (int i = 0; i < dataDirs.length; i++) {
      dataDirs[i] = new File(baseDir, "data" + (i + 1));
      Assert.assertTrue(dataDirs[i].mkdirs() || dataDirs[i].isDirectory());
      dataDir += dataDirs[i].getAbsolutePath() + ",";
    }
    dataDir = dataDir.substring(0, dataDir.length() - 1);
    channel = createFileChannel();
  }

  @After
  public void teardown() {
    if (channel != null && channel.isOpen()) {
      channel.stop();
    }
    FileUtils.deleteQuietly(baseDir);
  }

  protected Context createContext() {
    return createContext(new HashMap<String, String>());
  }

  protected Context createContext(Map<String, String> overrides) {
    return TestUtils.createFileChannelContext(checkpointDir.getAbsolutePath(),
        dataDir, backupDir.getAbsolutePath(), overrides);
  }

  protected FileChannel createFileChannel() {
    return createFileChannel(new HashMap<String, String>());
  }

  protected FileChannel createFileChannel(Map<String, String> overrides) {
    return TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, backupDir.getAbsolutePath(), overrides);
  }
}
