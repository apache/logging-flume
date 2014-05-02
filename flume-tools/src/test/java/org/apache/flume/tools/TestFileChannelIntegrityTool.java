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
package org.apache.flume.tools;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.channel.file.Log;
import org.apache.flume.channel.file.LogFile;
import org.apache.flume.channel.file.LogFileV3;
import org.apache.flume.channel.file.LogRecord;
import org.apache.flume.channel.file.Serialization;
import org.apache.flume.channel.file.WriteOrderOracle;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.AfterClass;
import static org.fest.reflect.core.Reflection.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;


public class TestFileChannelIntegrityTool {
  private static File baseDir;
  private static File origCheckpointDir;
  private static File origDataDir;
  private static Event event;
  private static Context ctx;

  private File checkpointDir;
  private File dataDir;


  @BeforeClass
  public static void setUpClass() throws Exception{
    createDataFiles();
  }

  @Before
  public void setUp() throws Exception {
    checkpointDir = new File(baseDir, "checkpoint");
    dataDir = new File(baseDir, "dataDir");
    Assert.assertTrue(checkpointDir.mkdirs() || checkpointDir.isDirectory());
    Assert.assertTrue(dataDir.mkdirs() || dataDir.isDirectory());
    File[] dataFiles = origDataDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if(name.contains("lock")) {
          return false;
        }
        return true;
      }
    });
    for(File dataFile : dataFiles) {
      Serialization.copyFile(dataFile, new File(dataDir, dataFile.getName()));
    }
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(checkpointDir);
    FileUtils.deleteDirectory(dataDir);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    FileUtils.deleteDirectory(origCheckpointDir);
    FileUtils.deleteDirectory(origDataDir);
  }

  @Test
  public void testFixCorruptRecords() throws Exception {
    doTestFixCorruptEvents(false);
  }
  @Test
  public void testFixCorruptRecordsWithCheckpoint() throws Exception {
    doTestFixCorruptEvents(true);
  }

  public void doTestFixCorruptEvents(boolean withCheckpoint) throws Exception {
    Set<String> corruptFiles = new HashSet<String>();
    File[] files = dataDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if(name.contains("lock") || name.contains("meta")) {
          return false;
        }
        return true;
      }
    });
    Random random = new Random();
    int corrupted = 0;
    for (File dataFile : files) {
      LogFile.SequentialReader reader =
        new LogFileV3.SequentialReader(dataFile, null, true);
      RandomAccessFile handle = new RandomAccessFile(dataFile, "rw");
      long eventPosition1 = reader.getPosition();
      LogRecord rec = reader.next();
      //No point corrupting commits, so ignore them
      if(rec == null ||
        rec.getEvent().getClass().getName().
          equals("org.apache.flume.channel.file.Commit")) {
        handle.close();
        reader.close();
        continue;
      }
      long eventPosition2 = reader.getPosition();
      rec = reader.next();
      handle.seek(eventPosition1 + 100);
      handle.writeInt(random.nextInt());
      corrupted++;
      corruptFiles.add(dataFile.getName());
      if (rec == null ||
        rec.getEvent().getClass().getName().
          equals("org.apache.flume.channel.file.Commit")) {
        handle.close();
        reader.close();
        continue;
      }
      handle.seek(eventPosition2 + 100);
      handle.writeInt(random.nextInt());
      corrupted++;
      handle.close();
      reader.close();

    }
    FileChannelIntegrityTool tool = new FileChannelIntegrityTool();
    tool.run(new String[] {"-l", dataDir.toString()});
    FileChannel channel = new FileChannel();
    channel.setName("channel");
    String cp;
    if(withCheckpoint) {
      cp = origCheckpointDir.toString();
    } else {
      FileUtils.deleteDirectory(checkpointDir);
      Assert.assertTrue(checkpointDir.mkdirs());
      cp = checkpointDir.toString();
    }
    ctx.put(FileChannelConfiguration.CHECKPOINT_DIR,cp);
    ctx.put(FileChannelConfiguration.DATA_DIRS, dataDir.toString());
    channel.configure(ctx);
    channel.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    int i = 0;
    while(channel.take() != null) {
      i++;
    }
    tx.commit();
    tx.close();
    channel.stop();
    Assert.assertEquals(25 - corrupted, i);
    files = dataDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        if(name.contains(".bak")) {
          return true;
        }
        return false;
      }
    });
    Assert.assertEquals(corruptFiles.size(), files.length);
    for(File file : files) {
      String name = file.getName();
      name = name.replaceAll(".bak", "");
      Assert.assertTrue(corruptFiles.remove(name));
    }
    Assert.assertTrue(corruptFiles.isEmpty());
  }

  private static void createDataFiles() throws Exception {
    final byte[] eventData = new byte[2000];
    for(int i = 0; i < 2000; i++) {
      eventData[i] = 1;
    }
    WriteOrderOracle.setSeed(System.currentTimeMillis());
    event = EventBuilder.withBody(eventData);
    baseDir = Files.createTempDir();
    if(baseDir.exists()) {
      FileUtils.deleteDirectory(baseDir);
    }
    baseDir = Files.createTempDir();
    origCheckpointDir = new File(baseDir, "chkpt");
    Assert.assertTrue(origCheckpointDir.mkdirs() || origCheckpointDir.isDirectory());
    origDataDir = new File(baseDir, "data");
    Assert.assertTrue(origDataDir.mkdirs() || origDataDir.isDirectory());
    FileChannel channel = new FileChannel();
    channel.setName("channel");
    ctx = new Context();
    ctx.put(FileChannelConfiguration.CAPACITY, "1000");
    ctx.put(FileChannelConfiguration.CHECKPOINT_DIR, origCheckpointDir.toString());
    ctx.put(FileChannelConfiguration.DATA_DIRS, origDataDir.toString());
    ctx.put(FileChannelConfiguration.MAX_FILE_SIZE, "10000");
    ctx.put(FileChannelConfiguration.TRANSACTION_CAPACITY, "100");
    channel.configure(ctx);
    channel.start();
    for (int j = 0; j < 5; j++) {
      Transaction tx = channel.getTransaction();
      tx.begin();
      for (int i = 0; i < 5; i++) {
        channel.put(event);
      }
      tx.commit();
      tx.close();
    }
    Log log = field("log")
      .ofType(Log.class)
      .in(channel)
      .get();

    Assert.assertTrue("writeCheckpoint returned false",
      method("writeCheckpoint")
        .withReturnType(Boolean.class)
        .withParameterTypes(Boolean.class)
        .in(log)
        .invoke(true));
    channel.stop();
  }
}
