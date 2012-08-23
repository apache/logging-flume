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
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestIntegration {

  private static final Logger LOG = LoggerFactory
          .getLogger(TestIntegration.class);
  private FileChannel channel;
  private File baseDir;
  private File checkpointDir;
  private File[] dataDirs;
  private String dataDir;

  @Before
  public void setup() {
    baseDir = Files.createTempDir();
    checkpointDir = new File(baseDir, "chkpt");
    Assert.assertTrue(checkpointDir.mkdirs() || checkpointDir.isDirectory());
    dataDirs = new File[3];
    dataDir = "";
    for (int i = 0; i < dataDirs.length; i++) {
      dataDirs[i] = new File(baseDir, "data" + (i+1));
      Assert.assertTrue(dataDirs[i].mkdirs() || dataDirs[i].isDirectory());
      dataDir += dataDirs[i].getAbsolutePath() + ",";
    }
    dataDir = dataDir.substring(0, dataDir.length() - 1);
  }
  @After
  public void teardown() {
    if(channel != null && channel.isOpen()) {
      channel.stop();
    }
    FileUtils.deleteQuietly(baseDir);
  }
  @Test
  public void testIntegration() throws IOException, InterruptedException {
    // set shorter checkpoint and filesize to ensure
    // checkpoints and rolls occur during the test
    Context context = new Context();
    context.put(FileChannelConfiguration.CHECKPOINT_DIR,
        checkpointDir.getAbsolutePath());
    context.put(FileChannelConfiguration.DATA_DIRS, dataDir);
    context.put(FileChannelConfiguration.CAPACITY, String.valueOf(10000));
    // Set checkpoint for 5 seconds otherwise test will run out of memory
    context.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "5000");
    context.put(FileChannelConfiguration.MAX_FILE_SIZE,
            String.valueOf(1024 * 1024 * 5));
    // do reconfiguration
    channel = new FileChannel();
    channel.setName("FileChannel-" + UUID.randomUUID());
    Configurables.configure(channel, context);
    channel.start();
    Assert.assertTrue(channel.isOpen());

    SequenceGeneratorSource source = new SequenceGeneratorSource();
    CountingSourceRunner sourceRunner = new CountingSourceRunner(source, channel);

    NullSink sink = new NullSink();
    sink.setChannel(channel);
    CountingSinkRunner sinkRunner = new CountingSinkRunner(sink);

    sinkRunner.start();
    sourceRunner.start();
    TimeUnit.SECONDS.sleep(30);
    // shutdown source
    sourceRunner.shutdown();
    while(sourceRunner.isAlive()) {
      Thread.sleep(10L);
    }
    // wait for queue to clear
    while(channel.getDepth() > 0) {
      Thread.sleep(10L);
    }
    // shutdown size
    sinkRunner.shutdown();
    // wait a few seconds
    TimeUnit.SECONDS.sleep(5);
    List<File> logs = Lists.newArrayList();
    for (int i = 0; i < dataDirs.length; i++) {
      logs.addAll(LogUtils.getLogs(dataDirs[i]));
    }
    LOG.info("Total Number of Logs = " + logs.size());
    for(File logFile : logs) {
      LOG.info("LogFile = " + logFile);
    }
    LOG.info("Source processed " + sinkRunner.getCount());
    LOG.info("Sink processed " + sourceRunner.getCount());
    for(Exception ex : sourceRunner.getErrors()) {
      LOG.warn("Source had error", ex);
    }
    for(Exception ex : sinkRunner.getErrors()) {
      LOG.warn("Sink had error", ex);
    }
    Assert.assertEquals(sinkRunner.getCount(), sinkRunner.getCount());
    Assert.assertEquals(Collections.EMPTY_LIST, sinkRunner.getErrors());
    Assert.assertEquals(Collections.EMPTY_LIST, sourceRunner.getErrors());
  }
}
