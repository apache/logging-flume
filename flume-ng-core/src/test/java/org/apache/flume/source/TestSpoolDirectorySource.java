/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestSpoolDirectorySource {
  static SpoolDirectorySource source;
  static MemoryChannel channel;
  private File tmpDir;

  @Before
  public void setUp() {
    source = new SpoolDirectorySource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
    tmpDir = Files.createTempDir();
  }

  @After
  public void tearDown() {
    for (File f : tmpDir.listFiles()) {
      f.delete();
    }
    tmpDir.delete();
  }

  @Test
  public void testPutFilenameHeader() throws IOException, InterruptedException {
    Context context = new Context();
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());
    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER,
        "true");
    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER_KEY,
        "fileHeaderKeyTest");

    Configurables.configure(source, context);
    source.start();
    Thread.sleep(500);
    Transaction txn = channel.getTransaction();
    txn.begin();
    Event e = channel.take();
    Assert.assertNotNull("Event must not be null", e);
    Assert.assertNotNull("Event headers must not be null", e.getHeaders());
    Assert.assertNotNull(e.getHeaders().get("fileHeaderKeyTest"));
    Assert.assertEquals(f1.getAbsolutePath(),
        e.getHeaders().get("fileHeaderKeyTest"));
    txn.commit();
    txn.close();
  }

  @Test
  public void testLifecycle() throws IOException, InterruptedException {
    Context context = new Context();
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());

    Configurables.configure(source, context);

    for (int i = 0; i < 10; i++) {
      source.start();

      Assert
          .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
              source, LifecycleState.START_OR_ERROR));
      Assert.assertEquals("Server is started", LifecycleState.START,
          source.getLifecycleState());

      source.stop();
      Assert.assertTrue("Reached stop or error",
          LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
      Assert.assertEquals("Server is stopped", LifecycleState.STOP,
          source.getLifecycleState());
    }
  }
}
