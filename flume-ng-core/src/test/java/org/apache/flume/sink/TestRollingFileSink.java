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

package org.apache.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TestRollingFileSink {

  private static final Logger logger = LoggerFactory
      .getLogger(TestRollingFileSink.class);

  private File tmpDir;
  private RollingFileSink sink;

  @Before
  public void setUp() {
    tmpDir = new File("/tmp/flume-rfs-" + System.currentTimeMillis() + "-"
        + Thread.currentThread().getId());

    sink = new RollingFileSink();

    sink.setChannel(new MemoryChannel());

    tmpDir.mkdirs();
  }

  @After
  public void tearDown() {
    tmpDir.delete();
  }

  @Test
  public void testLifecycle() throws InterruptedException, LifecycleException {
    Context context = new Context();

    context.put("sink.directory", tmpDir.getPath());

    Configurables.configure(sink, context);

    sink.start();
    sink.stop();
  }

  @Test
  public void testAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    Context context = new Context();

    context.put("sink.directory", tmpDir.getPath());
    context.put("sink.rollInterval", "1");
    context.put("sink.batchSize", "1");

    Configurables.configure(sink, context);

    Channel channel = new PseudoTxnMemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    for (int i = 0; i < 10; i++) {
      Event event = new SimpleEvent();

      event.setBody(("Test event " + i).getBytes());

      channel.put(event);
      sink.process();

      Thread.sleep(500);
    }

    sink.stop();

    for (String file : sink.getDirectory().list()) {
      BufferedReader reader =
          new BufferedReader(new FileReader(new File(sink.getDirectory(), file)));

      String lastLine = null;
      String currentLine = null;

      while ((currentLine = reader.readLine()) != null) {
        lastLine = currentLine;
      }

      logger.debug("Produced file:{} lastLine:{}", file, lastLine);

      reader.close();
    }
  }

  @Test
  public void testAppend2() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    Context context = new Context();

    context.put("sink.directory", tmpDir.getPath());
    context.put("sink.rollInterval", "0");
    context.put("sink.batchSize", "1");


    Configurables.configure(sink, context);

    Channel channel = new PseudoTxnMemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    for (int i = 0; i < 10; i++) {
      Event event = new SimpleEvent();

      event.setBody(("Test event " + i).getBytes());

      channel.put(event);
      sink.process();

      Thread.sleep(500);
    }

    sink.stop();

    for (String file : sink.getDirectory().list()) {
      BufferedReader reader =
          new BufferedReader(new FileReader(new File(sink.getDirectory(), file)));

      String lastLine = null;
      String currentLine = null;

      while ((currentLine = reader.readLine()) != null) {
        lastLine = currentLine;
        logger.debug("Produced file:{} lastLine:{}", file, lastLine);
      }


      reader.close();
    }
  }

  @Test
  public void testAppend3()
      throws InterruptedException, LifecycleException, EventDeliveryException, IOException {
    File tmpDir = new File("target/tmpLog");
    tmpDir.mkdirs();
    cleanDirectory(tmpDir);
    Context context = new Context();

    context.put("sink.directory", "target/tmpLog");
    context.put("sink.rollInterval", "0");
    context.put("sink.batchSize", "1");
    context.put("sink.pathManager.prefix", "test3-");
    context.put("sink.pathManager.extension", "txt");

    Configurables.configure(sink, context);

    Channel channel = new PseudoTxnMemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    for (int i = 0; i < 10; i++) {
      Event event = new SimpleEvent();

      event.setBody(("Test event " + i).getBytes());

      channel.put(event);
      sink.process();

      Thread.sleep(500);
    }

    sink.stop();

    for (String file : sink.getDirectory().list()) {
      BufferedReader reader =
          new BufferedReader(new FileReader(new File(sink.getDirectory(), file)));

      String lastLine = null;
      String currentLine = null;

      while ((currentLine = reader.readLine()) != null) {
        lastLine = currentLine;
        logger.debug("Produced file:{} lastLine:{}", file, lastLine);
      }

      reader.close();
    }
  }

  @Test
  public void testRollTime()
      throws InterruptedException, LifecycleException, EventDeliveryException, IOException {
    File tmpDir = new File("target/tempLog");
    tmpDir.mkdirs();
    cleanDirectory(tmpDir);
    Context context = new Context();

    context.put("sink.directory", "target/tempLog/");
    context.put("sink.rollInterval", "1");
    context.put("sink.batchSize", "1");
    context.put("sink.pathManager", "rolltime");
    context.put("sink.pathManager.prefix", "test4-");
    context.put("sink.pathManager.extension", "txt");

    Configurables.configure(sink, context);

    Channel channel = new PseudoTxnMemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    for (int i = 0; i < 10; i++) {
      Event event = new SimpleEvent();

      event.setBody(("Test event " + i).getBytes());

      channel.put(event);
      sink.process();

      Thread.sleep(500);
    }

    sink.stop();

    for (String file : sink.getDirectory().list()) {
      BufferedReader reader =
          new BufferedReader(new FileReader(new File(sink.getDirectory(), file)));

      String lastLine = null;
      String currentLine = null;

      while ((currentLine = reader.readLine()) != null) {
        lastLine = currentLine;
        logger.debug("Produced file:{} lastLine:{}", file, lastLine);
      }

      reader.close();
    }
  }

  private void cleanDirectory(File dir) {
    File[] files = dir.listFiles();
    for (File file : files) {
      file.delete();
    }
  }
}
