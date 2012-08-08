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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.LoggerSink;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestFileChannel {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestFileChannel.class);

  private FileChannel channel;
  private File checkpointDir;
  private File[] dataDirs;
  private String dataDir;
  private final Context context = new Context();

  @Before
  public void setup() {
    checkpointDir = Files.createTempDir();
    dataDirs = new File[3];
    dataDir = "";
    for (int i = 0; i < dataDirs.length; i++) {
      dataDirs[i] = Files.createTempDir();
      Assert.assertTrue(dataDirs[i].isDirectory());
      dataDir += dataDirs[i].getAbsolutePath() + ",";
    }
    dataDir = dataDir.substring(0, dataDir.length() - 1);
    channel = createFileChannel();

  }
  private FileChannel createFileChannel() {
    FileChannel channel = new FileChannel();
    channel.setName("fc-" + UUID.randomUUID()); // fixes mbean error
    context.put(FileChannelConfiguration.CHECKPOINT_DIR,
        checkpointDir.getAbsolutePath());
    context.put(FileChannelConfiguration.DATA_DIRS, dataDir);
    context.put(FileChannelConfiguration.CAPACITY, String.valueOf(10000));
    // Set checkpoint for 5 seconds otherwise test will run out of memory
    context.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "5000");
    Configurables.configure(channel, context);
    channel.start();
    return channel;
  }

  @After
  public void teardown() {
    if(channel != null) {
      channel.stop();
    }
    FileUtils.deleteQuietly(checkpointDir);
    for (int i = 0; i < dataDirs.length; i++) {
      FileUtils.deleteQuietly(dataDirs[i]);
    }
  }
  @Test
  public void testRestart() throws Exception {
    List<String> in = Lists.newArrayList();
    try {
      while(true) {
        in.addAll(putEvents(channel, "restart", 1, 1));
      }
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="+channel.getName()+"]",
          e.getMessage());
    }
    channel.stop();
    channel = createFileChannel();
    List<String> out = takeEvents(channel, 1, Integer.MAX_VALUE);
    Collections.sort(in);
    Collections.sort(out);
    Assert.assertEquals(in, out);
  }
  @Test
  public void testReconfigure() throws Exception {
    List<String> in = Lists.newArrayList();
    try {
      while(true) {
        in.addAll(putEvents(channel, "restart", 1, 1));
      }
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="+channel.getName()+"]",
          e.getMessage());
    }
    Configurables.configure(channel, context);
    List<String> out = takeEvents(channel, 1, Integer.MAX_VALUE);
    Collections.sort(in);
    Collections.sort(out);
    Assert.assertEquals(in, out);
  }
  @Test
  public void testPut() throws Exception {
    // should find no items
    int found = takeEvents(channel, 1, 5).size();
    Assert.assertEquals(0, found);
    List<String> expected = Lists.newArrayList();
    expected.addAll(putEvents(channel, "unbatched", 1, 5));
    expected.addAll(putEvents(channel, "batched", 5, 5));
    List<String> actual = takeEvents(channel, 1);
    Collections.sort(actual);
    Collections.sort(expected);
    Assert.assertEquals(expected, actual);
  }
  @Test
  public void testRollbackAfterNoPutTake() throws Exception {
    Transaction transaction;
    transaction = channel.getTransaction();
    transaction.begin();
    transaction.rollback();
    transaction.close();

    // ensure we can reopen log with no error
    channel.stop();
    channel = createFileChannel();
    transaction = channel.getTransaction();
    transaction.begin();
    Assert.assertNull(channel.take());
    transaction.commit();
    transaction.close();
  }
  @Test
  public void testCommitAfterNoPutTake() throws Exception {
    Transaction transaction;
    transaction = channel.getTransaction();
    transaction.begin();
    transaction.commit();
    transaction.close();

    // ensure we can reopen log with no error
    channel.stop();
    channel = createFileChannel();
    transaction = channel.getTransaction();
    transaction.begin();
    Assert.assertNull(channel.take());
    transaction.commit();
    transaction.close();
  }
  @Test
  public void testCapacity() throws Exception {
    channel.close();
    channel = createFileChannel();
    try {
      putEvents(channel, "capacity", 1, 6);
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity", e.getMessage());
    }
    // take an event, roll it back, and
    // then make sure a put fails
    Transaction transaction;
    transaction = channel.getTransaction();
    transaction.begin();
    Event event = channel.take();
    Assert.assertNotNull(event);
    transaction.rollback();
    transaction.close();
    // ensure the take the didn't change the state of the capacity
    try {
      putEvents(channel, "capacity", 1, 1);
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity", e.getMessage());
    }
    // ensure we the events back
    Assert.assertEquals(5, takeEvents(channel, 1, 5).size());
  }
  @Test
  public void testRollbackSimulatedCrash() throws Exception {
    int numEvents = 50;
    List<String> in = putEvents(channel, "rollback", 1, numEvents);

    Transaction transaction;
    // put an item we will rollback
    transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("rolled back".getBytes(Charsets.UTF_8)));
    transaction.rollback();
    transaction.close();

    // simulate crash
    channel.stop();
    channel = createFileChannel();

    // we should not get the rolled back item
    List<String> out = takeEvents(channel, 1, numEvents);
    Collections.sort(in);
    Collections.sort(out);
    Assert.assertEquals(in, out);
  }
  @Test
  public void testRollbackSimulatedCrashWithSink() throws Exception {
    int numEvents = 100;

    LoggerSink sink = new LoggerSink();
    sink.setChannel(channel);
    // sink will leave one item
    CountingSinkRunner runner = new CountingSinkRunner(sink, numEvents - 1);
    runner.start();
    putEvents(channel, "rollback", 10, numEvents);

    Transaction transaction;
    // put an item we will rollback
    transaction = channel.getTransaction();
    transaction.begin();
    byte[] bytes = "rolled back".getBytes(Charsets.UTF_8);
    channel.put(EventBuilder.withBody(bytes));
    transaction.rollback();
    transaction.close();

    while(runner.isAlive()) {
      Thread.sleep(10L);
    }
    Assert.assertEquals(numEvents - 1, runner.getCount());
    for(Exception ex : runner.getErrors()) {
      LOG.warn("Sink had error", ex);
    }
    Assert.assertEquals(Collections.EMPTY_LIST, runner.getErrors());

    // simulate crash
    channel.stop();
    channel = createFileChannel();

    List<String> out = takeEvents(channel, 1, 1);
    Assert.assertEquals(1, out.size());
    Assert.assertEquals("rollback-90-9", out.get(0));
  }
  @Test
  public void testThreaded() throws IOException, InterruptedException {
    int numThreads = 10;
    final CountDownLatch producerStopLatch = new CountDownLatch(numThreads);
    final CountDownLatch consumerStopLatch = new CountDownLatch(numThreads);
    final List<Exception> errors = Collections
        .synchronizedList(new ArrayList<Exception>());
    final List<String> expected = Collections
        .synchronizedList(new ArrayList<String>());
    final List<String> actual = Collections
        .synchronizedList(new ArrayList<String>());
    for (int i = 0; i < numThreads; i++) {
      final int id = i;
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            if (id % 2 == 0) {
              expected.addAll(putEvents(channel, Integer.toString(id), 1, 5));
            } else {
              expected.addAll(putEvents(channel, Integer.toString(id), 5, 5));
            }
            LOG.info("Completed some puts " + expected.size());
          } catch (Exception e) {
            LOG.error("Error doing puts", e);
            errors.add(e);
          } finally {
            producerStopLatch.countDown();
          }
        }
      };
      t.setDaemon(true);
      t.start();
    }
    for (int i = 0; i < numThreads; i++) {
      final int id = i;
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            while(!producerStopLatch.await(1, TimeUnit.SECONDS) ||
                expected.size() > actual.size()) {
              if (id % 2 == 0) {
                actual.addAll(takeEvents(channel, 1, Integer.MAX_VALUE));
              } else {
                actual.addAll(takeEvents(channel, 5, Integer.MAX_VALUE));
              }
            }
            if(actual.isEmpty()) {
              LOG.error("Found nothing!");
            } else {
              LOG.info("Completed some takes " + actual.size());
            }
          } catch (Exception e) {
            LOG.error("Error doing takes", e);
            errors.add(e);
          } finally {
            consumerStopLatch.countDown();
          }
        }
      };
      t.setDaemon(true);
      t.start();
    }
    Assert.assertTrue("Timed out waiting for producers",
        producerStopLatch.await(30, TimeUnit.SECONDS));
    Assert.assertTrue("Timed out waiting for consumer",
        consumerStopLatch.await(30, TimeUnit.SECONDS));
    Assert.assertEquals(Collections.EMPTY_LIST, errors);
    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }
  @Test
  public void testLocking() throws IOException {
    FileChannel fc = createFileChannel();
    Assert.assertTrue(!fc.isOpen());
  }
  @Test
  public void testIntegration() throws IOException, InterruptedException {
    // set shorter checkpoint and filesize to ensure
    // checkpoints and rolls occur during the test
    context.put(FileChannelConfiguration.CHECKPOINT_INTERVAL,
        String.valueOf(10L * 1000L));
    context.put(FileChannelConfiguration.MAX_FILE_SIZE,
        String.valueOf(1024 * 1024 * 5));
    // do reconfiguration
    Configurables.configure(channel, context);

    SequenceGeneratorSource source = new SequenceGeneratorSource();
    CountingSourceRunner sourceRunner = new CountingSourceRunner(source, channel);

    NullSink sink = new NullSink();
    sink.setChannel(channel);
    CountingSinkRunner sinkRunner = new CountingSinkRunner(sink);

    sinkRunner.start();
    sourceRunner.start();
    Thread.sleep(TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES));
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
    Thread.sleep(TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS));
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
  private static List<String> takeEvents(Channel channel,
      int batchSize) throws Exception {
    return takeEvents(channel, batchSize, Integer.MAX_VALUE);
  }
  private static List<String> takeEvents(Channel channel,
      int batchSize, int numEvents) throws Exception {
    List<String> result = Lists.newArrayList();
    for (int i = 0; i < numEvents; i += batchSize) {
      for (int j = 0; j < batchSize; j++) {
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
          Event event = channel.take();
          if(event == null) {
            transaction.commit();
            return result;
          }
          result.add(new String(event.getBody(), Charsets.UTF_8));
          transaction.commit();
        } catch (Exception ex) {
          transaction.rollback();
          throw ex;
        } finally {
          transaction.close();
        }
      }
    }
    return result;
  }
  private static List<String> putEvents(Channel channel, String prefix,
      int batchSize, int numEvents) throws Exception {
    List<String> result = Lists.newArrayList();
    for (int i = 0; i < numEvents; i += batchSize) {
      for (int j = 0; j < batchSize; j++) {
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
          String s = prefix + "-" + i +"-" + j;
          Event event = EventBuilder.withBody(s.getBytes(Charsets.UTF_8));
          result.add(s);
          channel.put(event);
          transaction.commit();
        } catch (Exception ex) {
          transaction.rollback();
          throw ex;
        } finally {
          transaction.close();
        }
      }
    }
    return result;
  }
}
