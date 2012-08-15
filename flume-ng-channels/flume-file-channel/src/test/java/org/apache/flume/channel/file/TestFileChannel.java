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
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

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
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestFileChannel {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestFileChannel.class);

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
    channel = createFileChannel();
  }
  @After
  public void teardown() {
    if(channel != null && channel.isOpen()) {
      channel.stop();
    }
    FileUtils.deleteQuietly(baseDir);
  }
  private Context createContext() {
    return createContext(new HashMap<String, String>());
  }
  private Context createContext(Map<String, String> overrides) {
    Context context = new Context();
    context.put(FileChannelConfiguration.CHECKPOINT_DIR,
        checkpointDir.getAbsolutePath());
    context.put(FileChannelConfiguration.DATA_DIRS, dataDir);
    context.put(FileChannelConfiguration.CAPACITY, String.valueOf(10000));
    // Set checkpoint for 5 seconds otherwise test will run out of memory
    context.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, "5000");
    context.putAll(overrides);
    return context;
  }
  private FileChannel createFileChannel() {
    return createFileChannel(new HashMap<String, String>());
  }
  private FileChannel createFileChannel(Map<String, String> overrides) {
    FileChannel channel = new FileChannel();
    channel.setName("FileChannel-" + UUID.randomUUID());
    Context context = createContext(overrides);
    Configurables.configure(channel, context);
    return channel;
  }
  @Test
  public void testRestartLogReplayV1() throws Exception {
    doTestRestart(true);
  }
  @Test
  public void testRestartLogReplayV2() throws Exception {
    doTestRestart(false);
  }
  public void doTestRestart(boolean useLogReplayV1) throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.USE_LOG_REPLAY_V1,
        String.valueOf(useLogReplayV1));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    List<String> in = Lists.newArrayList();
    try {
      while(true) {
        in.addAll(putEvents(channel, "restart", 1, 1));
      }
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="
          +channel.getName()+"]", e.getMessage());
    }
    channel.stop();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    List<String> out = takeEvents(channel, 1, Integer.MAX_VALUE);
    Collections.sort(in);
    Collections.sort(out);
    if(!out.equals(in)) {
      List<String> difference = new ArrayList<String>();
      if(in.size() > out.size()) {
        LOG.info("The channel shorted us");
        difference.addAll(in);
        difference.removeAll(out);
      } else {
        LOG.info("We got more events than expected, perhaps dups");
        difference.addAll(out);
        difference.removeAll(in);
      }
      LOG.error("difference = " + difference +
          ", in.size = " + in.size() + ", out.size = " + out.size());
      Assert.fail();
    }
  }
  @Test
  public void testReconfigure() throws Exception {
    channel.start();
    Assert.assertTrue(channel.isOpen());
    List<String> in = Lists.newArrayList();
    try {
      while(true) {
        in.addAll(putEvents(channel, "reconfig", 1, 1));
      }
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="
          +channel.getName()+"]", e.getMessage());
    }
    Configurables.configure(channel, createContext());
    List<String> out = takeEvents(channel, 1, Integer.MAX_VALUE);
    Collections.sort(in);
    Collections.sort(out);
    if(!out.equals(in)) {
      List<String> difference = new ArrayList<String>();
      if(in.size() > out.size()) {
        LOG.info("The channel shorted us");
        difference.addAll(in);
        difference.removeAll(out);
      } else {
        LOG.info("We got more events than expected, perhaps dups");
        difference.addAll(out);
        difference.removeAll(in);
      }
      LOG.error("difference = " + difference +
          ", in.size = " + in.size() + ", out.size = " + out.size());
      Assert.fail();
    }
  }
  @Test
  public void testPut() throws Exception {
    channel.start();
    Assert.assertTrue(channel.isOpen());
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
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Transaction transaction;
    transaction = channel.getTransaction();
    transaction.begin();
    transaction.rollback();
    transaction.close();

    // ensure we can reopen log with no error
    channel.stop();
    channel = createFileChannel();
    channel.start();
    Assert.assertTrue(channel.isOpen());
    transaction = channel.getTransaction();
    transaction.begin();
    Assert.assertNull(channel.take());
    transaction.commit();
    transaction.close();
  }
  @Test
  public void testCommitAfterNoPutTake() throws Exception {
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Transaction transaction;
    transaction = channel.getTransaction();
    transaction.begin();
    transaction.commit();
    transaction.close();

    // ensure we can reopen log with no error
    channel.stop();
    channel = createFileChannel();
    channel.start();
    Assert.assertTrue(channel.isOpen());
    transaction = channel.getTransaction();
    transaction.begin();
    Assert.assertNull(channel.take());
    transaction.commit();
    transaction.close();
  }
  @Test
  public void testCapacity() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(5));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    try {
      putEvents(channel, "fillup", 1, Integer.MAX_VALUE);
      Assert.fail();
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="+channel.getName()+"]",
          e.getMessage());
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
      Assert.fail();
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="+channel.getName()+"]",
          e.getMessage());
    }
    // ensure we the events back
    Assert.assertEquals(5, takeEvents(channel, 1, 5).size());
  }
  /**
   * This test is here to make sure we can replay a full queue
   * when we have a PUT with a lower txid than the take which
   * made that PUT possible. Here we fill up the queue so
   * puts will block. Start the put (which assigns a txid)
   * and while it's blocking initiate a take. That will
   * allow the PUT to take place but at a lower txid
   * than the take and additionally with pre-FLUME-1432 with
   * the same timestamp. After FLUME-1432 the PUT will have a
   * lower txid but a higher write order id and we can see
   * which event occurred first.
   */
  @Test
  public void testRaceFoundInFLUME1432() throws Exception {
    // the idea here is we will fill up the channel
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.KEEP_ALIVE, String.valueOf(10L));
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(10));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    try {
      putEvents(channel, "fillup", 1, Integer.MAX_VALUE);
      Assert.fail();
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="+channel.getName()+"]",
          e.getMessage());
    }
    // then do a put which will block but it will be assigned a tx id
    Future<String> put = Executors.newSingleThreadExecutor()
        .submit(new Callable<String>() {
      @Override
      public String call() throws Exception {
        List<String> result = putEvents(channel, "blocked-put", 1, 1);
        Assert.assertTrue(result.toString(), result.size() == 1);
        return result.remove(0);
      }
    });
    Thread.sleep(1000L); // ensure the put has started and is blocked
    // after which we do a take, will have a tx id after the put
    List<String> result = takeEvents(channel, 1, 1);
    Assert.assertTrue(result.toString(), result.size() == 1);
    String putmsg = put.get();
    Assert.assertNotNull(putmsg);
    String takemsg = result.remove(0);
    Assert.assertNotNull(takemsg);
    LOG.info("Got: put " + putmsg + ", take " + takemsg);
    channel.stop();
    channel = createFileChannel(overrides);
    // now when we replay, the transaction the put will be ordered
    // before the take when we used the txid as an order of operations
    channel.start();
    Assert.assertTrue(channel.isOpen());
  }
  @Test
  public void testRollbackSimulatedCrash() throws Exception {
    channel.start();
    Assert.assertTrue(channel.isOpen());
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
    channel.start();
    Assert.assertTrue(channel.isOpen());

    // we should not get the rolled back item
    List<String> out = takeEvents(channel, 1, numEvents);
    Collections.sort(in);
    Collections.sort(out);
    Assert.assertEquals(in, out);
  }
  @Test
  public void testRollbackSimulatedCrashWithSink() throws Exception {
    channel.start();
    Assert.assertTrue(channel.isOpen());
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
    channel.start();
    Assert.assertTrue(channel.isOpen());
    List<String> out = takeEvents(channel, 1, 1);
    Assert.assertEquals(1, out.size());
    String s = out.get(0);
    Assert.assertTrue(s, s.startsWith("rollback-90-9"));
  }
  @Test
  public void testThreaded() throws IOException, InterruptedException {
    channel.start();
    Assert.assertTrue(channel.isOpen());
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
    channel.start();
    Assert.assertTrue(channel.isOpen());
    FileChannel fileChannel = createFileChannel();
    fileChannel.start();
    Assert.assertTrue(!fileChannel.isOpen());
  }
  @Test
  public void testIntegration() throws IOException, InterruptedException {
    // set shorter checkpoint and filesize to ensure
    // checkpoints and rolls occur during the test
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CHECKPOINT_INTERVAL,
        String.valueOf(10L * 1000L));
    overrides.put(FileChannelConfiguration.MAX_FILE_SIZE,
        String.valueOf(1024 * 1024 * 5));
    // do reconfiguration
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());

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
  /**
   * This is regression test with files generated by a file channel
   * with the FLUME-1432 patch.
   */
  @Test
  public void testFileFormatV2postFLUME1432()
      throws Exception {
    copyDecompressed("fileformat-v2-checkpoint.gz",
        new File(checkpointDir, "checkpoint"));
    for (int i = 0; i < dataDirs.length; i++) {
      int fileIndex = i + 1;
      copyDecompressed("fileformat-v2-log-"+fileIndex+".gz",
          new File(dataDirs[i], "log-" + fileIndex));
    }
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(10));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    List<String> events = takeEvents(channel, 1);
    List<String> expected = Arrays.asList(new String[] {
        "2684", "2685", "2686", "2687", "2688", "2689", "2690", "2691"
        }
    );
    Assert.assertEquals(expected, events);
  }
  /**
   * This is a regression test with files generated by a file channel
   * without the FLUME-1432 patch.
   */
  @Test
  public void testFileFormatV2PreFLUME1432LogReplayV1()
      throws Exception {
    doTestFileFormatV2PreFLUME1432(true);
  }
  @Test
  public void testFileFormatV2PreFLUME1432LogReplayV2()
      throws Exception {
    doTestFileFormatV2PreFLUME1432(false);
  }
  public void doTestFileFormatV2PreFLUME1432(boolean useLogReplayV1)
      throws Exception {
    copyDecompressed("fileformat-v2-pre-FLUME-1432-checkpoint.gz",
        new File(checkpointDir, "checkpoint"));
    for (int i = 0; i < dataDirs.length; i++) {
      int fileIndex = i + 1;
      copyDecompressed("fileformat-v2-pre-FLUME-1432-log-"+fileIndex+".gz",
          new File(dataDirs[i], "log-" + fileIndex));
    }
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(10000));
    overrides.put(FileChannelConfiguration.USE_LOG_REPLAY_V1,
        String.valueOf(useLogReplayV1));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    List<String> events = takeEvents(channel, 1);
    Assert.assertEquals(50, events.size());
  }

  private static void copyDecompressed(String resource, File output)
      throws IOException {
    URL input =  Resources.getResource(resource);
    long copied = ByteStreams.copy(new GZIPInputStream(input.openStream()),
        new FileOutputStream(output));
    LOG.info("Copied " + copied + " bytes from " + input + " to " + output);
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
          String s = prefix + "-" + i +"-" + j + "-" + UUID.randomUUID();
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
