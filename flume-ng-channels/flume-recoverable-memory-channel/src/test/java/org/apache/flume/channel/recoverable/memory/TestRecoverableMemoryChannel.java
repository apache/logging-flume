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

package org.apache.flume.channel.recoverable.memory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.recoverable.memory.RecoverableMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.NullSink;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestRecoverableMemoryChannel {

  private static final Logger logger = LoggerFactory
  .getLogger(TestRecoverableMemoryChannel.class);

  private RecoverableMemoryChannel channel;
  Context context;

  private File dataDir;

  @Before
  public void setUp() {
    dataDir = Files.createTempDir();
    Assert.assertTrue(dataDir.isDirectory());
    channel = createFileChannel();

  }

  private RecoverableMemoryChannel createFileChannel() {
    RecoverableMemoryChannel channel = new RecoverableMemoryChannel();
    context = new Context();
    context.put(RecoverableMemoryChannel.WAL_DATA_DIR, dataDir.getAbsolutePath());
    Configurables.configure(channel, context);
    channel.start();
    return channel;
  }

  @After
  public void teardown() {
    FileUtils.deleteQuietly(dataDir);
  }
  @Test
  public void testRestart() throws Exception {
    List<String> in = Lists.newArrayList();
    try {
      while(true) {
        in.addAll(putEvents(channel, "restart", 1, 1));
      }
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity", e.getMessage());
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
      Assert.assertEquals("Cannot acquire capacity", e.getMessage());
    }
    Configurables.configure(channel, context);
    List<String> out = takeEvents(channel, 1, Integer.MAX_VALUE);
    Collections.sort(in);
    Collections.sort(out);
    Assert.assertEquals(in, out);
  }
  @Test
  public void testRollbackWithSink() throws Exception {
    final NullSink nullSink = new NullSink();
    Context ctx = new Context();
    ctx.put("batchSize", "1");
    nullSink.configure(ctx);
    nullSink.setChannel(channel);
    final int numItems = 99;
    Thread t = new Thread() {
      @Override
      public void run() {
        int count = 0;
        while(count++ < numItems) {
          try {
            nullSink.process();
            Thread.sleep(1);
          } catch(EventDeliveryException e) {
            break;
          } catch (Exception e) {
            Throwables.propagate(e);
          }
        }
      }
    };
    t.setDaemon(true);
    t.setName("NullSink");
    t.start();

    putEvents(channel, "rollback", 10, 100);

    Transaction transaction;
    // put an item we will rollback
    transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("this is going to be rolledback".getBytes(Charsets.UTF_8)));
    transaction.rollback();
    transaction.close();

    while(t.isAlive()) {
      Thread.sleep(1);
    }


    // simulate crash
    channel.stop();
    channel = createFileChannel();

    // get the item which was not rolled back
    transaction = channel.getTransaction();
    transaction.begin();
    Event event = channel.take();
    transaction.commit();
    transaction.close();
    Assert.assertNotNull(event);
    Assert.assertEquals("rollback-90-9", new String(event.getBody(), Charsets.UTF_8));
  }


  @Test
  public void testRollback() throws Exception {
    // put an item and commit
    putEvents(channel, "rollback", 1, 50);

    Transaction transaction;
    // put an item we will rollback
    transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody("this is going to be rolledback".getBytes(Charsets.UTF_8)));
    transaction.rollback();
    transaction.close();

    // simulate crash
    channel.stop();
    channel = createFileChannel();

    // get the item which was not rolled back
    transaction = channel.getTransaction();
    transaction.begin();
    Event event = channel.take();
    transaction.commit();
    transaction.close();
    Assert.assertNotNull(event);
    Assert.assertEquals("rollback-0-0", new String(event.getBody(), Charsets.UTF_8));
  }
  @Test
  public void testPut() throws Exception {
    // should find no items
    int found = takeEvents(channel, 1, 5).size();
    Assert.assertEquals(0, found);
    putEvents(channel, "unbatched", 1, 5);
    putEvents(channel, "batched", 5, 5);
  }
  @Test
  public void testThreaded() throws IOException, InterruptedException {
    int numThreads = 10;
    final CountDownLatch producerStopLatch = new CountDownLatch(numThreads);
    // due to limited capacity we must wait for consumers to start to put
    final CountDownLatch consumerStartLatch = new CountDownLatch(numThreads);
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
            consumerStartLatch.await();
            if (id % 2 == 0) {
              expected.addAll(putEvents(channel, Integer.toString(id), 1, 5));
            } else {
              expected.addAll(putEvents(channel, Integer.toString(id), 5, 5));
            }
            logger.info("Completed some puts " + expected.size());
          } catch (Exception e) {
            logger.error("Error doing puts", e);
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
            consumerStartLatch.countDown();
            consumerStartLatch.await();
            while(!producerStopLatch.await(1, TimeUnit.SECONDS) ||
                expected.size() > actual.size()) {
              if (id % 2 == 0) {
                actual.addAll(takeEvents(channel, 1, Integer.MAX_VALUE));
              } else {
                actual.addAll(takeEvents(channel, 5, Integer.MAX_VALUE));
              }
            }
            if(actual.isEmpty()) {
              logger.error("Found nothing!");
            } else {
              logger.info("Completed some takes " + actual.size());
            }
          } catch (Exception e) {
            logger.error("Error doing takes", e);
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
  private static List<String> takeEvents(Channel channel, int batchSize,
      int numEvents) throws Exception {
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
  private static List<String> putEvents(Channel channel, String prefix, int batchSize,
      int numEvents) throws Exception {
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
