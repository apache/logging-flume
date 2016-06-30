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

import com.google.common.base.Charsets;
import org.apache.flume.Transaction;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.LoggerSink;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

import static org.apache.flume.channel.file.TestUtils.compareInputAndOut;
import static org.apache.flume.channel.file.TestUtils.putEvents;
import static org.apache.flume.channel.file.TestUtils.takeEvents;

public class TestFileChannelRollback extends TestFileChannelBase {
  protected static final Logger LOG = LoggerFactory
      .getLogger(TestFileChannelRollback.class);

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  @After
  public void teardown() {
    super.teardown();
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
  public void testRollbackSimulatedCrash() throws Exception {
    channel.start();
    Assert.assertTrue(channel.isOpen());
    int numEvents = 50;
    Set<String> in = putEvents(channel, "rollback", 1, numEvents);

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
    Set<String> out = takeEvents(channel, 1, numEvents);
    compareInputAndOut(in, out);
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

    while (runner.isAlive()) {
      Thread.sleep(10L);
    }
    Assert.assertEquals(numEvents - 1, runner.getCount());
    for (Exception ex : runner.getErrors()) {
      LOG.warn("Sink had error", ex);
    }
    Assert.assertEquals(Collections.EMPTY_LIST, runner.getErrors());

    // simulate crash
    channel.stop();
    channel = createFileChannel();
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = takeEvents(channel, 1, 1);
    Assert.assertEquals(1, out.size());
    String s = out.iterator().next();
    Assert.assertTrue(s, s.startsWith("rollback-90-9"));
  }
}
