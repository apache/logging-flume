/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package com.cloudera.flume.agent.durability;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNodeWALNotifier;
import com.cloudera.flume.agent.durability.NaiveFileWALManager.WALData;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

/**
 * This test case exercises all the state transitions found when data goes
 * through the write ahead log and comes out the other side.
 */
public class TestFlumeNodeWALNotifierRacy {
  Logger LOG = LoggerFactory.getLogger(TestFlumeNodeWALNotifierRacy.class);

  Date date;
  CannedTagger tagger;
  AckListener mockAl;
  File tmpdir;
  NaiveFileWALManager walman;
  Map<String, WALManager> map;
  EventSink snk;
  EventSource src;

  /**
   * This issues simple incrementing integer toString as a tag for the next wal
   * file.
   */
  static class CannedTagger implements Tagger {
    int cur = 0;
    List<String> tags = Collections.synchronizedList(new ArrayList<String>());

    CannedTagger() {
    }

    @Override
    public String getTag() {
      return Integer.toString(cur);
    }

    @Override
    public String newTag() {
      cur++;
      String tag = Integer.toString(cur);
      tags.add(tag);
      return tag;
    }

    @Override
    public Date getDate() {
      return null;
    }

    List<String> getTags() {
      return tags;
    }
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    date = new Date();

    tagger = new CannedTagger();
    mockAl = mock(AckListener.class);

    tmpdir = FileUtil.mktempdir();
    walman = new NaiveFileWALManager(tmpdir);
    walman.open();

    map = new HashMap<String, WALManager>();
    map.put("wal", walman);
  }

  @After
  public void teardown() throws IOException {
    FileUtil.rmr(tmpdir);
  }

  /**
   * Attempt a retry on the specified tag. It is important that there is no
   * sychronization on this -- concurrency needs to be handled by the wal
   * sources and walmanager code.
   * 
   * @param tag
   * @throws IOException
   */
  public void triggerRetry(String tag) throws IOException {
    FlumeNodeWALNotifier notif = new FlumeNodeWALNotifier(map);
    notif.retry(tag);
  }

  /**
   * Transition to writing state.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  void toWritingState() throws IOException, InterruptedException {
    snk = walman.newAckWritingSink(tagger, mockAl);
    EventImpl evt = new EventImpl("foofoodata".getBytes());
    snk.open();
    snk.append(evt);
  }

  /**
   * Transition from writing to logged state
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  void toLoggedState() throws IOException, InterruptedException {
    // transition to logged state.
    snk.close();
  }

  /**
   * Transition from logged to sending state.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  void toSendingState() throws IOException, InterruptedException {
    // transition to sending state.
    src = walman.getUnackedSource();
    src.open();
    while (src.next() != null) {
      ;
    }
  }

  /**
   * Transition from sending to sent state.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  void toSentState() throws IOException, InterruptedException {
    // transition to sent state.
    src.close();
  }

  /**
   * Transition from sent to acked state
   * 
   * @param tag
   * @throws IOException
   */
  void toAckedState(String tag) throws IOException {
    // transition to acked state.
    walman.toAcked(tag);
  }

  /**
   * Make one state transition based on the current state of the wal tag. The
   * key point here is that even though there is no locking here, transition
   * between states can have retry's happen anywhere and data still only does
   * proper state transitions. function is executed.
   * 
   * @param tag
   * @return isDone (either in E2EACKED state, or no longer present)f
   * @throws IOException
   * @throws InterruptedException
   */
  boolean step(String tag) throws IOException, InterruptedException {
    WALData wd = walman.getWalData(tag);
    if (wd == null) {
      return true;
    }
    switch (wd.s) {
    case WRITING:
      LOG.info("LOGGED  tag '" + tag + "'");
      toLoggedState();
      return false;
    case LOGGED:
      LOG.info("SENDING tag '" + tag + "'");
      toSendingState();
      return false;
    case SENDING:
      LOG.info("SENT    tag '" + tag + "'");
      toSentState();
      return false;
    case SENT:
      LOG.info("ACKED   tag '" + tag + "'");
      toAckedState(tag);
      return false;
    case E2EACKED:
      return true;
    default:
      throw new IllegalStateException("Unexpected state " + wd.s);
    }
  }

  /**
   * Run the test for 10000 wal files. On laptop this normally finishes in 20s,
   * so timeout after 100s (100000ms).
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test(timeout = 100000)
  public void testRetryWriting() throws IOException, InterruptedException {
    final int count = 10000;
    retryWritingRacynessRun(count);
  }

  /**
   * The test manages threads -- on thread to marching through states and
   * another that introduces many many retry attempts.
   * 
   * @param count
   * @throws InterruptedException
   */
  void retryWritingRacynessRun(final int count) throws InterruptedException {
    // one for each thread
    final CountDownLatch done = new CountDownLatch(2);
    // start second thread
    final CountDownLatch canRetry = new CountDownLatch(1);

    // Thread 1 is creating new log data
    Thread t = new Thread() {
      public void run() {
        try {
          for (int i = 0; i < count; i++) {
            // there is an off by one thing here
            String tag = Integer.toString(i + 1);
            LOG.info("WRITING tag '" + tag + "'");
            toWritingState();
            canRetry.countDown();
            // tag = tagger.getTags().get(i);
            // first time will allow retry thread to start, otherwise do nothing
            while (!step(tag)) {
              LOG.info("Stepping on tag " + tag);
            }

          }
        } catch (Exception e) {
          LOG.error("This wasn't supposed to happen", e);
        } finally {
          done.countDown();
        }
      }
    };
    t.start();

    // thread 2 is periodically triggering retries.
    Thread t2 = new Thread() {
      public void run() {
        int count = 0;
        try {
          canRetry.await();
          while (done.getCount() > 1) {
            List<String> tags = tagger.getTags();
            Clock.sleep(10);

            // Key point -- read of the tag and the retry don't clash with other
            // threads state transition
            // synchronized (lock) {
            int sz = tags.size();
            for (int i = 0; i < sz; i++) {
              String tag = tags.get(i);
              WALData wd = walman.getWalData(tag);
              if (!walman.getWritingTags().contains(tag) && wd != null) {
                // any state but writing
                triggerRetry(tag);
                LOG.info("Forcing retry on tag '" + tag + "'");
                count++;
              }
              // }

            }
          }
        } catch (Exception e) {
          LOG.error("This wasn't supposed to happen either", e);
        } finally {
          done.countDown();
          LOG.info("Issued {} retries", count);
        }
      }
    };
    t2.start();

    done.await();

    assertEquals(0, walman.getWritingTags().size());
    assertEquals(0, walman.getLoggedTags().size());
    assertEquals(0, walman.getSendingTags().size());
    assertEquals(0, walman.getSentTags().size());
  }

  /**
   * You can run this test from the command line for an extended racyness
   * testing.
   */
  public static void main(String[] argv) {
    if (argv.length != 1) {
      System.err.println("usage: "
          + TestFlumeNodeWALNotifierRacy.class.getSimpleName() + " <n>");
      System.err
          .println("  Run wal racyness test sending <n> events.  This rolls "
              + "a new log file after every event and also attempts to "
              + "a retry every 10ms.");
      System.exit(1);
    }
    int count = Integer.parseInt(argv[0]);

    TestFlumeNodeWALNotifierRacy test = new TestFlumeNodeWALNotifierRacy();
    try {
      test.setup();
      test.retryWritingRacynessRun(count);
      test.teardown();
    } catch (Exception e) {
      System.err.println("Test failed");
      e.printStackTrace();
      System.exit(2);
    }
    // success!
  }
}
