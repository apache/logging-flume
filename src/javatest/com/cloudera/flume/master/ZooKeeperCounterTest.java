/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.FileUtil;

/**
 * Unit tests for ZooKeeperCounter
 */
public class ZooKeeperCounterTest {
  static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCounterTest.class);
  static ZooKeeperService svc;
  static File tmp;

  @BeforeClass
  static public void setupZK() throws IOException, InterruptedException {
    tmp = FileUtil.mktempdir();
    FlumeConfiguration.get().set(FlumeConfiguration.MASTER_ZK_LOGDIR,
        tmp.getAbsolutePath());
    svc = ZooKeeperService.getAndInit();
  }

  @AfterClass
  static public void tearDownZK() throws IOException {
    svc.shutdown();
    FileUtil.rmr(tmp);
  }

  /**
   * Test that counter by default returns monotonically increasing sequence.
   */
  @Test
  public void testMonotonicity() throws IOException, InterruptedException,
      KeeperException {
    ZooKeeperCounter seq = new ZooKeeperCounter(svc, "/seq-generator-test");

    for (long i = 0; i < 1024; ++i) {
      assertEquals(i, seq.incrementAndGet());
    }
    seq.shutdown();
  }

  /**
   * Test that changing the increment size changes the step in counter values
   * returned.
   */
  @Test
  public void testIncrementSize() throws IOException, InterruptedException,
      KeeperException {
    ZooKeeperCounter seq = new ZooKeeperCounter(svc, "/seq-generator-incr-test");

    for (long i = 0; i < 1024; ++i) {
      assertEquals(i * 4, seq.incrementAndGetBy(4L));
    }
    seq.shutdown();
  }
  
  /**
   * Test that we can reset the counter to a range of values
   */
  @Test
  public void testReset() throws IOException, InterruptedException, KeeperException {
    ZooKeeperCounter seq = new ZooKeeperCounter(svc, "/seq-generator-reset-test");
    
    for (long i=0; i < 1024; ++i) {
      seq.resetTo(i);
      assertEquals(i, seq.incrementAndGet());
    }
    seq.shutdown();
  }

  final static int LOOP_COUNT = 200;

  public class CounterThread extends Thread {
    private ZooKeeperCounter seq;
    public ArrayList<Long> counts = new ArrayList<Long>();

    public CounterThread(ZooKeeperCounter seq) {
      this.seq = seq;
    }

    public void run() {
      for (int i = 0; i < LOOP_COUNT; ++i) {
        try {
          counts.add(seq.incrementAndGet());
        } catch (Exception e) {
          // Just log - missing counter value will fail the test
          LOG.error("CounterThread saw exception",e);
        }
      }
    }
  }

  /**
   * Test that running two threads accessing the same counter together both see
   * a sequentially consistent view of the counter.
   */
  @Test
  public void testConcurrency() throws IOException, KeeperException,
      InterruptedException {
    ZooKeeperCounter seq = new ZooKeeperCounter(svc, "/seq-generator-conc-test");

    CounterThread t1 = new CounterThread(seq);
    CounterThread t2 = new CounterThread(seq);

    t1.start();
    t2.start();

    t1.join();
    t2.join();
    
    long last = -1;
    for (Long l : t1.counts) {
      assertTrue("Expected increasing counts", last < l);
      last = l;
    }
    last = -1;
    for (Long l : t2.counts) {
      assertTrue("Expected increasing counts", last < l);
      last = l;
    }
    
    ArrayList<Long> counts = t1.counts;    
    counts.addAll(t2.counts);
    Collections.sort(counts);

    for (long i = 0; i < LOOP_COUNT * 2; ++i) {
      assertEquals("Missing counter value", i, (long) counts.get((int) i));
    }
    seq.shutdown();
  }
}
