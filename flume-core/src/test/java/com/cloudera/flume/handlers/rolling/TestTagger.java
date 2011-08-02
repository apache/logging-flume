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
package com.cloudera.flume.handlers.rolling;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Taggers generate unique values used for acks and file names.
 */
public class TestTagger extends TestCase {
  public static final Logger LOG = LoggerFactory.getLogger(TestTagger.class);

  /**
   * This checks to make sure that tags are always get lexographically larger
   * over time.
   */
  @Test
  public void testTaggerNameMonotonic() {
    Tagger t = new ProcessTagger();
    String[] tags = new String[100];
    for (int i = 0; i < tags.length; i++) {
      tags[i] = t.newTag();
    }

    for (int i = 1; i < tags.length; i++) {
      assertTrue(tags[i - 1].compareTo(tags[i]) < 0);
    }
  }

  /**
   * This checks to make sure that tags are always get lexographically larger
   * over time. A ProcessTagger actually uses thread id # as part of its sort
   * and this verifies that it is the least significant. A Roller can call the
   * new tag method in either of its threads, so we need to take this into
   * account.
   */
  @Test
  public void testThreadedTaggerNameMonotonic() throws InterruptedException {
    final Tagger t = new ProcessTagger();
    final Queue<String> tags = new ArrayBlockingQueue<String>(1000);
    final Object lock = new Object();
    final CountDownLatch start = new CountDownLatch(10);
    final CountDownLatch done = new CountDownLatch(10);
    class TagThread extends Thread {
      public void run() {
        start.countDown();

        try {
          // start all the "same" time
          start.await();
          while (true) {
            // make new tag and insert atomic
            synchronized (lock) {
              String s = t.newTag();
              boolean accepted = tags.offer(s);
              if (!accepted) {
                done.countDown();
                return;
              }
              LOG.info("added tag: {}", s);
            }
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    TagThread[] thds = new TagThread[10];
    for (int i = 0; i < thds.length; i++) {
      thds[i] = new TagThread();
      thds[i].start();
    }
    done.await();

    String[] aTags = tags.toArray(new String[0]);
    for (int i = 1; i < aTags.length; i++) {
      assertTrue(aTags[i - 1].compareTo(aTags[i]) < 0);
    }
  }
}
