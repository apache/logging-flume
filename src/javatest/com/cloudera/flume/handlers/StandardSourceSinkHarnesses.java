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
package com.cloudera.flume.handlers;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.junit.Test;

import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Clock;

/**
 * Standard harnesses for source and sink semantics compliance.
 */
public class StandardSourceSinkHarnesses {

  static public void testOpenOpen(final Logger LOG, final EventSource src)
      throws IOException {
    src.open();
    try {
      src.open();
    } catch (Exception e) {
      LOG.info("Expected failure " + e.getMessage());
      return;
    }
    src.close();
    fail("Open Open should fail");
  }

  static public void testCloseClose(final Logger LOG, final EventSource src)
      throws IOException {
    // close should be ok
    src.close();

    // open and then close should be ok.
    src.open();
    src.close();

    // second close should be ok.
    src.close();
  }

  /**
   * This test starts a source, and assumes the next call blocks. It does this
   * in one thread and then attempts to close it from another. If the thread
   * doesn't return the test will timeout and fail.
   */
  @Test
  static public void testSourceConcurrentClose(final Logger LOG,
      final EventSource src) throws InterruptedException, IOException {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);

    new Thread() {
      @Override
      public void run() {
        try {
          src.open();
          started.countDown();
          src.next();
          done.countDown();
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }.start();

    assertTrue("Open timed out", started.await(5, TimeUnit.SECONDS));
    // give some time for next to get called and block
    Clock.sleep(100);
    src.close();
    assertTrue("Next timed out", done.await(5, TimeUnit.SECONDS));
  }

  /**
   * This test starts and stops a source over and over to verify that the close
   * cleanly
   */
  static public void testSourceOpenClose(Logger LOG, EventSource src)
      throws IOException {
    for (int i = 0; i < 50; i++) {
      LOG.info("ThirftEventSource open close attempt " + i);
      src.open();
      src.close();
    }
  }

}
