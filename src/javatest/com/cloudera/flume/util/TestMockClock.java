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
package com.cloudera.flume.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * This tests activating a blocked MockClock after some virtualized sleep time
 * has passed.
 */
public class TestMockClock {

  volatile boolean done = false;

  @Test
  public void testSleepResume() {
    final MockClock m = new MockClock(0);
    done = false;

    Thread t = new Thread() {
      public void run() {
        try {
          m.doSleep(100);
          done = true; // only changes if not interrupted
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    t.start();

    // The mock clock doesn't always wake up the outstanding sleeps right away.
    // (similarly sleep time is a suggestion, and a minimum, not a wake up exact
    // t time units from now.)

    // Here, we expect to wake up at 100, but usually wake up around 150,
    // sometimes in the 300 range! To compensate for now, we over-estimate a
    // bit.
    for (int i = 0; i < 500; i++) {
      m.forward(10);
      if (done) {
        t.interrupt();
        System.out.println("done at time: " + m.getUnixTime());
        // success!
        return;
      }
    }

    t.interrupt();
    Assert.fail("mock clock should have woken up and flipped done");
  }
}
