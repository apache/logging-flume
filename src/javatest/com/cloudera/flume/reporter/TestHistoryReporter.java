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
package com.cloudera.flume.reporter;

import java.io.IOException;

import junit.framework.TestCase;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.flume.reporter.history.CountHistoryReporter;
import com.cloudera.flume.reporter.history.DumbTagger;
import com.cloudera.flume.util.MockClock;
import com.cloudera.util.Clock;
import com.cloudera.util.Pair;

/**
 * This tests the time lining abilities of the history reporter.
 */
public class TestHistoryReporter extends TestCase {

  Tagger t = new DumbTagger();

  /**
   * This version uses wall clock time to test roll over from one epoch to the
   * next.
   */
  public void testTestCountHistory() throws IOException, InterruptedException {
    // have a huge period and just force them in the test.
    CountHistoryReporter r = new CountHistoryReporter("test timeline", 5000000,
        t);
    r.open();
    Event e = new EventImpl("Test message".getBytes());

    // just a little forward to make things slighlty "out of sync"
    // 3
    r.append(e);
    r.append(e);
    r.append(e);
    r.forcedRotate();

    // 2
    r.append(e);
    r.append(e);
    r.forcedRotate();

    // 4
    r.append(e);
    r.append(e);
    r.append(e);
    r.append(e);
    r.forcedRotate();

    // 0 // this never registers!
    r.forcedRotate();

    // 1
    r.append(e);
    r.forcedRotate();

    r.append(e);

    long[] ans = { 3, 2, 4, 0, 1 };
    int i = 0;
    for (Pair<Long, CounterSink> p : r.getHistory()) {
      System.out.printf("time: %,18d count: %8d\n", p.getLeft(), p.getRight()
          .getCount());
      assertEquals(ans[i], p.getRight().getCount());
      i++;
    }
    r.close();
  }

  /**
   * This version uses a mock clock to test roll over from one epoch to the
   * next. We can check mock clock time as well to make sure it works.
   */
  public void testTestCountHistoryClocked() throws IOException {
    MockClock m = new MockClock(0);
    Clock.setClock(m);

    CountHistoryReporter r = new CountHistoryReporter("test timeline", 500, t);
    r.open();
    Event e = new EventImpl("Test message".getBytes());

    // just a little forward to make things slighlty "out of sync"
    // 3
    r.append(e);
    r.append(e);
    r.append(e);
    m.forward(501);

    // 2
    r.append(e);
    r.append(e);
    m.forward(501);

    // 4
    r.append(e);
    r.append(e);
    r.append(e);
    r.append(e);
    m.forward(501);

    // 0 // this never registers!
    m.forward(501);

    // 1
    r.append(e);
    m.forward(501);

    r.append(e);

    long[] times = { 0, 501, 1002, 1503, 2004 };
    long[] ans = { 3, 2, 4, 0, 1 };
    int i = 0;
    for (Pair<Long, CounterSink> p : r.getHistory()) {
      System.out.printf("time: %8d count: %8d\n", p.getLeft(), p.getRight()
          .getCount());
      assertEquals(ans[i], p.getRight().getCount());
      assertEquals((long) p.getLeft(), times[i]);
      i++;
    }
    r.close();
  }

}
