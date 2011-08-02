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
package com.cloudera.flume.handlers.debug;

import java.io.IOException;

import junit.framework.TestCase;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.FanOutSink;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * This tests a latched decorator. The latch blocks until it has been blocked a
 * give number of times. This is useful for forcing an order on certain
 * operations for reliability testing.
 * 
 * TODO (jon) problem with this approach is that extra triggers may happen it we
 * are not likely able to control the scheduler order.
 */
public class TestLatchedDeco extends TestCase {

  /**
   * Nothing is done until the trigger is toggled "total" times. We trigger in a
   * separate thread.
   */
  public void testLatchedDeco() throws IOException, InterruptedException {
    final CounterSink c = new CounterSink("count");
    final FanOutSink<EventSink> s = new FanOutSink<EventSink>();
    s.add(c);
    s.add(new ConsoleEventSink());
    final int total = 10;
    final LatchedDecorator<EventSink> l = new LatchedDecorator<EventSink>(s, 0,
        total);
    l.open();
    Thread t = new Thread() {
      public void run() {
        try {
          long count;
          int i = 0;
          while ((count = c.getCount()) < total) {
            l.trigger();
            i++;
            sleep(10);

            System.out.println("triggered " + i + " times, count is " + count);
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();

    for (int i = 0; i < total; i++) {
      Event e = new EventImpl(("message " + i).getBytes());
      l.append(e);
    }
    t.join();
    long count = c.getCount();
    System.out.println("trigger thread joined, count is now: " + count);
    assertEquals(total, count);
  }
}
