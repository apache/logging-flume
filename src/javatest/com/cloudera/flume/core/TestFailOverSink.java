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
package com.cloudera.flume.core;

import java.io.IOException;

import junit.framework.TestCase;

import com.cloudera.flume.handlers.debug.IntervalFlakeyEventSink;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * This tests the failover pipe mechanism. The semantics of this is that it
 * attempts to write to the primary, and if it failes with an exception, it
 * fails over to the secondary. If the secondary fails as well, an exception is
 * thrown.
 */
public class TestFailOverSink extends TestCase {

  public void testFailOverSink() throws IOException {
    CounterSink primary = new CounterSink("primary");
    CounterSink secondary = new CounterSink("backup");

    IntervalFlakeyEventSink<EventSink> flake = new IntervalFlakeyEventSink<EventSink>(
        primary, 2);
    FailOverSink failsink = new FailOverSink(flake, secondary);
    failsink.open();
    for (int i = 0; i < 100; i++) {
      Event e = new EventImpl(("event " + i).getBytes());
      failsink.append(e);
    }

    // this should succeed, with the counts being equal in primary and
    // secondary.
    failsink.close();
    assertEquals(primary.getCount(), secondary.getCount());
  }

  /**
   * This does multiple levels of failover.
   */
  public void testMultiFailOverSink() throws IOException {
    CounterSink primary = new CounterSink("primary");
    CounterSink secondary = new CounterSink("backup");
    CounterSink tertiary = new CounterSink("tertiary");

    IntervalFlakeyEventSink<EventSink> flake1 = new IntervalFlakeyEventSink<EventSink>(
        primary, 2);
    IntervalFlakeyEventSink<EventSink> flake2 = new IntervalFlakeyEventSink<EventSink>(
        secondary, 2);
    FailOverSink failsink0 = new FailOverSink(flake2, tertiary);
    FailOverSink failsink = new FailOverSink(flake1, failsink0);
    failsink.open();
    for (int i = 0; i < 100; i++) {
      Event e = new EventImpl(("event " + i).getBytes());
      failsink.append(e);
    }

    // this should succeed, with the counts being equal in primary and
    // secondary, and tertiary.
    failsink.close();
    assertEquals(50, primary.getCount());
    assertEquals(25, secondary.getCount());
    assertEquals(25, tertiary.getCount());

  }
}
