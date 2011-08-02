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
package com.cloudera.flume.handlers.thrift;

import junit.framework.TestCase;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.thrift.PrioritizedThriftEventSource.EventQueue;

/**
 * This verifies the behavior of the comparator for the event priority queue
 */
public class TestEventQueue extends TestCase {

  /**
   * These are specifically generate for tests below
   */
  final Event e1 = new EventImpl("A".getBytes(), Event.Priority.INFO);
  final Event e2 = new EventImpl("B".getBytes(), Event.Priority.INFO);
  final Event e3 = new EventImpl("C".getBytes(), Event.Priority.INFO);
  final Event e4 = new EventImpl("D".getBytes(), Event.Priority.INFO);

  final Event e5 = new EventImpl("E".getBytes(), Event.Priority.WARN);
  final Event e6 = new EventImpl("F".getBytes(), Event.Priority.DEBUG);

  @SuppressWarnings("serial")
  public void testEventQueuePriority() throws InterruptedException {

    EventQueue q = new EventQueue(10) {

      {
        add(e5);
        add(e6);
        add(e1);
      }
    };

    assertEquals(q.take(), e5); // WARN
    assertEquals(q.take(), e1); // INFO
    assertEquals(q.take(), e6); // DEBUG

  }

  @SuppressWarnings("serial")
  public void testEventQueueNanos() throws InterruptedException {

    EventQueue q = new EventQueue(10) {
      {
        add(e4);
        System.out.println("insert " + e4.getTimestamp() + " " + e4.getNanos()
            + " " + e4);
        add(e2);
        System.out.println("insert " + e2.getTimestamp() + " " + e2.getNanos()
            + " " + e2);
        add(e1);
        System.out.println("insert " + e1.getTimestamp() + " " + e1.getNanos()
            + " " + e1);
        add(e3);
        System.out.println("insert " + e3.getTimestamp() + " " + e3.getNanos()
            + " " + e3);
      }
    };

    Event[] ordered = { e1, e2, e3, e4 };
    for (int i = 0; i < ordered.length; i++) {
      Event qe = q.take();
      System.out.println("take   " + qe.getTimestamp() + " " + qe.getNanos()
          + " " + qe);
      assertEquals(ordered[i], qe);
    }

  }

  @SuppressWarnings("serial")
  public void testEventQueueMillis() throws InterruptedException {
    Thread.sleep(100);

    final Event e = new EventImpl("test".getBytes());
    EventQueue q = new EventQueue(10) {
      {
        add(e);
        System.out.println("insert " + e.getTimestamp() + " " + e.getNanos()
            + " " + e);
        add(e4);
        System.out.println("insert " + e4.getTimestamp() + " " + e4.getNanos()
            + " " + e4);
        add(e2);
        System.out.println("insert " + e2.getTimestamp() + " " + e2.getNanos()
            + " " + e2);
        add(e1);
        System.out.println("insert " + e1.getTimestamp() + " " + e1.getNanos()
            + " " + e1);
        add(e3);
        System.out.println("insert " + e3.getTimestamp() + " " + e3.getNanos()
            + " " + e3);
      }
    };

    Event[] ordered = { e1, e2, e3, e4, e };
    for (int i = 0; i < ordered.length; i++) {
      Event qe = q.take();
      System.out.println("take   " + qe.getTimestamp() + " " + qe.getNanos()
          + " " + qe);
      assertEquals(ordered[i], qe);
    }

  }

}
