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

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.handlers.debug.ExceptionTwiddleDecorator;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.flume.util.MockClock;
import com.cloudera.util.Clock;

/**
 * This tests the backoff failover pipe mechanism. This acts similarly to the
 * normal backoff pipe, except that it backs off the primary for and
 * exponentially growing period of time.
 **/
public class TestBackOffFailOverSink {

  /**
   * tests a series of messages being sent when append of the primary will fail
   * succeed or fail based on its twiddle state.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testFailOverSink() throws IOException, InterruptedException {
    MockClock mock = new MockClock(0);
    Clock.setClock(mock);

    CounterSink primary = new CounterSink("primary");
    CounterSink secondary = new CounterSink("backup");
    ExceptionTwiddleDecorator<CounterSink> twiddle = new ExceptionTwiddleDecorator<CounterSink>(
        primary);
    BackOffFailOverSink failsink = new BackOffFailOverSink(twiddle, secondary,
        100, 10000); // 100 ms
    // initial
    // backoff,
    // 10000ms max
    // backoff
    failsink.open();

    Event e = new EventImpl("event".getBytes());
    // two successful appends to primary.
    failsink.append(e);
    failsink.append(e);
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());
    Assert.assertEquals(2, primary.getCount());
    Assert.assertEquals(0, secondary.getCount());

    mock.forward(100);
    twiddle.setAppendOk(false); // go to fail over.
    failsink.append(e); // primary fails and automatically go to 2ndary
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());
    Assert.assertEquals(1, failsink.getFails()); // one attempt on primary
                                                 // failed.
    Assert.assertEquals(2, primary.getCount()); // same as before,
    Assert.assertEquals(1, secondary.getCount()); // message went to the
                                                  // secondary

    mock.forward(50);
    failsink.append(e); // skip primary and just go to 2ndary
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());
    Assert.assertEquals(1, failsink.getFails()); // still only one attempt on
                                                 // primary
    Assert.assertEquals(2, primary.getCount()); // same as before,
    Assert.assertEquals(2, secondary.getCount()); // message went to the
                                                  // secondary

    mock.forward(50);
    failsink.append(e); // after this fails backoff is now 200
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());
    Assert.assertEquals(2, failsink.getFails()); // try primary
    Assert.assertEquals(0, primary.getCount()); // resets because primary
                                                // restarted
    // (and still fails)
    Assert.assertEquals(3, secondary.getCount()); // but failover to secondary

    mock.forward(200);
    failsink.append(e); // should go to 2ndary, after this fails backoff is now
    // 400
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());
    Assert.assertEquals(3, failsink.getFails());
    Assert.assertEquals(0, primary.getCount());
    Assert.assertEquals(4, secondary.getCount());

    twiddle.setAppendOk(true);
    failsink.append(e); // even through primary is ok, we are backing off
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());
    Assert.assertEquals(3, failsink.getFails());
    Assert.assertEquals(0, primary.getCount());
    Assert.assertEquals(5, secondary.getCount());

    mock.forward(400);
    failsink.append(e); // now that the backoff has expired, we retry the
    // primary and succeed
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());
    Assert.assertEquals(3, failsink.getFails());
    Assert.assertEquals(1, primary.getCount());
    Assert.assertEquals(5, secondary.getCount());

    // this should succeed, with the counts being equal in primary and
    // secondary.
    failsink.close();
  }

  /**
   * Purposely tests backoff timeout.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testFailTimeout() throws IOException, InterruptedException {
    System.out.println("===========================");
    MockClock mock = new MockClock(0);
    Clock.setClock(mock);

    CounterSink primary = new CounterSink("primary");
    CounterSink secondary = new CounterSink("backup");
    ExceptionTwiddleDecorator<CounterSink> twiddle = new ExceptionTwiddleDecorator<CounterSink>(
        primary);
    BackOffFailOverSink failsink = new BackOffFailOverSink(twiddle, secondary,
        100, 1000); // 100 ms
    // initial
    // backoff,
    // 10000ms max
    // backoff
    failsink.open();

    Event e = new EventImpl("event".getBytes());

    mock.forward(100);
    twiddle.setAppendOk(false); // go to fail over.
    failsink.append(e);
    failsink.append(e);
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());

    mock.forward(100);
    failsink.append(e);
    failsink.append(e);
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());

    mock.forward(200);
    failsink.append(e);
    failsink.append(e);
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());

    mock.forward(400);
    failsink.append(e);
    failsink.append(e);
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());
    Assert.assertEquals(4, failsink.getFails());
    Assert.assertEquals(0, primary.getCount());
    Assert.assertEquals(8, secondary.getCount());

    mock.forward(800);
    failsink.append(e);
    failsink.append(e);
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());
    Assert.assertEquals(5, failsink.getFails());
    Assert.assertEquals(0, primary.getCount());
    Assert.assertEquals(10, secondary.getCount());

    // without capping there would be no new fail here bug still the
    // twelve on the secondary count.
    mock.forward(1000);
    failsink.append(e);
    failsink.append(e);
    System.out.println(mock);
    System.out.printf("pri: %4d sec: %4d fail: %4d\n", primary.getCount(),
        secondary.getCount(), failsink.getFails());
    Assert.assertEquals(6, failsink.getFails());
    Assert.assertEquals(0, primary.getCount());
    Assert.assertEquals(12, secondary.getCount());

  }

  /**
   * This tests the new failover builder that uses specs strings as arguments
   * and instantiates them!
   * 
   * @throws InterruptedException
   */
  @Test
  public void testFailoverBuilder() throws IOException, InterruptedException {
    SinkBuilder bld = FailOverSink.builder();
    EventSink snk = bld.build(new ReportTestingContext(),
        "{intervalFlakeyAppend(2) => counter(\"pri\") } ", "counter(\"sec\")");
    snk.open();

    Event e = new EventImpl("foo".getBytes());
    snk.append(e);
    snk.append(e);
    snk.append(e);
    snk.append(e);
    snk.append(e);

    snk.close();
    CounterSink priCnt = (CounterSink) ReportManager.get().getReportable("pri");
    CounterSink secCnt = (CounterSink) ReportManager.get().getReportable("sec");
    // these are timing based, may fail.
    Assert.assertEquals(3, priCnt.getCount());
    Assert.assertEquals(2, secCnt.getCount());
  }

  /**
   * This tests the new failover builder that uses specs strings as arguments
   * and instantiates them!
   * 
   * @throws InterruptedException
   */
  @Test
  public void testBackoffFailoverBuilder() throws IOException,
      InterruptedException {
    SinkBuilder bld = BackOffFailOverSink.builder();
    EventSink snk = bld.build(new ReportTestingContext(),
        "{intervalFlakeyAppend(2) => counter(\"pri\") } ", "counter(\"sec\")");
    snk.open();

    Event e = new EventImpl("foo".getBytes());
    snk.append(e);
    snk.append(e);
    snk.append(e);
    snk.append(e);
    snk.append(e);

    snk.close();
    CounterSink priCnt = (CounterSink) ReportManager.get().getReportable("pri");
    CounterSink secCnt = (CounterSink) ReportManager.get().getReportable("sec");
    // these are timing based, may fail.
    Assert.assertEquals(1, priCnt.getCount());
    Assert.assertEquals(4, secCnt.getCount());

  }
}
