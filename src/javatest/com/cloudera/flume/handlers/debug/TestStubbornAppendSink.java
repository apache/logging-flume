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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;

/**
 * This makes sure that if an exception occurs, that the subordinate sink is
 * closed and re-opened.
 */
public class TestStubbornAppendSink {

  @Test
  public void testStubborn() throws IOException, InterruptedException {
    // just using as an int reference
    final AtomicInteger ok = new AtomicInteger();

    EventSink failAppend = new EventSink.Base() {
      int n = 4; // fail every nth append
      int count = 0;

      @Override
      public void append(Event e) throws IOException {
        count++;
        if (count % n == 0) {
          System.out.println("failed at " + count);
          throw new IOException("Failed, but will succeed later");
        }
        ok.incrementAndGet();
        System.out.print(".");
      }

      @Override
      public void close() throws IOException {
        System.out.println("close");
      }

      @Override
      public void open() throws IOException {
        System.out.println("open");
      }
    };

    StubbornAppendSink<EventSink> sink = new StubbornAppendSink<EventSink>(
        failAppend);
    sink.open();
    for (int i = 0; i < 100; i++) {
      Event e = new EventImpl(("attempt " + i).getBytes());
      sink.append(e);
    }
    Assert.assertEquals(ok.get(), 100);

    ReportEvent rpt = sink.getReport();
    Writer out = new OutputStreamWriter(System.out);
    rpt.toJson(out);
    out.flush();

    // 100 good messages. every 4th message fails -- 3 good 1 bad.
    // 00 01 02 xx 03 04 05 xx 06 07 08 xx ...
    // so 100 good msgs, 133 total messages, 33 bad msgs
    Assert.assertEquals(new Long(100), rpt
        .getLongMetric(StubbornAppendSink.A_SUCCESSES));
    Assert.assertEquals(new Long(33), rpt
        .getLongMetric(StubbornAppendSink.A_FAILS));
    Assert.assertEquals(new Long(33), rpt
        .getLongMetric(StubbornAppendSink.A_RECOVERS));
  }

  /**
   * This test is similar to the previous but uses mockito. (and make much fewer
   * calls)
   * 
   * @throws InterruptedException
   */
  @Test
  public void testStubbornNew() throws IOException, InterruptedException {
    EventSink failAppend = mock(EventSink.class);
    Event e = new EventImpl("test".getBytes());

    // for mocking void void returning calls, we use this mockito
    // syntax (it is kinda gross but still cleaner than the version above)
    // the first two calls "succeed", the third throws io exn, and then the
    // fourth
    doNothing().doNothing().doThrow(new IOException()).doNothing().when(
        failAppend).append(Mockito.<Event> anyObject());
    doReturn(new ReportEvent("stub")).when(failAppend).getReport();

    StubbornAppendSink<EventSink> sink = new StubbornAppendSink<EventSink>(
        failAppend);
    sink.open();

    for (int i = 0; i < 3; i++) {
      sink.append(e);
      System.out.println(i);
    }

    ReportEvent rpt = sink.getReport();
    Assert.assertEquals(new Long(1), rpt
        .getLongMetric(StubbornAppendSink.A_FAILS));
    Assert.assertEquals(new Long(1), rpt
        .getLongMetric(StubbornAppendSink.A_RECOVERS));
  }

  @Test
  public void testStubbornIntervalFlakey() throws IOException,
      InterruptedException {

    // count resets on open and close, this one does not.
    final AtomicInteger ok = new AtomicInteger();

    EventSink cnt = new EventSink.Base() {
      @Override
      public void append(Event e) throws IOException {
        ok.incrementAndGet();
      }
    };

    // Every 5th event throws IOException
    IntervalFlakeyEventSink<EventSink> flake = new IntervalFlakeyEventSink<EventSink>(
        cnt, 5);
    StubbornAppendSink<EventSink> sink = new StubbornAppendSink<EventSink>(
        flake);
    sink.open();
    for (int i = 0; i < 100; i++) {
      Event e = new EventImpl(("attempt " + i).getBytes());
      sink.append(e);
    }
    Assert.assertEquals(ok.get(), 100);

    ReportEvent rpt = sink.getReport();
    // why isn't this 25?
    Assert.assertEquals(new Long(24), rpt
        .getLongMetric(StubbornAppendSink.A_FAILS));
    Assert.assertEquals(new Long(24), rpt
        .getLongMetric(StubbornAppendSink.A_RECOVERS));

  }

  @Test
  public void testExceptionFallthrough() throws IOException,
      InterruptedException {
    EventSink mock = mock(EventSink.class);
    // two ok, and then two exception throwing cases
    doNothing().doNothing().doThrow(new IOException()).doThrow(
        new IOException()).when(mock).append(Mockito.<Event> anyObject());
    doReturn(new ReportEvent("stub")).when(mock).getReport();

    StubbornAppendSink<EventSink> sink = new StubbornAppendSink<EventSink>(mock);
    Event e = new EventImpl("foo".getBytes());
    sink.open();
    sink.append(e);
    sink.append(e);
    try {
      sink.append(e);
    } catch (Exception exn) {
      ReportEvent rpt = sink.getReport();
      Assert.assertEquals(new Long(2), rpt
          .getLongMetric(StubbornAppendSink.A_SUCCESSES));
      Assert.assertEquals(new Long(1), rpt
          .getLongMetric(StubbornAppendSink.A_FAILS));
      Assert.assertEquals(new Long(0), rpt
          .getLongMetric(StubbornAppendSink.A_RECOVERS));
      return;
    }
    Assert.fail("should have thrown exception");
  }
}
