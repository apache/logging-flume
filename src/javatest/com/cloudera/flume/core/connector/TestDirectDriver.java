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
package com.cloudera.flume.core.connector;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.debug.InsistentAppendDecorator;
import com.cloudera.flume.handlers.debug.InsistentOpenDecorator;
import com.cloudera.flume.handlers.debug.LazyOpenDecorator;
import com.cloudera.flume.handlers.debug.LazyOpenSource;
import com.cloudera.flume.handlers.debug.NoNlASCIISynthSource;
import com.cloudera.flume.handlers.debug.StubbornAppendSink;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.BackoffPolicy;
import com.cloudera.util.CappedExponentialBackoff;
import com.cloudera.util.Clock;

/**
 * This tests the open, close, execution, shutdown and cancellation sematnics of
 * the direct driver
 */
public class TestDirectDriver {
  /**
   * Test a thread cancel on something blocked on open. This forces a
   * InterruptedException throw. Normally an insistentOpen will never return if
   * the subsink's open always fails. cancel() forces the interrupt flag
   * (InterruptedException on blocked things) which the insistent open
   * translates into a IOException.
   * 
   * (Ideally it should propagate the InterruptedException, but I think that
   * change is pervasive and will wait for the next major version)
   */
  @Test
  public void testInsistentOpenCancel() throws IOException,
      InterruptedException {
    EventSink fail4eva = mock(EventSink.Base.class);
    doThrow(new IOException("mock exception")).when(fail4eva).open();
    doReturn(new ReportEvent("stub")).when(fail4eva).getReport();

    // max 5s, backoff initially at 10ms
    BackoffPolicy bop = new CappedExponentialBackoff(10, 5000);
    final InsistentOpenDecorator<EventSink> insistent = new InsistentOpenDecorator<EventSink>(
        fail4eva, bop);
    final EventSink sink = new LazyOpenDecorator<EventSink>(insistent);
    sink.open();

    // create an endless stream of data
    final EventSource source = new LazyOpenSource<EventSource>(
        new NoNlASCIISynthSource(0, 100));
    source.open();

    DirectDriver driver = new DirectDriver(source, sink);
    driver.start();
    Clock.sleep(1000); // let the insistent open try a few times.
    driver.stop();

    boolean closed = driver.join(1000);
    assertFalse(closed);

    driver.cancel();
    assertTrue(driver.join(1000)); // closed this time.

  }

  /**
   * Test a thread cancel on something blocked on append. This forces a
   * InterruptedException throw. Normally an insistentAppend will never return
   * if the subsink's open always fails. cancel() forces the interrupt flag
   * (InterruptedException on blocked things) which the insistent append
   * translates into a IOException.
   * 
   * (Ideally it should propagate the InterruptedException, but I think that
   * change is pervasive and will wait for the next major version)
   */
  @Test
  public void testInsistentAppendCancel() throws IOException,
      InterruptedException {
    EventSink fail4eva = mock(EventSink.Base.class);
    final Event e = new EventImpl("foo".getBytes());
    doThrow(new IOException("mock exception")).when(fail4eva).append(e);
    doReturn(new ReportEvent("mock report")).when(fail4eva).getReport();
    doReturn("mock name").when(fail4eva).getName();

    // max 5s, backoff initially at 10ms
    BackoffPolicy bop = new CappedExponentialBackoff(10, 5000);
    final EventSink insistent = new InsistentAppendDecorator<EventSink>(
        fail4eva, bop);
    final EventSink sink = new LazyOpenDecorator<EventSink>(insistent);
    sink.open();

    // create an endless stream of data
    final EventSource source = new EventSource.Base() {
      @Override
      public Event next() {
        return e;
      }
    };
    // no need to open this.

    DirectDriver driver = new DirectDriver(source, sink);
    driver.start();
    Clock.sleep(1000); // let the insistent append try a few times.
    driver.stop();

    boolean closed = driver.join(1000);
    assertFalse(closed);

    driver.cancel();
    assertTrue(driver.join(1000)); // closed this time.

  }

  /**
   * This checks to make sure that even though there is along roll period, the
   * sink exits quickly due to the cancel call.
   */
  @Test
  public void testRollSinkCancel() throws IOException, InterruptedException {
    EventSink fail4eva = mock(EventSink.Base.class);
    final Event e = new EventImpl("foo".getBytes());
    doThrow(new IOException("mock exception")).when(fail4eva).append(e);
    doReturn(new ReportEvent("mock report")).when(fail4eva).getReport();
    doReturn("mock name").when(fail4eva).getName();

    // max 5s, backoff initially at 10ms
    BackoffPolicy bop = new CappedExponentialBackoff(10, 5000);
    final EventSink insistent = new InsistentAppendDecorator<EventSink>(
        fail4eva, bop);
    final EventSink sink = new LazyOpenDecorator<EventSink>(insistent);
    final EventSink roll = new RollSink(new ReportTestingContext(), "mock",
        10000, 100) {
      @Override
      public EventSink newSink(Context ctx) throws IOException {
        return sink;
      }
    };
    roll.open();

    // create an endless stream of data
    final EventSource source = new EventSource.Base() {
      @Override
      public Event next() {
        return e;
      }
    };
    // no need to open this.

    DirectDriver driver = new DirectDriver(source, roll);
    driver.start();
    Clock.sleep(1000); // let the insistent append try a few times.
    driver.stop();

    boolean closed = driver.join(1000);
    assertFalse(closed);

    driver.cancel();
    assertTrue(driver.join(1000)); // closed this time.

  }

  /**
   * Test a thread cancel on something blocked on open. This forces a
   * InterruptedException throw. Normally an insistentOpen will never return if
   * the subsink's open always fails. cancel() forces the interrupt flag
   * (InterruptedException on blocked things) which the insistent open
   * translates into a IOException.
   * 
   * (Ideally it should propagate the InterruptedException, but I think that
   * change is pervasive and will wait for the next major version)
   */
  @Test
  public void testDFOSubsinkCancel() throws IOException, InterruptedException {
    EventSink fail4eva = mock(EventSink.Base.class);
    doThrow(new IOException("mock exception")).when(fail4eva).open();
    doReturn(new ReportEvent("stub")).when(fail4eva).getReport();

    // max 5s, backoff initially at 10ms
    BackoffPolicy bop = new CappedExponentialBackoff(10, 5000);

    final InsistentOpenDecorator<EventSink> insistent = new InsistentOpenDecorator<EventSink>(
        fail4eva, bop);
    final StubbornAppendSink<EventSink> stubborn = new StubbornAppendSink<EventSink>(
        insistent);
    final InsistentAppendDecorator<EventSink> append = new InsistentAppendDecorator<EventSink>(
        stubborn, new CappedExponentialBackoff(100, 100000));
    final EventSink sink = new LazyOpenDecorator<EventSink>(append);
    sink.open();

    // create an endless stream of data
    final EventSource source = new LazyOpenSource<EventSource>(
        new NoNlASCIISynthSource(0, 100));
    source.open();

    DirectDriver driver = new DirectDriver(source, sink);
    driver.start();
    Clock.sleep(1000); // let the insistent open try a few times.
    driver.stop();

    boolean closed = driver.join(1000);
    assertFalse(closed);

    driver.cancel();
    assertTrue(driver.join(1000)); // closed this time.
  }

}
