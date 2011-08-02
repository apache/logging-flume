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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.util.MockClock;
import com.cloudera.util.Clock;

/**
 * This tests the insistent opener (it tries many times before giving up)
 */
public class TestInsistentOpen {

  /**
   * Test that IOD retries the correct number of times when opening a sink that
   * fails twice and then succeeds.
   */
  @Test
  public void testInsistent() throws IOException {
    // TODO(henry): this test relies on real clocks, and shouldn't. See below.
    EventSink fail2x = mock(EventSink.Base.class);
    // two exceptions then some success
    doThrow(new IOException()).doThrow(new IOException()).doNothing().when(
        fail2x).open();
    doReturn(new ReportEvent("stub")).when(fail2x).getReport();

    // max 5s, backoff initially at 10ms

    // WARNING! This test relies on being able to sleep for ~10ms and be woken
    // up three times in a 5s period. Seems plausible! But if you are looking at
    // this comment, it's probably because the test is failing on a loaded
    // machine...
    InsistentOpenDecorator<EventSink> sink = new InsistentOpenDecorator<EventSink>(
        fail2x, 5000, 10);
    sink.open();
    sink.append(new EventImpl("test".getBytes()));
    sink.close();
    fail2x.getReport();

    ReportEvent rpt = sink.getReport();
    assertEquals(new Long(1), Attributes.readLong(rpt,
        InsistentOpenDecorator.A_REQUESTS));
    assertEquals(new Long(3), Attributes.readLong(rpt,
        InsistentOpenDecorator.A_ATTEMPTS));
    assertEquals(new Long(1), Attributes.readLong(rpt,
        InsistentOpenDecorator.A_SUCCESSES));
    assertEquals(new Long(2), Attributes.readLong(rpt,
        InsistentOpenDecorator.A_RETRIES));
    System.out.println(rpt.toText());
  }

  /**
   * Test that an IOD tries the correct number of times to reopen a failing
   * sink, then gives up.
   */
  @Test
  public void testInsistentRetry() {
    final MockClock m = new MockClock(0);

    EventSink failWhale = new EventSink.Base() {
      public ReportEvent getReport() {
        return new ReportEvent("failwhale-report");
      }

      @Override
      public void open() throws IOException {
        // Forward by 100ms, should cause IOD to try eleven times then give up
        m.forward(100);
        throw new IOException();
      }
    };

    Clock.setClock(m);
    // max 1s, backoff initially at 10ms
    InsistentOpenDecorator<EventSink> sink = new InsistentOpenDecorator<EventSink>(
        failWhale, 1000, 10);

    try {
      sink.open();
    } catch (IOException e1) {

      ReportEvent rpt = sink.getReport();
      assertEquals(new Long(1), Attributes.readLong(rpt,
          InsistentOpenDecorator.A_REQUESTS));
      assertEquals(new Long(0), Attributes.readLong(rpt,
          InsistentOpenDecorator.A_SUCCESSES));

      // 11 attempts - one each at 100 * x for x in [0,1,2,3,4,5,6,7,8,9,10]
      // Retry trigger in IOD only fails when time > max, but passes when time =
      // max.
      assertEquals(new Long(11), Attributes.readLong(rpt,
          InsistentOpenDecorator.A_ATTEMPTS));
      assertEquals(new Long(11), Attributes.readLong(rpt,
          InsistentOpenDecorator.A_RETRIES));
      Clock.resetDefault();
      return; // success
    }
    fail("Ehr? Somehow the failwhale succeeded!");

  }
}
