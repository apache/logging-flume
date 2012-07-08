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
package com.cloudera.flume.handlers.endtoend;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.codehaus.jettison.json.JSONException;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.FanOutSink;
import com.cloudera.flume.handlers.debug.ConsoleEventSink;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.StubbornAppendSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportTestUtils;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * Some tests to verify that the checksums are calculated and are equivalent on
 * both sides.
 */
public class TestAckChecksumDecos {

  public static final Logger LOG = LoggerFactory
      .getLogger(TestAckChecksumDecos.class);

  // Just a test to see the output.
  @Test
  public void testCheckCheckerConsole() throws IOException,
      InterruptedException {

    int msgs = 5;
    CounterSink cnt = new CounterSink("count");
    AckChecksumInjector<EventSink> aci = new AckChecksumInjector<EventSink>(
        new FanOutSink<EventSink>(cnt, new ConsoleEventSink()));

    aci.open();
    for (int i = 0; i < msgs; i++) {
      Event e = new EventImpl(("this is a test " + i).getBytes());
      aci.append(e);
    }
    aci.close();
    assertEquals(5 + 2, cnt.getCount()); // should have gotten 5 messages + 1
    // start ack and 1 stop ack.
  }

  /**
   * Send messages in order and make sure they check out
   * 
   * @throws InterruptedException
   */
  @Test
  public void testCheckChecker() throws IOException, InterruptedException {
    int msgs = 100;
    MemorySinkSource mss = new MemorySinkSource();
    AckChecksumChecker<EventSink> acc = new AckChecksumChecker<EventSink>(mss);
    AckChecksumInjector<EventSink> aci = new AckChecksumInjector<EventSink>(acc);

    aci.open();
    for (int i = 0; i < msgs; i++) {
      Event e = new EventImpl(("this is a test " + i).getBytes());
      aci.append(e);
    }
    aci.close(); // will throw exception if checksum doesn't match

    Event eo = null;
    int count = 0;
    while ((eo = mss.next()) != null) {
      System.out.println(eo);
      count++;
    }

    assertEquals(msgs, count); // extra ack messages should have been consumed
  }

  /**
   * Send messages when some message are reordered and make sure they check out
   * 
   * @throws InterruptedException
   */
  @Test
  public void testReorderedChecker() throws IOException, InterruptedException {
    MemorySinkSource mss = new MemorySinkSource();
    AckChecksumChecker<EventSink> cc = new AckChecksumChecker<EventSink>(mss);
    ReorderDecorator<EventSink> ro = new ReorderDecorator<EventSink>(cc, .5,
        .5, 0);
    AckChecksumInjector<EventSink> cp = new AckChecksumInjector<EventSink>(ro);

    cp.open();
    for (int i = 0; i < 100; i++) {
      Event e = new EventImpl(("this is a test " + i).getBytes());
      cp.append(e);
    }
    cp.close();

    Event eo = null;
    while ((eo = mss.next()) != null) {
      System.out.println(eo);
    }
  }

  /**
   * error case: no start message
   * 
   * @throws InterruptedException
   */
  @Test
  public void testNoStart() throws IOException, InterruptedException {
    final int msgs = 100;
    MemorySinkSource mss = new MemorySinkSource();
    AckChecksumChecker<EventSink> cc = new AckChecksumChecker<EventSink>(mss);
    EventSinkDecorator<EventSink> dropFirst = new EventSinkDecorator<EventSink>(
        cc) {
      int count = 0;

      public void append(Event e) throws IOException, InterruptedException {
        if (count == 0) {
          count++;
          return;
        }
        count++;
        getSink().append(e);
      }
    };
    AckChecksumInjector<EventSink> cp = new AckChecksumInjector<EventSink>(
        dropFirst);

    try {

      cp.open();
      for (int i = 0; i < msgs; i++) {
        Event e = new EventImpl(("this is a test " + i).getBytes());
        cp.append(e);
      }

      cp.close();
    } catch (IOException ioe) {
      // This is a semantics change -- no start increments a counter
      fail("didn't throw no start exception");
    }
  }

  /**
   * error case: no stop message
   * 
   * TODO (jon) There is a possibility that we may leak memory over time if the
   * tag fails and is never cleaned up. (e.g. if we fail over to another
   * collector, this may stick around). A flush or ageoff is probably needed in
   * the long term.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testNoStop() throws IOException, InterruptedException {
    final int msgs = 100;
    MemorySinkSource mss = new MemorySinkSource();
    AckChecksumChecker<EventSink> cc = new AckChecksumChecker<EventSink>(mss);
    EventSinkDecorator<EventSink> dropStop = new EventSinkDecorator<EventSink>(
        cc) {
      int drop = msgs + 1; // intial + messages, the drop the last
      int count = 0;

      public void append(Event e) throws IOException, InterruptedException {
        if (count == drop) {
          count++;
          return;
        }
        count++;
        getSink().append(e);
      }
    };
    AckChecksumInjector<EventSink> cp = new AckChecksumInjector<EventSink>(
        dropStop);

    cp.open();
    for (int i = 0; i < msgs; i++) {
      Event e = new EventImpl(("this is a test " + i).getBytes());
      cp.append(e);
    }
    cp.close();

    // We don't detect an error here..
    // TODO (jon) some timeout mechanism?
  }

  /**
   * error case: duplicate/dropped message (bad checksum)
   * 
   * @throws InterruptedException
   */
  @Test
  public void testDupe() throws IOException, InterruptedException {
    final int msgs = 100;
    MemorySinkSource mss = new MemorySinkSource();
    AckChecksumChecker<EventSink> cc = new AckChecksumChecker<EventSink>(mss,
        new AckListener.Empty() {
          @Override
          public void err(String group) throws IOException {
            throw new IOException("Fail");
          }
        });
    EventSinkDecorator<EventSink> dupeMsg = new EventSinkDecorator<EventSink>(
        cc) {
      int drop = msgs / 2;
      int count = 0;

      public void append(Event e) throws IOException, InterruptedException {
        if (count == drop) {
          getSink().append(e); // extra message send.
        }
        count++;
        getSink().append(e);
      }
    };
    AckChecksumInjector<EventSink> cp = new AckChecksumInjector<EventSink>(
        dupeMsg);

    try {
      cp.open();
      for (int i = 0; i < 100; i++) {
        Event e = new EventImpl(("this is a test " + i).getBytes());
        cp.append(e);
      }
      cp.close();
    } catch (IOException ioe) {
      return;
    }

    LOG.info(cc.getMetrics().toJson());

    fail("should have failed");
  }

  /**
   * Make sure the builder works.
   */
  @Test
  public void testAckInjectorBuilderArgs() throws FlumeSpecException {
    FlumeBuilder.buildSink(new Context(), "{ ackInjector => null}");
  }

  /**
   * Throw error when builder encounters too many args.
   */
  @Test(expected = FlumeSpecException.class)
  public void testAckInjectorBuilderBadArgs() throws FlumeSpecException {
    FlumeBuilder.buildSink(new Context(), "{ ackInjector(false) => null}");
  }

  @Test
  public void testAckCheckerBuilderArgs() throws FlumeSpecException {
    FlumeBuilder.buildSink(new Context(), "{ackChecker => null}");
  }

  @Test(expected = FlumeSpecException.class)
  public void testAckCheckerBuilderBadArgs() throws FlumeSpecException {
    FlumeBuilder.buildSink(new Context(), "{ackChecker(false) => null}");
  }

  /**
   * Test insistent append metrics
   */
  @Test
  public void testStubbornAppendMetrics() throws JSONException,
      FlumeSpecException, IOException, InterruptedException {
    ReportTestUtils.setupSinkFactory();

    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "ackChecker one");
    ReportEvent rpt = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(rpt).toString());
    assertNotNull(rpt.getLongMetric(AckChecksumChecker.A_ACK_ENDS));
    assertNotNull(rpt.getLongMetric(AckChecksumChecker.A_ACK_FAILS));
    assertNotNull(rpt.getLongMetric(AckChecksumChecker.A_ACK_STARTS));
    assertNotNull(rpt.getLongMetric(AckChecksumChecker.A_ACK_SUCCESS));
    assertNotNull(rpt.getLongMetric(AckChecksumChecker.A_ACK_UNEXPECTED));
    assertEquals("One", rpt.getStringMetric("One.name"));

  }

}
