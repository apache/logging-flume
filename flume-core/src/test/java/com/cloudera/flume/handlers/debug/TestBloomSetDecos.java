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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.BloomCheckDecorator.BloomCheckState;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.bloom.BloomSet;

/**
 * These test the bloom set injector and bloom set checker decorators.
 * 
 * These are generally done on the order of 100M messages -- which is the size
 * of the current scaling test framework. This is cpu intensive and probably
 * should not be used in production, until its performance characteristics are
 * better understood.
 * 
 */
public class TestBloomSetDecos {

  @Test
  public void testBloomSetCompare() {
    BloomSet b1 = new BloomSet(10000, 5);
    BloomSet b2 = new BloomSet(10000, 5);

    for (int i = 0; i < 10; i++) {
      b1.addInt(i);
      if (i % 2 == 0)
        b2.addInt(i);
    }

    assertEquals(b1, b1);
    assertEquals(b2, b2);
    assertTrue(b1.contains(b2));
    assertFalse(b2.contains(b1));
  }

  /**
   * The parameters of the bloom filters are different so this should always
   * reject.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testBloomBadSizes() {
    BloomSet b1 = new BloomSet(10000, 5);
    BloomSet b2 = new BloomSet(10000, 6);

    for (int i = 0; i < 10; i++) {
      b1.addInt(i);
      b2.addInt(i);
    }

    assertFalse(b1.contains(b2));
    assertFalse(b2.contains(b1));
  }

  /**
   * Test it with larger number of events and larger number of slots. This
   * function may be fragile -- while this should always pass it doesn't reveal
   * what the false positive rate is.
   */
  @Test
  @Ignore("Takes too long to run")
  public void testBloomSetCompare100M() {
    // generally we want about 9-10 bits per entry.
    BloomSet b1 = new BloomSet(1000000000, 2); // 1B bits ~= 125MB
    BloomSet b2 = new BloomSet(1000000000, 2);

    // int drop = 543215432; // drop this one..
    // int drop = 543215431; // drop this one..
    int drop = 54323423;
    for (int i = 0; i < 100000000; i++) { // 100M "entries"
      b1.addInt(i);

      if (i != drop)
        // oops, we dropped one.
        b2.addInt(i);
    }

    assertTrue(b1.contains(b2));
    assertFalse(b2.contains(b1));
  }

  /** Test it with larger number of events and larger number of slots. */
  @Test
  @Ignore("Takes too long to run")
  public void testBloomSetCompare100Mx10M() {
    // generally we want about 9-10 bits per entry.
    BloomSet b1 = new BloomSet(1000000000, 2); // 1B bits ~= 125MB
    BloomSet b2 = new BloomSet(1000000000, 2);

    for (int i = 0; i < 100000000; i++) { // 100M "entries"
      if (i != 234000)
        b1.addInt(i); // drop one that is included in the other set.

      if (i <= 10000000)
        b2.addInt(i);

      // only add the first 10M to the second hash
    }

    assertFalse(b1.contains(b2)); // b1 doesn't have all b2 has!
    assertFalse(b2.contains(b1));
  }

  @Test
  public void testBloomGenBuilders() {
    SinkDecoBuilder b = BloomGeneratorDeco.builder();
    Context ctx = new Context();
    b.build(ctx, "2234", "123");
    b.build(ctx, "1234");
    b.build(ctx);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBloomGenBuilderFail() {
    SinkDecoBuilder b = BloomGeneratorDeco.builder();
    b.build(new Context(), "2234", "123", "r3414");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBloomGenBuilderFail2() {
    SinkDecoBuilder b = BloomGeneratorDeco.builder();
    b.build(new Context(), "r3414");
  }

  @Test
  public void testBloomCheckBuilders() {
    SinkDecoBuilder b = BloomGeneratorDeco.builder();
    Context ctx = new Context();
    b.build(ctx, "2234", "123");
    b.build(ctx, "1234");
    b.build(ctx);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBloomCheckBuilderFail() {
    SinkDecoBuilder b = BloomGeneratorDeco.builder();
    b.build(new Context(), "2234", "123", "r3414");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBloomCheckBuilderFail2() {
    SinkDecoBuilder b = BloomGeneratorDeco.builder();
    b.build(new Context(), "r3414");
  }

  @Test
  public void testBuildWithRptSink() throws FlumeSpecException {
    String spec = "{bloomCheck(1,2, \"text(\\\"test\\\")\") => null } ";
    FlumeBuilder.buildSink(new Context(), spec);
  }

  /**
   * Instantiate decos, run them and check their reports.
   * 
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testBloomDecos() throws FlumeSpecException, IOException,
      InterruptedException {
    String spec = "{ bloomGen(10000,2) => { bloomCheck(10000,2) => counter(\"test\")} } ";
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(), spec);
    EventSource src = FlumeBuilder.buildSource(LogicalNodeContext
        .testingContext(), "asciisynth(10000)");
    snk.open();
    src.open();
    EventUtil.dumpAll(src, snk);
    src.close();
    snk.close();

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("test");
    assertEquals(ctr.getCount(), 10000);

    // Hack until we get a better mechanism:
    BloomCheckDecorator bcd = (BloomCheckDecorator) (((EventSinkDecorator<EventSink>) snk)
        .getSink());
    ReportEvent r = bcd.getMetrics();
    assertEquals(BloomCheckState.SUCCESS.toString(), new String(r
        .get(BloomCheckDecorator.A_STATE)));
    assertEquals(1, Attributes.readInt(r, BloomCheckDecorator.A_SUCCESS)
        .intValue());
    assertEquals(0, Attributes.readInt(r, BloomCheckDecorator.A_FAILS)
        .intValue());
  }

  /**
   * Tests to make sure the report sink receives data.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testBloomReportSink() throws FlumeSpecException, IOException,
      InterruptedException {
    String spec = "{bloomGen(100,2) => {bloomCheck(100,2,\"counter(\\\"test\\\") \")  => counter(\"total\") } } }";
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(), spec);
    snk.open();
    snk.append(new EventImpl(new byte[0]));
    snk.append(new EventImpl(new byte[0]));

    CounterSink ctr = (CounterSink) ReportManager.get().getReportable("test");
    assertEquals(0, ctr.getCount());
    CounterSink total = (CounterSink) ReportManager.get()
        .getReportable("total");
    assertEquals(2, total.getCount());

    snk.close(); // will trigger a bloom report.
    assertEquals(1, ctr.getCount());
  }

}
