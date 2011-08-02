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

import static org.junit.Assert.*;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.aggregator.CounterSink;

/**
 * Demonstrates that lazy open defers until append happens to actually open.
 */
public class TestLazyOpen {
  public static final Logger LOG = LoggerFactory.getLogger(TestLazyOpen.class);

  static class OpenInstanceCountingSink extends EventSink.Base {
    int opened = 0;
    int closed = 0;

    @Override
    public void append(Event e) throws IOException {
    }

    @Override
    public void close() throws IOException {
      closed++;
      LOG.info("actually closed");
    }

    @Override
    public void open() throws IOException {
      opened++;
      LOG.info("actually open happened now");
    }
  }

  @Test
  public void testLazyOpen() throws IOException {
    OpenInstanceCountingSink snk = new OpenInstanceCountingSink();

    LazyOpenDecorator<EventSink> lazy = new LazyOpenDecorator<EventSink>(snk);
    lazy.open();
    LOG.info("lazy decorator opened");
    assertEquals(0, snk.opened);

    LOG.info("appending");
    Event e = new EventImpl("foo".getBytes());
    lazy.append(e);
    assertEquals(1, snk.opened);
    LOG.info("done");
  }

  @Test
  public void testLazyClosed() throws IOException {
    OpenInstanceCountingSink snk = new OpenInstanceCountingSink();

    LazyOpenDecorator<EventSink> lazy = new LazyOpenDecorator<EventSink>(snk);
    lazy.open();
    lazy.close();
    LOG.info("lazy decorator opened");
    assertEquals(0, snk.closed);

    LOG.info("appending");
    Event e = new EventImpl("foo".getBytes());
    lazy.open();
    lazy.append(e);
    lazy.close();
    assertEquals(1, snk.opened);
    LOG.info("done");
  }

  /**
   * Tests the lazy open through another mechanism, and tests the builder
   */
  @Test
  public void testLazyOpenBuild() throws IOException, FlumeSpecException {
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "{ lazyOpen => counter(\"count\") } ");
    CounterSink cnt = (CounterSink) ReportManager.get().getReportable("count");

    boolean ok = false;
    Event e = new EventImpl("event".getBytes());
    snk.open();
    try {
      cnt.append(e);
    } catch (Exception ex) {
      ok = true;
      // should be thrown because not actually open
    }
    assertTrue(ok);

    snk.append(e);

    assertEquals(1, cnt.getCount());
    snk.close();
  }
}
