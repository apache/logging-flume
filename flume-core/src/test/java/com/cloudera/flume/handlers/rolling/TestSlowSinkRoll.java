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
package com.cloudera.flume.handlers.rolling;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.codehaus.jettison.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.conf.SinkFactoryImpl;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.handlers.hdfs.EscapedCustomDfsSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.ReportTestUtils;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

public class TestSlowSinkRoll {
  public static final Logger LOG = LoggerFactory.getLogger(TestSlowSinkRoll.class);
  public static final String NUM_EVENTS = "num_events";
  public class DummySource extends EventSource.Base {
    private static final int DEF_MAX_EVENTS = 10;
    private final int maxEvents;

    long counter;
    public DummySource() {
      maxEvents = DEF_MAX_EVENTS;
    }

    public DummySource(int maxEv) {
      maxEvents = maxEv;
    }

    @Override
    public Event next() throws InterruptedException {
      if (counter == maxEvents) {
        throw new InterruptedException("Max events exceeded");
      }
      counter++;
      LOG.info("Generated event <junk" + counter + ">");
      return new EventImpl(("junk" + counter + " ").getBytes());
    }

    @Override
    public void close() throws InterruptedException {
      LOG.info("close");
    }

    @Override
    public void open() throws RuntimeException {
      LOG.info("open");
    }

    public Long getCount() {
      return counter;
    }
  };

  @Before
  public void setDebug() {
    // log4j specific debugging level
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  // the sink has a long delay, increase the roller's timeout and make sure that there
  // are no events lost
  @Test
  public void testLongTimeout() throws IOException, InterruptedException {
    final File f = FileUtil.mktempdir();
    Logger rollLog = LoggerFactory.getLogger(RollSink.class);

    RollSink snk = new RollSink(new Context(), "test", 2000, 250) {
      @Override
      protected EventSink newSink(Context ctx) throws IOException {
        return new EscapedCustomDfsSink(ctx, "file:///" + f.getPath(),
            "sub-%{service}%{rolltag}") {
          @Override
          public void append(final Event e) throws IOException, InterruptedException {
            super.append(e);
            Clock.sleep(1500);
          }
        };
      }

      @Override
      synchronized public ReportEvent getMetrics() {
        // the EvenSink getMetrics doesn't report num events, so use getReport() for now
        ReportEvent rpt = super.getReport();
        long cnt = rpt.getLongMetric(EventSink.Base.R_NUM_EVENTS);
        rpt.setLongMetric(NUM_EVENTS, cnt);
        return rpt;
      }
    };
    snk.setTimeOut(2000);

    DummySource source = new DummySource(7);
    DirectDriver driver = new DirectDriver(source, snk);
    driver.start();
    Clock.sleep(12000);
    driver.stop();

    assertEquals(snk.getMetrics().getLongMetric(NUM_EVENTS), source.getCount());
  }

  // the sink has a long delay, make sure that slow append gets aborted by roller
  @Test
  public void testSlowSinkRoll() throws IOException, InterruptedException {
    final File f = FileUtil.mktempdir();
    final AtomicBoolean firstPass = new AtomicBoolean(true);

    RollSink snk = new RollSink(new Context(), "test", 1000, 250) {
      @Override
      protected EventSink newSink(Context ctx) throws IOException {
        return new EscapedCustomDfsSink(ctx, "file:///" + f.getPath(),
            "sub-%{service}%{rolltag}") {
          @Override
          public void append(final Event e) throws IOException, InterruptedException {
            super.append(e);
            if (firstPass.get()) {
              firstPass.set(false);
              Clock.sleep(3000);
            }
          }
        };
      }
    };

    DummySource source = new DummySource(4);
    DirectDriver driver = new DirectDriver(source, snk);
    driver.start();
    Clock.sleep(6000);
    driver.stop();
    assertTrue(snk.getMetrics().getLongMetric(RollSink.A_ROLL_ABORTED_APPENDS) > Long.valueOf(0));
  }

  // the sink has a long delay and roll is configured to wait (timeout is 0 )
  // make sure that roller waited for appends and there are no aborts
  @Test
  public void testWaitingSlowSinkRoll() throws IOException, InterruptedException {
    final File f = FileUtil.mktempdir();

    RollSink snk = new RollSink(new Context(), "test", 2000, 250) {
      @Override
      protected EventSink newSink(Context ctx) throws IOException {
        return new EscapedCustomDfsSink(ctx, "file:///" + f.getPath(),
            "sub-%{service}%{rolltag}") {
          @Override
          public void append(final Event e) throws IOException, InterruptedException {
            super.append(e);
            Clock.sleep(1500);
          }
        };
      }
    };
    snk.setTimeOut(0);
    DummySource source = new DummySource(4);
    DirectDriver driver = new DirectDriver(source, snk);
    driver.start();
    Clock.sleep(12200);
    driver.stop();
    assertEquals(snk.getMetrics().getLongMetric(RollSink.A_ROLL_ABORTED_APPENDS), Long.valueOf(0));
  }

}
