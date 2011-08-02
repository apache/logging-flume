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
package com.cloudera.flume.agent.diskfailover;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.rolling.ProcessTagger;
import com.cloudera.flume.handlers.rolling.TimeTrigger;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.BenchmarkHarness;

public class TestDiskFailoverDeco {
  @Before
  public void setup() {
    BenchmarkHarness.setupLocalWriteDir();
  }

  @After
  public void cleanup() {
    BenchmarkHarness.setupLocalWriteDir();

  }

  /**
   * Semantically, if an exception is thrown by an internal thread, the
   * exception should get percolated up to the appender.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testHandleExns() throws IOException, InterruptedException {
    EventSink msnk = mock(EventSink.class);
    doNothing().when(msnk).open();
    doNothing().when(msnk).close();
    doNothing().doThrow(new IOException("foo")).doNothing().when(msnk).append(
        Mockito.<Event> anyObject());
    doReturn(new ReportEvent("blah")).when(msnk).getReport();

    // cannot write to the same instance.
    Event e1 = new EventImpl(new byte[0]);
    Event e2 = new EventImpl(new byte[0]);
    Event e3 = new EventImpl(new byte[0]);

    EventSource msrc = mock(EventSource.class);
    doNothing().when(msrc).open();
    doNothing().when(msrc).close();

    when(msrc.next()).thenReturn(e1).thenReturn(e2).thenReturn(e3).thenReturn(
        null);

    DiskFailoverDeco<EventSink> snk = new DiskFailoverDeco<EventSink>(msnk,
        FlumeNode.getInstance().getDFOManager(), new TimeTrigger(
            new ProcessTagger(), 60000), 300000);
    snk.open();
    try {
      EventUtil.dumpAll(msrc, snk);
      snk.close();
    } catch (IOException ioe) {
      // expected the IO exception from the underlying sink to percolate up to
      // this append.

      return;
    }
    fail("Expected IO exception to percolate to this thread");

    // TODO (jon) sometimes a IllegalStateException is thrown, sometimes not.
    // Inconsistency is bad.
  }

  /**
   * These should be successful
   */
  @Test
  public void testBuilder() {
    DiskFailoverDeco.builder().build(new Context());
    DiskFailoverDeco.builder().build(new Context(), "1000");
  }

  /**
   * Fail (expected at most 1 arg
   */
  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail1() {
    DiskFailoverDeco.builder().build(new Context(), "12341", "foo");
  }

  /**
   * Fail (expected number)
   */
  @Test(expected = IllegalArgumentException.class)
  public void testBuilderFail2() {
    DiskFailoverDeco.builder().build(new Context(), "foo");
  }

}
