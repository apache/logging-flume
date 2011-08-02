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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportTestUtils;
import com.cloudera.flume.reporter.ReportUtil;

public class TestFanOutSink {
  public static final Logger LOG = LoggerFactory
      .getLogger(TestFanOutSink.class);

  /**
   * Verify that events are independent down each fanout path
   * 
   * @throws InterruptedException
   */
  @Test
  public void testIndependentEvents() throws FlumeSpecException, IOException,
      InterruptedException {

    String spec = "[ { value(\"foo\",\"bar\") => null}, { value(\"foo\",\"bar\" ) => null } ] ";
    EventSink snk = FlumeBuilder.buildSink(new Context(), spec);
    snk.open();
    snk.append(new EventImpl("event body".getBytes()));

  }

  /**
   * Test metrics
   * 
   * @throws FlumeSpecException
   */
  @Test
  public void testGetFanOutMetrics() throws JSONException, FlumeSpecException {
    ReportTestUtils.setupSinkFactory();

    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "[ one, two, three]");
    ReportEvent rpt = snk.getMetrics();
    LOG.info(ReportUtil.toJSONObject(rpt).toString());
    assertEquals(3, rpt.getLongMetric(FanOutSink.R_SUBSINKS).longValue());
    assertNull(rpt.getStringMetric("Fanout[0].name"));
    assertNull(rpt.getStringMetric("Fanout[1].name"));
    assertNull(rpt.getStringMetric("Fanout[2].name"));

    ReportEvent all = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(all).toString());
    assertEquals(3, rpt.getLongMetric(FanOutSink.R_SUBSINKS).longValue());
    assertEquals("One", all.getStringMetric("Fanout[0].name"));
    assertEquals("Two", all.getStringMetric("Fanout[1].name"));
    assertEquals("Three", all.getStringMetric("Fanout[2].name"));
  }

}
