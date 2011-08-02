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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.antlr.runtime.RecognitionException;
import org.codehaus.jettison.json.JSONException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.avro.AvroDataFileOutputFormat;
import com.cloudera.flume.handlers.avro.AvroJsonOutputFormat;
import com.cloudera.flume.handlers.text.output.RawOutputFormat;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;

public class TestTextFileSink {

  public static final Logger LOG = LoggerFactory
      .getLogger(TestTextFileSink.class);

  @Test
  public void testTextKWArg() throws RecognitionException, FlumeSpecException {
    String s = "text(\"filename\", format=\"raw\")";
    TextFileSink snk = (TextFileSink) FlumeBuilder.buildSink(new Context(), s);
    assertTrue(snk.getFormat() instanceof RawOutputFormat);

    s = "text(\"filename\", format=\"avrodata\")";
    snk = (TextFileSink) FlumeBuilder.buildSink(new Context(), s);
    assertTrue(snk.getFormat() instanceof AvroDataFileOutputFormat);

    s = "text(\"filename\", format=\"avrojson\")";
    snk = (TextFileSink) FlumeBuilder.buildSink(new Context(), s);
    assertTrue(snk.getFormat() instanceof AvroJsonOutputFormat);
  }

  /**
   * Test insistent append metrics
   */
  @Test
  public void testTextFileMetrics() throws JSONException, FlumeSpecException,
      IOException, InterruptedException {
    EventSink snk = FlumeBuilder.buildSink(new ReportTestingContext(),
        "text(\"filename\")");
    ReportEvent rpt = ReportUtil.getFlattenedReport(snk);
    LOG.info(ReportUtil.toJSONObject(rpt).toString());
    assertNotNull(rpt.getLongMetric(ReportEvent.A_COUNT));
  }

}
