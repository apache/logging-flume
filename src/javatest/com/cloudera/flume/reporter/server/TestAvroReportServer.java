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

package com.cloudera.flume.reporter.server;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.util.Utf8;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Attributes.Type;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.reporter.server.avro.AvroFlumeReportServer;
import com.cloudera.flume.reporter.server.avro.AvroFlumeReport;

/**
 * Test cases for the Avro-based report server
 */
public class TestAvroReportServer {
  protected static final Logger LOG = LoggerFactory
      .getLogger(TestAvroReportServer.class);
  AvroReportServer reportServer;
  final static int PORT = 23456;
  protected AvroFlumeReportServer avroClient;
  URL url;
  HttpTransceiver transport;

  /**
   * Create a new ReportServer and add a single report to it
   */
  @Before
  public void startReportServer() throws IOException {
    final ReportEvent reportEvent = new ReportEvent("test");

    reportServer = new AvroReportServer(PORT);
    reportServer.serve();
    url = new URL("http", "localhost", PORT, "/");
    reportEvent.setDoubleMetric("doubleAttr", .5);
    Attributes.register("doubleAttr", Type.DOUBLE);
    Reportable reportable = new Reportable() {
      @Override
      public String getName() {
        return "reportable1";
      }

      @Override
      public ReportEvent getReport() {
        return reportEvent;
      }
    };

    ReportManager.get().add(reportable);
  }

  @After
  public void stopReportServer() {
    reportServer.stop();
  }

  @Test
  public void testGetAllReports() throws IOException {

    transport = new HttpTransceiver(url);
    try {
      this.avroClient = (AvroFlumeReportServer) SpecificRequestor.getClient(
          AvroFlumeReportServer.class, transport);
    } catch (Exception e) {
      throw new IOException("Failed to open AvroReportServer at " + PORT
          + " : " + e.getMessage());
    }

    Map<CharSequence, AvroFlumeReport> ret = avroClient.getAllReports();
    assertTrue(ret.size() == 1);

    AvroFlumeReport rep = ret.get(new Utf8("reportable1"));

    assertNotNull("Report 'reportable1' not found", rep);
    assertTrue("Expected exactly one double metric",
        rep.doubleMetrics.size() == 1);
    assertTrue("Expected double metric to be equal to 0.5", rep.doubleMetrics
        .get(new Utf8("doubleAttr")) == 0.5);
  }

  @Test
  public void testGetReportByName() throws IOException {
    transport = new HttpTransceiver(url);
    try {
      this.avroClient = (AvroFlumeReportServer) SpecificRequestor.getClient(
          AvroFlumeReportServer.class, transport);
    } catch (Exception e) {
      throw new IOException("Failed to open AvroReportServer at " + PORT
          + " : " + e.getMessage());
    }

    AvroFlumeReport rep = avroClient.getReportByName("reportable1");

    assertNotNull("Report 'reportable1' not found", rep);
    assertTrue("Expected exactly one double metric",
        rep.doubleMetrics.size() == 1);
    assertTrue("Expected double metric to be equal to 0.5", rep.doubleMetrics
        .get(new Utf8("doubleAttr")) == 0.5);
  }

  /**
   * Standard server test, make sure that open and close repeatedly don't throw.
   */
  @Test
  public void testOpenClose() throws IOException {
    // stop the current reportserver, which is opened at the default PORT
    reportServer.stop();

    for (int i = 0; i < 20; ++i) {
      reportServer = new AvroReportServer(PORT + i);
      reportServer.serve();
      reportServer.stop();
    }
  }
}
