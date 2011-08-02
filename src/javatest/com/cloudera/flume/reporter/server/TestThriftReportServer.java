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

import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Attributes.Type;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.reporter.server.thrift.ThriftFlumeReport;
import com.cloudera.flume.reporter.server.thrift.ThriftFlumeReportServer;

/**
 * Test cases for the Thrift-based report server
 */
public class TestThriftReportServer {
  protected static final Logger LOG = LoggerFactory
      .getLogger(TestThriftReportServer.class);
  ThriftReportServer reportServer;
  final static int PORT = 23456;

  /**
   * Create a new ThriftReportServer and add a single report to it
   */
  @Before
  public void startReportServer() throws TTransportException {
    final ReportEvent reportEvent = new ReportEvent("test");

    reportServer = new ThriftReportServer(PORT);
    reportServer.serve();

    reportEvent.setDoubleMetric("doubleAttr", .5);
    Attributes.register("doubleAttr", Type.DOUBLE);
    Reportable reportable = new Reportable() {
      @Override
      public String getName() {
        return "reportable1";
      }

      @Override
      public ReportEvent getMetrics() {
        return reportEvent;
      }

      @Override
      public Map<String, Reportable> getSubMetrics() {
        return ReportUtil.noChildren();
      }
    };

    ReportManager.get().add(reportable);
  }

  @After
  public void stopReportServer() {
    reportServer.stop();
  }

  @Test
  public void testGetAllReports() throws TException {
    TTransport transport = new TSocket("localhost", PORT);
    TProtocol protocol = new TBinaryProtocol(transport);
    transport.open();

    ThriftFlumeReportServer.Client client = new ThriftFlumeReportServer.Client(
        protocol);
    Map<String, ThriftFlumeReport> ret = client.getAllReports();
    assertTrue(ret.size() == 1);
    ThriftFlumeReport rep = ret.get("reportable1");

    assertNotNull("Report 'reportable1' not found", rep);
    assertTrue("Expected exactly one double metric",
        rep.getDoubleMetricsSize() == 1);
    assertTrue("Expected double metric to be equal to 0.5", rep
        .getDoubleMetrics().get("doubleAttr") == 0.5);
  }

  @Test
  public void testGetReportByName() throws TException {
    TTransport transport = new TSocket("localhost", PORT);
    TProtocol protocol = new TBinaryProtocol(transport);
    transport.open();
    ThriftFlumeReportServer.Client client = new ThriftFlumeReportServer.Client(
        protocol);

    ThriftFlumeReport rep = client.getReportByName("reportable1");

    assertNotNull("Report 'repotable1' not found", rep);
    assertTrue("Expected exactly one double metric",
        rep.getDoubleMetricsSize() == 1);
    assertTrue("Expected double metric to be equal to 0.5", rep
        .getDoubleMetrics().get("doubleAttr") == 0.5);
  }

  /**
   * Standard server test, make sure that open and close repeatedly don't throw.
   */
  @Test
  public void testOpenClose() throws TException {
    // stop the current reportserver, which is opened at the default PORT
    reportServer.stop();

    for (int i = 0; i < 20; ++i) {
      reportServer = new ThriftReportServer(PORT + i);
      reportServer.serve();
      reportServer.stop();
    }
  }
}
