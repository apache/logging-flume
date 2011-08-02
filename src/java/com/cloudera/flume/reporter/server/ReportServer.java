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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.avro.FlumeReportAvro;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Attributes.Type;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.flume.util.ThriftServer;
import com.google.common.base.Preconditions;

/**
 * Serves reports over Thrift
 */
public class ReportServer extends ThriftServer implements
    FlumeReportServer.Iface {
  final static protected Logger LOG = Logger.getLogger(ReportServer.class);

  /**
   * Reads the port to start on from cfg, property REPORT_SERVER_PORT.
   */
  public ReportServer(FlumeConfiguration cfg) {
    this.port = cfg.getReportServerPort();
  }

  /**
   * Constructs a ReportServer to start on supplied port
   */
  public ReportServer(int port) {
    this.port = port;
  }

  /**
   * Turn a ReportEvent into a serializable object
   */
  static public FlumeReport reportToThrift(ReportEvent report) {
    Preconditions.checkNotNull(report, "reportToThrift: report is null");
    Map<String, String> stringMap = new HashMap<String, String>(report
        .getAllStringMetrics());
    Map<String, Double> doubleMap = new HashMap<String, Double>(report
        .getAllDoubleMetrics());
    Map<String, Long> longMap = new HashMap<String, Long>(report
        .getAllLongMetrics());
    for (String k : report.getAttrs().keySet()) {
      Type t = Attributes.getType(k);

      // If there's nothing in the Attributes table, guess at String
      // When the Attributes table goes away this won't be necessary.
      if (t == null) {
        t = Type.STRING;
      }
      switch (t) {
      case DOUBLE:
        doubleMap.put(k, Attributes.readDouble(report, k));
        break;
      case STRING:
        stringMap.put(k, Attributes.readString(report, k));
        break;
      case LONG:
        longMap.put(k, Attributes.readLong(report, k));
        break;
      default:
        LOG.warn("Unknown type " + t);
      }
    }
    return new FlumeReport(stringMap, longMap, doubleMap);
  }

  /**
   * Turn a ReportEvent into a serializable object
   */
  static public FlumeReportAvro reportToAvro(ReportEvent report) {
    Preconditions.checkNotNull(report, "reportToThrift: report is null");
    Map<String, String> stringMap = new HashMap<String, String>(report
        .getAllStringMetrics());
    Map<String, Double> doubleMap = new HashMap<String, Double>(report
        .getAllDoubleMetrics());
    Map<String, Long> longMap = new HashMap<String, Long>(report
        .getAllLongMetrics());
    for (String k : report.getAttrs().keySet()) {
      Type t = Attributes.getType(k);

      // If there's nothing in the Attributes table, guess at String
      // When the Attributes table goes away this won't be necessary.
      if (t == null) {
        t = Type.STRING;
      }
      switch (t) {
      case DOUBLE:
        doubleMap.put(k, Attributes.readDouble(report, k));
        break;
      case STRING:
        stringMap.put(k, Attributes.readString(report, k));
        break;
      case LONG:
        longMap.put(k, Attributes.readLong(report, k));
        break;
      default:
        LOG.warn("Unknown type " + t);
      }
    }

    FlumeReportAvro out = new FlumeReportAvro();

    Map<CharSequence, CharSequence> stringMapUtf = new HashMap<CharSequence, CharSequence>();
    for (String s : stringMap.keySet()) {
      stringMapUtf.put(s, stringMap.get(s));
    }

    Map<CharSequence, Double> doubleMapUtf = new HashMap<CharSequence, Double>();
    for (String s : doubleMap.keySet()) {
      doubleMapUtf.put(s, doubleMap.get(s));
    }

    Map<CharSequence, Long> longMapUtf = new HashMap<CharSequence, Long>();
    for (String s : longMap.keySet()) {
      longMapUtf.put(s, longMap.get(s));
    }

    out.stringMetrics = stringMapUtf;
    out.doubleMetrics = doubleMapUtf;
    out.longMetrics = longMapUtf;
    return out;
  }

  /**
   * Thrift interface: returns a serializable report with given name or null if
   * report doesn't exist
   */
  @Override
  public FlumeReport getReportByName(String reportName) throws TException {
    ReportManager reportManager = ReportManager.get();
    Map<String, Reportable> reports = reportManager.getReportables();
    if (reports.containsKey(reportName)) {
      return reportToThrift(reports.get(reportName).getReport());
    }

    return null;
  }

  /**
   * Thrift interface: returns a map of reports in serializable form
   */
  @Override
  public Map<String, FlumeReport> getAllReports() throws TException {
    Map<String, FlumeReport> retMap = new HashMap<String, FlumeReport>();

    ReportManager reportManager = ReportManager.get();
    Map<String, Reportable> reports = reportManager.getReportables();

    for (Entry<String, Reportable> e : reports.entrySet()) {
      FlumeReport report = reportToThrift(e.getValue().getReport());
      retMap.put(e.getKey(), report);
    }
    return retMap;
  }

  /**
   * Starts the Thrift server
   */
  public void serve() throws TTransportException {
    LOG.info("Starting ReportServer...");
    TProcessor processor = new FlumeReportServer.Processor(this);
    this.start(processor, port, "Flume Report Server");
    LOG.info("ReportServer started on port " + port);
  }

  /**
   * Stops the Thrift server
   */
  public void stop() {
    LOG.info("Stopping ReportServer...");
    super.stop();
    LOG.info("ReportServer stopped");
  }
}
