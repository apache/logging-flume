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

package com.cloudera.flume.reporter;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LogicalNode;
import com.cloudera.flume.agent.MasterRPC;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.Clock;

/**
 * Polls the ReportManager for all reports (period configured by
 * REPORTER_POLLER_PERIOD) and pushes them to a Master server.
 */
@Deprecated
public class MasterReportPusher {

  static final Logger LOG = LoggerFactory.getLogger(MasterReportPusher.class);

  final FlumeConfiguration cfg;
  final ReportManager rptMan;
  final MasterRPC masterRPC;
  final PusherThread pusherThread = new PusherThread();

  volatile boolean shutdown = false;

  /**
   * Constructs a new MasterReportPusher that will read REPORTER_POLLER_PERIOD
   * from the supplied configuration, reports from rptMan and push them via
   * masterRPC.
   */
  public MasterReportPusher(FlumeConfiguration cfg, ReportManager rptMan,
      MasterRPC rpcMan) {
    this.cfg = cfg;
    this.rptMan = rptMan;
    this.masterRPC = rpcMan;
  }

  /**
   * Signals the PusherThread to stop.
   */
  protected void doShutdown() {
    shutdown = true;
  }

  /**
   * Starts the report pusher thread
   */
  public void start() {
    pusherThread.start();
  }

  /**
   * Stops the report pusher thread. Does not wait until complete. TODO: add
   * option to wait until done.
   */
  public void stop() {
    doShutdown();
  }

  /**
   * Thread to do the periodic pushing work. Every report is pushed with a name
   * that
   */
  class PusherThread extends Thread {
    void queryReportMan(Map<String, ReportEvent> reports) {
      Map<String, Reportable> reportables = rptMan.getReportables();
      for (Entry<String, Reportable> e : reportables.entrySet()) {
        reports.put(e.getKey(), e.getValue().getMetrics());
      }
    }

    void querySrcSinkReports(Map<String, ReportEvent> reports) {
      Collection<LogicalNode> lnodes = FlumeNode.getInstance()
          .getLogicalNodeManager().getNodes();
      for (LogicalNode n : lnodes) {
        n.getReports(reports);
      }
    }

    void sendReports() throws IOException {
      Map<String, ReportEvent> reports = new HashMap<String, ReportEvent>();

      queryReportMan(reports);
      querySrcSinkReports(reports);

      masterRPC.putReports(reports);
    }

    public void run() {
      try {
        while (!shutdown) {
          Clock.sleep(cfg.getReporterPollPeriod());
          sendReports();
        }
      } catch (InterruptedException e) {
        LOG.warn("MasterReportPusher.PusherThread was interrupted", e);
      } catch (IOException e) {
        LOG.error("IOException in MasterReportPusher.PusherThread", e);
      }
    }
  }
}
