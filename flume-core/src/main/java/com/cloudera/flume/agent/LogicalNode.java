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

package com.cloudera.flume.agent;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Driver;
import com.cloudera.flume.core.Driver.DriverState;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.connector.DirectDriver;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.master.StatusManager.NodeStatus;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.NetUtils;

/**
 * This is a logical node. This contains state about a node, it does not have
 * any specific RPC mechanism associated with it.
 * 
 * TODO (jon) this is a slight lie, FlumeConfigData is a thrift generate struct.
 * 
 * Here is how a configuration is loaded, and where errors are handled:
 * 
 * A call to checkConfig triggers a logicalNode update check based on info
 * provided by FlumeConfigData for the logical node. It is assumed that there
 * are not multiple concurrent checkConfig calls.
 * 
 * If the config needs to be updated, the logical node updates itself via
 * loadConfig. If there is a previous configuration the driver, its source and
 * its sink is first closed. Configuration for a source and a sink are
 * instantiated and then instantiated into a new Driver. Any
 * parsing/instantiation failures exit by throwing exceptions.
 * 
 * Currently a separate thread is used to reconfigure a node. A previous patch
 * made the logicalNode lazily open the source and sinks. This defers real open
 * errors into the main driver thread. This actually differs any open exceptions
 * into the driver thread which is actually a simplifies error handling.
 * 
 * An instantiated driver that pulls events out of the sources and into the
 * sink. Run this until an unhandled exception occurs or the source exits with
 * null signaling that it has been completely drained.
 * 
 * TODO (jon) This class is not consistently synchronized currently. Another
 * look at this code may be necessary.
 */
public class LogicalNode implements Reportable {
  static final Logger LOG = LoggerFactory.getLogger(LogicalNode.class);

  private FlumeConfigData lastGoodCfg = null;
  private Driver driver; // the connector that pumps data from src to snk
  private EventSink snk; // current sink and source instances.
  private EventSource src;
  private NodeStatus state;
  private String nodeName;
  private String nodeMsg;

  private Context ctx;

  // Largest value that is smaller than all legitimate versions
  public static final long VERSION_INFIMUM = -1;

  // metrics
  private AtomicLong reconfigures = new AtomicLong(0);

  public static final String A_RECONFIGURES = "reconfigures";

  /**
   * For JAXB
   */
  public LogicalNode() {

  }

  /**
   * Creates a logical node that will accept any configuration
   */
  public LogicalNode(Context ctx, String name) {
    this.nodeName = name;
    this.ctx = ctx;

    // Note: version and lastSeen aren't kept up-to-date on the logical node.
    // The master fills them in when it receives a NodeStatus heartbeat.
    state = new NodeStatus(NodeState.HELLO, 0, 0, NetUtils.localhost(),
        FlumeNode.getInstance().getPhysicalNodeName());
    // Set version to -1 so that all non-negative versions will be 'later'
    lastGoodCfg = new FlumeConfigData(0, "null", "null", VERSION_INFIMUM,
        VERSION_INFIMUM, FlumeConfiguration.get().getDefaultFlowName());
  }

  private void ensureClosedSourceSink(EventSource newSrc, EventSink newSnk)
      throws IOException, InterruptedException {
    if (newSnk != null) {
      snk = newSnk;
    }

    if (newSrc != null) {
      src = newSrc;
    }
  }

  /**
   * This opens the specified source and sink. If this is successful, it then
   * sets these as the node's source and sink, and starts a thread that pumps
   * source values into the sink. This will block until the previous driver has
   * shutdown but will return as the new driver is starting.
   */
  synchronized void loadNodeDriver(EventSource newSrc, EventSink newSnk)
      throws IOException, InterruptedException {
    ensureClosedSourceSink(newSrc, newSnk);
    startNodeDriver();
  }

  /**
   * This opens the specified source and sink. If this is successful, it then
   * sets these as the node's source and sink, and starts a thread that pumps
   * source values into the sink.
   * 
   * This will block for up to the given amount of time waiting for ACTIVE state
   * to be reached.
   * 
   * This is only used for testing.
   */
  synchronized void startNodeDriver(EventSource newSrc, EventSink newSink,
      long ms) throws IOException, InterruptedException {
    ensureClosedSourceSink(newSrc, newSink);
    startNodeDriver();
    getDriver().waitForState(DriverState.ACTIVE, ms);
  }

  /**
   * This stops any existing connection (source=>sink pumper), and then creates
   * a new one with the specified *already opened* source and sink arguments.
   */
  private void startNodeDriver() throws IOException {

    if (driver != null) {
      // stop the existing connector.
      driver.stop();
      try {
        // default is 30s.
        long timeout = FlumeConfiguration.get().getNodeCloseTimeout();
        if (!driver.join(timeout)) {
          LOG.error("Forcing driver to exit uncleanly");
          driver.cancel(); // taking too long, cancel the thread
        }
      } catch (InterruptedException e) {
        LOG.error("Previous driver took too long to close!", e);
      }
    }

    // this will be replaceable with multi-threaded queueing versions or other
    // mechanisms
    driver = new DirectDriver("logicalNode " + nodeName, src, snk);
    this.state.state = NodeState.ACTIVE;
    driver.start();
    reconfigures.incrementAndGet();
  }

  public void loadConfig(FlumeConfigData cfg) throws IOException,
      RuntimeException, FlumeSpecException {

    // got a newer configuration
    LOG.debug("Attempt to load config " + cfg);
    EventSink newSnk = null;
    EventSource newSrc = null;
    try {
      String errMsg = null;
      if (cfg.sinkConfig == null || cfg.sinkConfig.length() == 0) {
        errMsg = this.getName() + " - empty sink";
      }

      if (cfg.sourceConfig == null || cfg.sourceConfig.length() == 0) {
        errMsg = this.getName() + " - empty source";
      }

      if (errMsg != null) {
        LOG.info(errMsg);
        return; // Do nothing.
      }

      newSnk = FlumeBuilder.buildSink(ctx, cfg.sinkConfig);
      newSrc = FlumeBuilder.buildSource(ctx, cfg.sourceConfig);

      // TODO (jon) ERROR isn't quite right here -- the connection is in ERROR
      // but the previous connection is ok. Need to just add states to the
      // connections, and have each node maintain a list of connections.

      // This error conditions should not occur. One way we could have this
      // error is if the node does not have all plugins that the master has
      // installed. The master could then accept a particular source/sink but
      // the node would fail when attempting to instantiate it.
      if (newSnk == null) {
        LOG.error("failed to create sink config: " + cfg.sinkConfig);
        state.state = NodeState.ERROR;
        return;
      }

      if (newSrc == null) {
        LOG.error("failed to create sink config: " + cfg.sourceConfig);
        state.state = NodeState.ERROR;
        return;
      }

    } catch (RuntimeException e) {
      LOG.error("Runtime ex: " + new File(".").getAbsolutePath() + " " + cfg, e);
      state.state = NodeState.ERROR;
      throw e;
    } catch (FlumeSpecException e) {
      LOG.error(
          "FlumeSpecExn : " + new File(".").getAbsolutePath() + " " + cfg, e);
      state.state = NodeState.ERROR;
      throw e;
    }

    try {
      loadNodeDriver(newSrc, newSnk);

      // We have successfully opened the source and sinks for the config. We can
      // mark this as the last good / successful config. It does not mean that
      // this configuration will open without errors!
      this.lastGoodCfg = cfg;

      LOG.info("Node config successfully set to " + cfg);
    } catch (InterruptedException e) {
      // TODO figure out what to do on interruption
      LOG.error("Load Config interrupted", e);
    }
  }

  /**
   * Takes a FlumeConfigData and attempts load/config the node.
   * 
   * True if successful, false if failed
   */
  synchronized public boolean checkConfig(FlumeConfigData data) {

    if (data == null) {
      return false;
    }

    // check if config is too old or the same as current
    if (lastGoodCfg != null) {
      if (data.getSourceVersion() <= lastGoodCfg.getSourceVersion()) {
        if (data.getSourceVersion() < lastGoodCfg.getSourceVersion()) {
          // retrieved configuration is older!?
          LOG.warn("reject because config older than the current. ");
          return false;
        }

        LOG.debug("do nothing: retrieved config ("
            + new Date(data.getSourceVersion()) + ") same as current ("
            + new Date(lastGoodCfg.getSourceVersion()) + "). ");
        return false;
      }
    }

    try {
      loadConfig(data);
    } catch (Exception e) {
      // Catch the exception to prevent backoff
      LOG.warn("Configuration " + data + " failed to load!", e);
      return false;
    }
    return true;
  }

  @Deprecated
  synchronized public void getReports(Map<String, ReportEvent> reports) {
    String phyName = FlumeNode.getInstance().getPhysicalNodeName();
    String rprefix = phyName + "." + getName() + ".";

    if (snk != null) {
      snk.getReports(rprefix, reports);
    }
    if (src != null) {
      src.getReports(rprefix, reports);
    }
  }

  @Deprecated
  public synchronized ReportEvent getReport() {
    ReportEvent rpt = new ReportEvent(nodeName);
    rpt.setStringMetric("nodename", nodeName);
    rpt.setStringMetric("version", new Date(lastGoodCfg.timestamp).toString());
    rpt.setStringMetric("state", state.state.toString());
    rpt.setStringMetric("hostname", state.host);
    rpt.setStringMetric("sourceConfig", (lastGoodCfg.sourceConfig == null) ? ""
        : lastGoodCfg.sourceConfig);
    rpt.setStringMetric("sinkConfig", (lastGoodCfg.sinkConfig == null) ? ""
        : lastGoodCfg.sinkConfig);
    rpt.setStringMetric("message", nodeMsg);
    rpt.setLongMetric(A_RECONFIGURES, reconfigures.get());
    rpt.setStringMetric("physicalnode", state.physicalNode);

    if (snk != null) {
      rpt.hierarchicalMerge("sink", snk.getReport());
    }

    return rpt;
  }

  public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(nodeName);
    rpt.setStringMetric("nodename", nodeName);
    rpt.setStringMetric("version", new Date(lastGoodCfg.timestamp).toString());
    rpt.setStringMetric("state", driver.getState().toString());
    rpt.setStringMetric("hostname", state.host);
    rpt.setStringMetric("sourceConfig", (lastGoodCfg.sourceConfig == null) ? ""
        : lastGoodCfg.sourceConfig);
    rpt.setStringMetric("sinkConfig", (lastGoodCfg.sinkConfig == null) ? ""
        : lastGoodCfg.sinkConfig);
    rpt.setStringMetric("message", nodeMsg);
    rpt.setLongMetric(A_RECONFIGURES, reconfigures.get());
    rpt.setStringMetric("physicalnode", state.physicalNode);

    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    Map<String, Reportable> map = new HashMap<String, Reportable>();
    if (src != null) {
      map.put("source." + src.getName(), src);
    }
    if (snk != null) {
      map.put("sink." + snk.getName(), snk);
    }
    return map;
  }

  public String getName() {
    return nodeName;
  }

  public long getConfigVersion() {
    if (lastGoodCfg == null)
      return 0;
    return lastGoodCfg.getTimestamp();
  }

  public NodeStatus getStatus() {
    state.state = NodeState.valueOf(getDriver().getState().toString());
    return state;
  }

  /**
   * @return the driver responsible for opening/appending/closing
   */
  public Driver getDriver() {
    return driver;
  }

  /**
   * This is a synchronous close operation for the logical node.
   */
  public void close() throws IOException, InterruptedException {
    if (driver != null) {
      // stop the existing connector.
      nodeMsg = nodeName + "closing";
      driver.stop();
      // wait for driver thread to end.
      boolean success = driver.waitForAtLeastState(DriverState.IDLE, 30000);
      if (!success) {
        try {
          driver.cancel(); // signal driver to finish
          driver.join();
        } catch (InterruptedException e) {
          LOG.error("Unexpected interruption when closing logical node");
        }
      }
    }

  }

  /**
   * For testing only
   */
  public EventSink getSink() {
    return snk;
  }

  /**
   * For testing only
   */
  public EventSource getSource() {
    return src;
  }

}
