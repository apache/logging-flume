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

package com.cloudera.flume.master;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.Clock;

/**
 * This manages the last seen and heartbeat data on the master flume
 * configuration node.
 * 
 * There should be no references to Thrift here.
 */
public class StatusManager implements Reportable {

  static final Logger LOG = LoggerFactory.getLogger(StatusManager.class);

  @XmlRootElement
  public static class NodeStatus {
    public NodeState state;
    public long version;
    public long lastseen;
    public String host;
    public String physicalNode;

    public NodeStatus() {

    }

    public NodeStatus(NodeState state, long version, long lastseen,
        String host, String physicalNode) {
      this.state = state;
      this.version = version;
      this.lastseen = lastseen;
      this.physicalNode = physicalNode;
      this.host = host;
    }

    @Override
    public String toString() {
      return "node status (state " + state + ", version " + new Date(version)
          + ", lastseen " + new Date(lastseen) + ", host " + host + ")";
    }
  }

  /**
   * This is the state that the master thinks the node is in. This is a super
   * set of the DriverState, which is the state that the node's driver thinks it
   * is in.
   */
  public enum NodeState {
    HELLO, OPENING, ACTIVE, CLOSING, IDLE, ERROR, LOST, DECOMMISSIONED
  };

  // runtime state of the flume system
  final Map<String, NodeStatus> statuses = new HashMap<String, NodeStatus>();

  public boolean updateHeartbeatStatus(String host, String physicalNode,
      String logicalNode, NodeState stat, long version) {
    LOG.debug("Heartbeat from host:" + host + " logicalnode:" + logicalNode
        + " (" + stat + "," + new Date(version) + ")");

    boolean configChanged = false;
    synchronized (statuses) {

      String expectedPhys = FlumeMaster.getInstance().getSpecMan()
          .getPhysicalNode(logicalNode);

      if (expectedPhys == null || !expectedPhys.equals(physicalNode)) {
        stat = NodeState.DECOMMISSIONED;
      }

      NodeStatus status = statuses.get(logicalNode);
      if (status == null) {
        status = new NodeStatus(stat, version, Clock.unixTime(), "", "");
        configChanged = true;
      }

      status.state = stat;
      status.version = version;
      status.lastseen = Clock.unixTime();
      status.host = host;
      status.physicalNode = physicalNode;
      statuses.put(logicalNode, status);
    }

    return configChanged;
  }

  /**
   * checks to see if any node's last heart beat was too long ago
   */
  public void checkup() {
    long now = Clock.unixTime();
    HashMap<String, NodeStatus> ss = null;
    synchronized (statuses) {
      ss = new HashMap<String, NodeStatus>(statuses);
    }
    // max out to missing 10 heart beats
    int maxMissed = FlumeConfiguration.get().getMasterMaxMissedheartbeats();
    long timeout = FlumeConfiguration.get().getConfigHeartbeatPeriod()
        * maxMissed;
    for (Entry<String, NodeStatus> e : ss.entrySet()) {
      NodeStatus ns = e.getValue();
      long delta = now - ns.lastseen;
      if (delta > timeout) {
        ns.state = NodeState.LOST;
      }

      // Check mapping exists, if it doesn't set status to DECOMMISSIONED
      String expectedPhys = FlumeMaster.getInstance().getSpecMan()
          .getPhysicalNode(e.getKey());
      if (expectedPhys == null || !expectedPhys.equals(ns.physicalNode)) {
        ns.state = NodeState.DECOMMISSIONED;
      }

    }
  }

  @Override
  public String getName() {
    return "Status";
  }

  /**
   * TODO (jon) convert to a report
   */
  @Override
  public ReportEvent getMetrics() {
    StringBuilder status = new StringBuilder();
    status.append("<div class=\"StatusManager\">");
    status.append("<h2>Node status</h2>\n<table border=\"1\">"
        + "<tr><th>logical node</th><th>physical node</th><th>host name</th>"
        + "<th>status</th><th>version</th><th>last seen delta (s)</th>"
        + "<th>last seen</th></tr>");

    long now = Clock.unixTime();
    SortedMap<String, NodeStatus> sorted = new TreeMap<String, NodeStatus>(
        getNodeStatuses());
    for (Entry<String, NodeStatus> e : sorted.entrySet()) {
      status.append("\n<tr>");
      NodeStatus v = e.getValue();
      String version = (v.version == 0) ? "none" : new Date(v.version)
          .toString();
      status.append("<td>" + e.getKey() + "</td>");
      status.append("<td>" + v.physicalNode + "</td>");
      status.append("<td>" + v.host + "</td>");

      status.append("<td>" + v.state + "</td>");
      status.append("<td>" + version + "</td>");
      status.append("<td>" + ((now - v.lastseen) / 1000));
      status.append("<td>" + new Date(v.lastseen) + "</td>");
      status.append("</tr>");
    }
    status.append("</table>");
    status.append("</div>");

    return ReportEvent.createLegacyHtmlReport("", status.toString());
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    return ReportUtil.noChildren();
  }

  /**
   * Returns a copy of the hashmap containing the mapping from node names to
   * their status object.
   */
  public Map<String, NodeStatus> getNodeStatuses() {
    HashMap<String, NodeStatus> ss = null;
    synchronized (statuses) {
      ss = new HashMap<String, NodeStatus>(statuses);
    }
    return ss;
  }

  /**
   * Returns the NodeStatus of for the given node name
   */
  public NodeStatus getStatus(String node) {
    synchronized (statuses) {
      return statuses.get(node);
    }
  }

  /**
   * Purge a status entry for the specified logical node
   */
  public boolean purge(String node) {
    NodeStatus val;
    synchronized (statuses) {
      val = statuses.remove(node);
    }
    return val != null;
  }

  /**
   * Purge all status entries from the logical node status table.
   */
  public void purgeAll() {
    synchronized (statuses) {
      statuses.clear();
    }
  }
}
