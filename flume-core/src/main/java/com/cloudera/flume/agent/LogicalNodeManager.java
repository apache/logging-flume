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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.ReportTestingContext;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportManager;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.Clock;
import com.cloudera.util.MultipleIOException;

/**
 * This maintains a set of the logical nodes in the process.
 */
public class LogicalNodeManager implements Reportable {
  protected static final Logger LOG = LoggerFactory
      .getLogger(LogicalNodeManager.class);

  Map<String, LogicalNode> threads = new ConcurrentHashMap<String, LogicalNode>();
  String physicalNode;

  public LogicalNodeManager(String physicalNode) {
    this.physicalNode = physicalNode;
  }

  synchronized public Collection<LogicalNode> getNodes() {
    return new ArrayList<LogicalNode>(threads.values());
  }

  /**
   * Give a logical node name, and src and a sink spec, generate a new logical
   * node.
   */
  synchronized public void spawn(String name, String src, String snk)
      throws IOException, FlumeSpecException {
    Context ctx = new LogicalNodeContext(physicalNode, name);
    spawn(ctx, name, FlumeBuilder.buildSource(ctx, src), new CompositeSink(ctx,
        snk));
  }

  /**
   * Give a logical node name, and src and a sink spec, generate a new logical
   * node. This gives assumes the current version and is ONLY used for testing.
   */
  synchronized public void testingSpawn(String name, String src, String snk)
      throws IOException, FlumeSpecException {
    LogicalNode nd = threads.get(name);
    if (nd == null) {
      Context ctx = new ReportTestingContext(new LogicalNodeContext(
          physicalNode, name));

      LOG.info("creating new logical node " + name);
      nd = new LogicalNode(ctx, name);
      threads.put(nd.getName(), nd);
    }
    long ver = Clock.unixTime();
    FlumeConfigData fcd = new FlumeConfigData(ver, src, snk, ver, ver,
        FlumeConfiguration.get().getDefaultFlowName());
    nd.loadConfig(fcd);

  }

  // TODO (jon) make private
  synchronized void spawn(Context ctx, String name, EventSource src,
      EventSink snk) throws IOException {
    LogicalNode nd = threads.get(name);
    if (nd == null) {
      LOG.info("creating new logical node " + name);
      nd = new LogicalNode(ctx, name);
      threads.put(nd.getName(), nd);
    }

    try {
      nd.loadNodeDriver(src, snk);
    } catch (InterruptedException e) {
      // TODO verify this is reasonable behavior
      LOG.error("spawn was interrupted", e);
    }

  }

  /**
   * Turn a logical node off on a node
   */
  synchronized public void decommission(String name) throws IOException,
      InterruptedException {
    LogicalNode nd = threads.remove(name);
    if (nd == null) {
      throw new IOException("Attempting to decommission node '" + name
          + "' but it does not exist!");
    }
    ReportManager.get().remove(nd);
    nd.close();

  }

  @Deprecated
  public ReportEvent getReport() {
    ReportEvent rpt = new ReportEvent(getName());
    for (LogicalNode t : threads.values()) {
      rpt.hierarchicalMerge(t.getName(), t.getReport());
    }
    return rpt;
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(getName());
    // TODO add number of nodes present.
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    Collection<LogicalNode> copy = getNodes();
    Map<String, Reportable> map = new HashMap<String, Reportable>(copy.size());
    for (LogicalNode ln : copy) {
      map.put(ln.getName(), ln);
    }
    return map;
  }

  public String getPhysicalNodeName() {
    return physicalNode;
  }

  @Override
  public String getName() {
    return "LogicalNodeManager";
  }

  synchronized public LogicalNode get(String ln) {
    return threads.get(ln);
  }

  /**
   * Decommission all logical nodes except for those found in the excludes list.
   */
  synchronized public void decommissionAllBut(List<String> excludes)
      throws IOException, InterruptedException {
    Set<String> decoms = new HashSet<String>(threads.keySet()); // copy keyset
    decoms.removeAll(excludes);

    List<IOException> exns = new ArrayList<IOException>();
    for (String ln : decoms) {
      try {
        decommission(ln);
      } catch (IOException e) {
        exns.add(e);
      }
    }

    if (exns.size() > 0) {
      throw MultipleIOException.createIOException(exns);
    }

  }
}
