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
package com.cloudera.flume.master.availability;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;

/**
 * This interface for algorithm implementations that generate a spec with
 * failover lists. Implementations are responsible for maintaining state.
 * 
 * In the current incarnation, given a set of collectors, this calculates an
 * ordered list of collectors for a particular agent. The interface is pull only
 * -- it calculates information and returns information when requested or
 * notified of changes. Other mechanism should be responsible for polling for
 * the changes.
 * 
 * This assumes global information and should live on the master.
 */
abstract public class FailoverChainManager {
  static final Logger LOG = LoggerFactory.getLogger(FailoverChainManager.class);

  public FailoverChainManager() {
  }

  /**
   * Get a list of the names of failover nodes for a particular agent.
   */
  abstract public List<String> getFailovers(String agent);

  /**
   * Add a new collector to the system.
   */
  abstract public void addCollector(String s);

  /**
   * Remove a collector to the system.
   */
  abstract public void removeCollector(String s);

  /**
   * Give an agent name, a spec with '%s' escape sequence in it, and an ordered
   * list of failovers, generate a the spec for the sink that implements it.
   * This is a spec because we want to essentially translate a user-provided
   * spec into a concrete spec that will be sent to the agent..
   */
  public static String genAvailableSinkSpec(String spec,
      List<String> orderedCollectors) {

    StringBuilder sb = new StringBuilder();
    for (String c : orderedCollectors) {
      sb.append(','); // just assume comma on first -- see format string below
      sb.append('"');
      sb.append(StringEscapeUtils.escapeJava(c));
      sb.append('"');
    }
    String escapedSpec = StringEscapeUtils.escapeJava(spec);
    return "failChain(\"" + escapedSpec + "\"" + sb.toString() + ")";

  }

  /**
   * This uses the instance's state to generate a spec
   */
  public String getFailChainSinkSpec(String agent, String spec) {
    List<String> orderedCollectors = getFailovers(agent);

    // no collectors?
    if (orderedCollectors.size() == 0) {
      String defaultCollector = FlumeConfiguration.get().getCollectorHost();
      LOG.warn("No collectors currently, using default collector host: "
          + defaultCollector);

      // fall back to default specified in flume-site.xml

      // getFailovers returns unmodifiable so we need to create a new list
      orderedCollectors = new ArrayList<String>();
      orderedCollectors.add(defaultCollector);
    }

    return genAvailableSinkSpec(spec, orderedCollectors);
  }
}
