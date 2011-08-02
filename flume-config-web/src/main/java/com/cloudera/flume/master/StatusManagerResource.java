package com.cloudera.flume.master;
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


import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.flume.master.StatusManager;
import com.cloudera.flume.master.StatusManager.NodeStatus;

@Path("/status")
public class StatusManagerResource {
  final public static Logger LOG = LoggerFactory
      .getLogger(StatusManagerResource.class);
  private final StatusManager stats;

  public StatusManagerResource(StatusManager statMan) {
    this.stats = statMan;
  }

  /**
   * Must be public for jersey to instantiate and process.
   */
  public StatusManagerResource() {
    this.stats = FlumeMaster.getInstance().getStatMan();
  }

  public static JSONObject translateNodeStatus(NodeStatus ns)
      throws JSONException {
    JSONObject o = new JSONObject();
    o.put("host", ns.host);
    o.put("lastseen", ns.lastseen);
    o.put("version", ns.version);
    o.put("physicalNode", ns.physicalNode);
    o.put("state", ns.state);
    return o;
  }

  /**
   * Get all node statuses
   */
  @GET
  @Produces("application/json")
  public JSONObject statuses() {
    JSONObject o = new JSONObject();
    try {
      for (Entry<String, NodeStatus> e : stats.getNodeStatuses().entrySet()) {
        o.put(e.getKey(), translateNodeStatus(e.getValue()));
      }
    } catch (JSONException e) {
      LOG.warn("Problem encoding JSON", e);
    }
    return o;
  }

  /**
   * Sub path for only getting data specific for a particular node.
   */
  @GET
  @Path("{node}")
  @Produces("application/json")
  public JSONObject getConfig(@PathParam("node") String node) {
    try {
      return translateNodeStatus(stats.getStatus(node));
    } catch (JSONException e) {
      LOG.warn("Problem encoding JSON", e);
      return new JSONObject();
    }
  }
}
