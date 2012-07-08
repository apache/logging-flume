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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.log.Log;

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.agent.LogicalNode;

@Path("/reports")
public class NodeReportResource {

  FlumeNode node;

  @Context
  UriInfo uriInfo;

  /**
   * Must be public for jersey to instantiate and process.
   */
  public NodeReportResource() {
    this.node = FlumeNode.getInstance();
  }

  @GET
  @Produces("application/json")
  public JSONObject getReport() {
    try {
      JSONObject obj = new JSONObject();

      // add logical node links:
      Map<String, String> lnMap = new HashMap<String, String>();

      for (LogicalNode ln : node.getLogicalNodeManager().getNodes()) {
        String name = ln.getName();
        UriBuilder ub = uriInfo.getAbsolutePathBuilder();
        URI userUri = ub.path(ln.getName()).build();
        String lnk = userUri.toASCIIString();
        lnMap.put(name, lnk);
      }
      obj.put("logicalnodes", lnMap);

      obj
          .put("jvmInfo", ReportUtil
              .toJSONObject(node.getVMInfo().getMetrics()));
      obj.put("sysInfo", ReportUtil.toJSONObject(node.getSystemInfo()
          .getMetrics()));

      return obj;
    } catch (JSONException e1) {
      Log.warn("Problem converting report to JSON", e1);
    }

    // error case, return empty object
    return new JSONObject();
  }

  /**
   * Sub path for only getting data specific for a particular node.
   * 
   * @param node
   * @return
   */
  @GET
  @Path("{node}")
  @Produces("application/json")
  public JSONObject getConfig(@PathParam("node") String lnode) {
    LogicalNode ln = node.getLogicalNodeManager().get(lnode);
    if (ln == null) {
      // no node found, return empty.
      return new JSONObject();
    }

    try {
      ReportEvent rpt = ReportUtil.getFlattenedReport(ln);
      return ReportUtil.toJSONObject(rpt);
    } catch (JSONException e) {
      Log.warn("Problem converting report to JSON", e);
      return new JSONObject();
    }
  }

}
