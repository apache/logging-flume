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

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.xml.bind.annotation.XmlRootElement;

import com.cloudera.flume.master.StatusManager.NodeStatus;

@Path("/status")
public class StatusManagerResource {
  private final StatusManager stats;

  /**
   * Must be public for jersey to instantiate and process.
   */
  public StatusManagerResource() {
    this.stats = FlumeMaster.getInstance().getStatMan();
  }

  /**
   * Get the old style generated html report
   */
  @GET
  @Path("report")
  @Produces("text/html")
  public String getOldReport() {
    return stats.getReport().toHtml();
  }

  /**
   * Wrapper class to provide XmlRootElement.
   */
  @XmlRootElement
  public static class Statuses {
    public Map<String, NodeStatus> status;

    public Statuses() {
    }

    public Statuses(Map<String, NodeStatus> cfgs) {
      this.status = cfgs;
    }
  }

  /**
   * Get all node statuses
   */
  @GET
  @Produces("application/json")
  public Statuses statuses() {
    return new Statuses(stats.statuses);
  }

  /**
   * Sub path for only getting data specific for a particular node.
   */
  @GET
  @Path("{node}")
  @Produces("application/json")
  public NodeStatus getConfig(@PathParam("node") String node) {
    return stats.getStatus(node);
  }
}
