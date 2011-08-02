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

import com.cloudera.flume.conf.FlumeConfigData;

@Path("/configs")
public class ConfigManagerResource {
  private final ConfigurationManager configs;

  /**
   * Must be public for jersey to instantiate and process.
   */
  public ConfigManagerResource() {
    this.configs = FlumeMaster.getInstance().getSpecMan();
  }

  /**
   * Wrapper class to provide XmlRootElement.
   */
  @XmlRootElement
  public static class Configs {
    public Map<String, FlumeConfigData> configs;

    public Configs() {

    }

    public Configs(Map<String, FlumeConfigData> cfgs) {
      this.configs = cfgs;
    }
  }

  // The Java method will process HTTP GET requests
  @GET
  @Produces("application/json")
  public Configs configs() {
    return new Configs(configs.getAllConfigs());
  }

  /**
   * Sub path for only getting data specific for a partricular node.
   * 
   * @param node
   * @return
   */
  @GET
  @Path("{node}")
  @Produces("application/json")
  public FlumeConfigData getConfig(@PathParam("node") String node) {
    return configs.getConfig(node);
  }
}
