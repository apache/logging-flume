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


import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.master.ConfigurationManager;
import com.cloudera.flume.master.FlumeMaster;

@Path("/configs")
public class ConfigManagerResource {
  final public static Logger LOG = LoggerFactory
      .getLogger(ConfigManagerResource.class);
  private final ConfigurationManager configs;

  public ConfigManagerResource(ConfigurationManager cfgs) {
    this.configs = cfgs;
  }

  /**
   * Must be public for jersey to instantiate and process.
   */
  public ConfigManagerResource() {
    this.configs = FlumeMaster.getInstance().getSpecMan();
  }

  public static JSONObject toJSONObject(FlumeConfigData fcd)
      throws JSONException {
    JSONObject o = new JSONObject();
    o.put("flowID", fcd.flowID);
    o.put("sinkVersion", fcd.sinkVersion);
    o.put("sourceVersion", fcd.sourceVersion);
    o.put("timestamp", fcd.timestamp);
    o.put("sinkConfig", fcd.sinkConfig);
    o.put("sourceConfig", fcd.sourceConfig);
    return o;
  }

  /**
   * return a JSONObject that presents the configs and translated configs from
   * the master's configuration manager.
   */
  @GET
  @Produces("application/json")
  public JSONObject configman() {
    JSONObject o = new JSONObject();
    try {
      o.put("configs", toJSONObject(configs.getAllConfigs()));
      o.put("translatedConfigs", toJSONObject(configs.getTranslatedConfigs()));
    } catch (JSONException e) {
      LOG.warn("Problem encoding JSON", e);
      return new JSONObject();
    }
    return o;
  }

  public JSONObject toJSONObject(Map<String, FlumeConfigData> cfgs) {
    JSONObject o = new JSONObject();
    try {
      for (Entry<String, FlumeConfigData> e : cfgs.entrySet()) {
        o.put(e.getKey(), toJSONObject(e.getValue()));
      }
    } catch (JSONException e1) {
      LOG.warn("Problem encoding JSON", e1);
      return new JSONObject();
    }
    return o;
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
  public FlumeConfigData getConfig(@PathParam("node") String node) {
    return configs.getConfig(node);
  }
}
