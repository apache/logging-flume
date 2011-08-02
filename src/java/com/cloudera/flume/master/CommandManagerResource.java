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

import java.util.Map.Entry;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/commands")
public class CommandManagerResource {
  final public static Logger LOG = LoggerFactory
      .getLogger(CommandManagerResource.class);
  private final CommandManager commands;

  public CommandManagerResource(CommandManager cmds) {
    this.commands = cmds;
  }

  /**
   * Must be public for jersey to instantiate and process.
   */
  public CommandManagerResource() {
    this.commands = FlumeMaster.getInstance().getCmdMan();
  }

  JSONObject toJSONObject(CommandStatus c) throws JSONException {
    JSONObject o = new JSONObject();
    o.put("cmd", c.cmd);
    o.put("cmdId", c.cmdId);
    o.put("curState", c.curState);
    o.put("message", c.message);
    return o;
  }

  @GET
  @Produces("application/json")
  public JSONObject getLogs() {
    JSONObject o = new JSONObject();
    try {
      for (Entry<Long, CommandStatus> e : commands.statuses.entrySet()) {
        o.put(e.getKey().toString(), toJSONObject(e.getValue()));
      }
    } catch (JSONException e) {
      LOG.warn("Problem encoding JSON", e);
      return new JSONObject();
    }
    return o;
  }

  @GET
  @Path("{idx: [0-9]*}")
  @Produces("application/json")
  public JSONObject getLog(@PathParam("idx") String sidx) {
    long idx = Long.parseLong(sidx);
    CommandStatus cmd = commands.getStatus(idx);
    if (cmd == null) {
      // return empty;
      return new JSONObject();
    }
    try {
      return toJSONObject(cmd);
    } catch (JSONException e) {
      LOG.warn("Problem encoding JSON", e);
      return new JSONObject();
    }
  }

}
