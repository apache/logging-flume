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

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;

/**
 * Implementation of the admin RPC interface. Since the wire protocol is
 * different for separate RPC implementations, they each run their own stub
 * servers, then delegate all requests to this common class.
 */
public class MasterAdminServer {
  Logger LOG = Logger.getLogger(MasterAdminServer.class);
  final protected FlumeMaster master;
  private RPCServer stubServer;

  public MasterAdminServer(FlumeMaster master, FlumeConfiguration config)
      throws IOException {
    Preconditions.checkArgument(master != null,
        "FlumeConfigMaster is null in MasterAdminServer!");
    this.master = master;
    String rpcType = config.getMasterHeartbeatRPC();
    this.stubServer = null;
    if (FlumeConfiguration.RPC_TYPE_AVRO.equals(rpcType)) {
      stubServer = new MasterAdminServerAvro(this);
    } else if (FlumeConfiguration.RPC_TYPE_THRIFT.equals(rpcType)) {
      stubServer = new MasterAdminServerThrift(this);
    } else {
      throw new IOException("No valid RPC framework specified in config");
    }
  }

  public boolean isFailure(long cmdid) {
    return master.getCmdMan().isFailure(cmdid);
  }

  public boolean isSuccess(long cmdid) {
    return master.getCmdMan().isSuccess(cmdid);
  }

  public long submit(Command command) {
    return master.submit(command);
  }

  public void serve() throws IOException {
    this.stubServer.serve();
  }

  public void stop() throws IOException {
    this.stubServer.stop();
  }

  public Map<String, StatusManager.NodeStatus> getNodeStatuses() {
    Map<String, StatusManager.NodeStatus> statuses = master.getStatMan()
        .getNodeStatuses();
    Map<String, StatusManager.NodeStatus> ret = new HashMap<String, StatusManager.NodeStatus>();
    for (Entry<String, StatusManager.NodeStatus> e : statuses.entrySet()) {
      ret.put(e.getKey(), e.getValue());
    }
    return ret;
  }

  public Map<String, FlumeConfigData> getConfigs() {
    return master.getSpecMan().getAllConfigs();
  }

  /**
   * Fetch the list of logical to physical mappings and return it as {@link Map}
   * &lt;{@link String}, {@link List}&lt;String&gt;&gt; where the key is the
   * physical node name and the value is a list of logical nodes mapped to it.
   * 
   * @param physicalNode
   *          The physical node for which to fetch mappings or null to fetch
   *          all.
   * @return the node map
   */
  public Map<String, List<String>> getMappings(String physicalNode) {
    Map<String, List<String>> resultMap = new HashMap<String, List<String>>();

    if (physicalNode != null) {
      List<String> logicalNodes = master.getSpecMan().getLogicalNode(
          physicalNode);

      if (logicalNodes != null && logicalNodes.size() > 0) {
        resultMap.put(physicalNode, master.getSpecMan().getLogicalNode(
            physicalNode));
      }
    } else {
      Multimap<String, String> m = master.getSpecMan().getLogicalNodeMap();

      // Transform the multimap into a map of string => list<string>.
      for (Entry<String, String> entry : m.entries()) {
        if (!resultMap.containsKey(entry.getKey())) {
          resultMap.put(entry.getKey(), new LinkedList<String>());
        }

        resultMap.get(entry.getKey()).add(entry.getValue());
      }
    }

    return resultMap;
  }

  /*
   * Returns true if there is a command in the command manager with the
   * specified id.
   */
  public boolean hasCmdId(long cmdid) {
    return master.getCmdMan().getStatus(cmdid) != null;
  }

  public CommandStatus getCommandStatus(long cmdId) {
    return master.getCmdMan().getStatus(cmdId);
  }
}
