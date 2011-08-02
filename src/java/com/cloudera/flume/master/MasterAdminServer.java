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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeConfigData;
import com.google.common.base.Preconditions;

/**
 * Implementation of the admin RPC interface. Since the 
 * wire protocol is different for separate RPC implementations, they each 
 * run their own stub servers, then delegate all requests to this common class.
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
    Map<String, StatusManager.NodeStatus> ret = 
      new HashMap<String, StatusManager.NodeStatus>();
    for (Entry<String, StatusManager.NodeStatus> e : statuses.entrySet()) {
      ret.put(e.getKey(), e.getValue());
    }
    return ret;
  }

  public Map<String, FlumeConfigData> getConfigs() {
    return master.getSpecMan().getAllConfigs();
  }

  public boolean hasCmdId(long cmdid) {
    return master.getCmdMan().getStatus(cmdid) != null;
  }
}
