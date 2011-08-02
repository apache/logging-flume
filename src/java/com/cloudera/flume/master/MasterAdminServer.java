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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer;
import com.cloudera.flume.conf.thrift.FlumeMasterCommand;
import com.cloudera.flume.conf.thrift.FlumeNodeStatus;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer.Iface;
import com.cloudera.flume.util.ThriftServer;
import com.google.common.base.Preconditions;

/**
 * Implementation of the admin RPC interface over Thrift
 */
public class MasterAdminServer extends ThriftServer implements Iface {
  Logger LOG = Logger.getLogger(MasterAdminServer.class);
  final int port;
  final protected FlumeMaster master;

  public MasterAdminServer(FlumeMaster master) {
    this(master, FlumeConfiguration.get().getConfigAdminPort());
  }

  public MasterAdminServer(FlumeMaster master, int port) {
    Preconditions.checkArgument(master != null,
        "FlumeConfigMaster is null in MasterAdminServer!");
    this.master = master;
    this.port = port;
  }

  @Override
  public boolean isFailure(long cmdid) throws TException {
    return master.getCmdMan().isFailure(cmdid);
  }

  @Override
  public boolean isSuccess(long cmdid) throws TException {
    return master.getCmdMan().isSuccess(cmdid);
  }

  @Override
  public long submit(FlumeMasterCommand command) throws TException {
    return master.submit(new Command(command.command, command.arguments
        .toArray(new String[0])));
  }

  public void serve() throws TTransportException {
    LOG.info(String.format(
        "Starting blocking thread pool server for admin server on port %d...",
        port));
    this.start(new FlumeMasterAdminServer.Processor(this), port,
        "MasterAdminServer");
  }

  protected FlumeNodeStatus toThrift(StatusManager.NodeStatus status) {
    return new FlumeNodeStatus(MasterClientServer.stateToThrift(status.state),
        status.version, status.lastseen, status.host, status.physicalNode);
  }

  @Override
  public Map<String, FlumeNodeStatus> getNodeStatuses() throws TException {
    Map<String, StatusManager.NodeStatus> statuses = master.getStatMan()
        .getNodeStatuses();
    Map<String, FlumeNodeStatus> ret = new HashMap<String, FlumeNodeStatus>();
    for (Entry<String, StatusManager.NodeStatus> e : statuses.entrySet()) {
      ret.put(e.getKey(), toThrift(e.getValue()));
    }
    return ret;
  }

  @Override
  public Map<String, FlumeConfigData> getConfigs() throws TException {
    return master.getSpecMan().getAllConfigs();
  }

  /*
   * Returns true if there is a command in the command manager with the
   * specified id.
   */
  @Override
  public boolean hasCmdId(long cmdid) throws TException {
    return master.getCmdMan().getStatus(cmdid) != null;
  }
}
