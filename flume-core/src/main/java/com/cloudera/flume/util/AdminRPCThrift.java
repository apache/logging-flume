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
package com.cloudera.flume.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.thrift.CommandStatusThrift;
import com.cloudera.flume.conf.thrift.FlumeMasterCommandThrift;
import com.cloudera.flume.conf.thrift.FlumeNodeStatusThrift;
import com.cloudera.flume.conf.thrift.ThriftFlumeConfigData;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer.Client;
import com.cloudera.flume.master.Command;
import com.cloudera.flume.master.CommandStatus;
import com.cloudera.flume.master.MasterAdminServerThrift;
import com.cloudera.flume.master.MasterClientServerThrift;
import com.cloudera.flume.master.StatusManager.NodeStatus;

/**
 * Thrift implementation of the Flume admin control RPC. This class manages the
 * connection to a master and provides type conversion.
 */
public class AdminRPCThrift implements AdminRPC {
  static final Logger LOG = LoggerFactory.getLogger(AdminRPCThrift.class);

  Client masterClient;

  public AdminRPCThrift(String masterHost, int masterPort) throws IOException {
    TTransport masterTransport = new TSocket(masterHost, masterPort);
    TProtocol protocol = new TBinaryProtocol(masterTransport);
    try {
      masterTransport.open();
    } catch (TTransportException e) {
      throw new IOException(e);
    }
    masterClient = new Client(protocol);
    LOG.info("Connected to master at " + masterHost + ":" + masterPort);
  }

  @Override
  public Map<String, FlumeConfigData> getConfigs() throws IOException {
    Map<String, ThriftFlumeConfigData> results;
    try {
      results = masterClient.getConfigs();
    } catch (TException e) {
      throw new IOException(e);
    }
    Map<String, FlumeConfigData> out = new HashMap<String, FlumeConfigData>();
    for (String s : results.keySet()) {
      out.put(s, MasterClientServerThrift.configFromThrift(results.get(s)));
    }
    return out;
  }

  @Override
  public Map<String, NodeStatus> getNodeStatuses() throws IOException {
    Map<String, FlumeNodeStatusThrift> results;
    try {
      results = masterClient.getNodeStatuses();
    } catch (TException e) {
      throw new IOException(e);
    }
    Map<String, NodeStatus> out = new HashMap<String, NodeStatus>();
    for (String s : results.keySet()) {
      out.put(s, MasterAdminServerThrift.statusFromThrift(results.get(s)));
    }
    return out;
  }

  @Override
  public Map<String, List<String>> getMappings(String physicalNode)
      throws IOException {
    try {
      return masterClient.getMappings(physicalNode);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean hasCmdId(long cmdid) throws IOException {
    try {
      return masterClient.hasCmdId(cmdid);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public CommandStatus getCommandStatus(long cmdid) throws IOException {
    try {
      CommandStatusThrift cst = masterClient.getCmdStatus(cmdid);
      if (cst == null) {
        throw new IOException("Illegal command id: " + cmdid);
      }
      FlumeMasterCommandThrift cmdt = cst.getCmd();
      Command cmd = new Command(cmdt.getCommand(), cmdt.arguments
          .toArray(new String[0]));
      CommandStatus cs = new CommandStatus(cst.getCmdId(), cmd,
          CommandStatus.State.valueOf(cst.state), cst.getMessage());
      return cs;
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isFailure(long cmdid) throws IOException {
    try {
      return masterClient.isFailure(cmdid);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isSuccess(long cmdid) throws IOException {
    try {
      return masterClient.isSuccess(cmdid);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long submit(Command command) throws IOException {
    try {
      return masterClient.submit(MasterAdminServerThrift
          .commandToThrift(command));
    } catch (TException e) {
      throw new IOException(e);
    }
  }

}
