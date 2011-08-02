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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.thrift.CommandStatusThrift;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer;
import com.cloudera.flume.conf.thrift.FlumeMasterCommandThrift;
import com.cloudera.flume.conf.thrift.FlumeNodeStatusThrift;
import com.cloudera.flume.conf.thrift.ThriftFlumeConfigData;
import com.cloudera.flume.conf.thrift.FlumeMasterAdminServer.Iface;
import com.cloudera.flume.util.ThriftServer;
import com.google.common.base.Preconditions;

/**
 * Thrift implementation of the MasterAdmin server. This is a stub server
 * responsible only for listening for requests and function signatures which
 * contain Thrift-specific types. It delegates all function calls to a
 * MasterAdminServer.
 */
public class MasterAdminServerThrift extends ThriftServer implements Iface,
    RPCServer {
  // STATIC TYPE CONVERSION METHODS
  /**
   * Convert from a { @link StatusManager.NodeStatus } object to Thrift's { @link
   * FlumeNodeStatusThrift }.
   */
  public static FlumeNodeStatusThrift statusToThrift(
      StatusManager.NodeStatus status) {
    long time = System.currentTimeMillis();
    FlumeNodeStatusThrift out = new FlumeNodeStatusThrift();
    out.state = MasterClientServerThrift.stateToThrift(status.state);
    out.version = status.version;
    out.lastseen = status.lastseen;
    out.lastSeenDeltaMillis = time - status.lastseen;
    out.host = status.host;
    out.physicalNode = status.physicalNode;
    return out;
  }

  /**
   * Convert from a { @link FlumeNodeStatusThrift } object to the native { @link
   * StatusManager.NodeStatus }.
   */
  public static StatusManager.NodeStatus statusFromThrift(
      FlumeNodeStatusThrift status) {
    return new StatusManager.NodeStatus(MasterClientServerThrift
        .stateFromThrift(status.state), status.version, status.lastseen,
        status.host, status.physicalNode);
  }

  /**
   * Convert from a { @link Command } object to a { @link
   * FlumeMasterCommandThrift } object.
   */
  public static FlumeMasterCommandThrift commandToThrift(Command cmd) {
    FlumeMasterCommandThrift out = new FlumeMasterCommandThrift();
    out.command = cmd.command;
    out.arguments = new LinkedList<String>();
    for (String s : cmd.args) {
      out.arguments.add(s);
    }
    return out;
  }

  /**
   * Convert from a { @link commandFromThrift } object to a { @link Command }
   * object.
   */
  public static Command commandFromThrift(FlumeMasterCommandThrift cmd) {
    return new Command(cmd.command, cmd.arguments.toArray(new String[0]));
  }

  static final Logger LOG = LoggerFactory.getLogger(MasterAdminServerThrift.class);
  final int port;
  final protected MasterAdminServer delegate;

  public MasterAdminServerThrift(MasterAdminServer delegate) {
    Preconditions.checkArgument(delegate != null,
        "MasterAdminServer is null in MasterAdminServerThrift!");
    this.delegate = delegate;
    this.port = FlumeConfiguration.get().getConfigAdminPort();
  }

  // STUB FUNCTION SIGNATURES
  @Override
  public boolean isFailure(long cmdid) throws TException {
    return delegate.isFailure(cmdid);
  }

  @Override
  public boolean isSuccess(long cmdid) throws TException {
    return delegate.isSuccess(cmdid);
  }

  @Override
  public long submit(FlumeMasterCommandThrift command) throws TException {
    return delegate.submit(commandFromThrift(command));
  }

  protected static FlumeNodeStatusThrift toThrift(
      StatusManager.NodeStatus status) {
    long time = System.currentTimeMillis();
    return new FlumeNodeStatusThrift(MasterClientServerThrift
        .stateToThrift(status.state), status.version, status.lastseen, time
        - status.lastseen, status.host, status.physicalNode);
  }

  @Override
  public Map<String, FlumeNodeStatusThrift> getNodeStatuses() throws TException {
    Map<String, StatusManager.NodeStatus> statuses = delegate.getNodeStatuses();
    Map<String, FlumeNodeStatusThrift> ret = new HashMap<String, FlumeNodeStatusThrift>();
    for (Entry<String, StatusManager.NodeStatus> e : statuses.entrySet()) {
      ret.put(e.getKey(), toThrift(e.getValue()));
    }
    return ret;
  }

  @Override
  public Map<String, List<String>> getMappings(String physicalNode)
      throws TException {
    return delegate.getMappings(physicalNode);
  }

  @Override
  public Map<String, ThriftFlumeConfigData> getConfigs() throws TException {
    Map<String, ThriftFlumeConfigData> out = new HashMap<String, ThriftFlumeConfigData>();
    Map<String, FlumeConfigData> orig = delegate.getConfigs();
    for (String key : orig.keySet()) {
      out.put(key, MasterClientServerThrift.configToThrift(orig.get(key)));
    }
    return out;
  }

  @Override
  public boolean hasCmdId(long cmdid) throws TException {
    return delegate.hasCmdId(cmdid);
  }

  // CONTROL FUNCTIONS
  public void serve() throws IOException {
    LOG.info(String.format(
        "Starting blocking thread pool server for admin server on port %d...",
        port));
    try {
      this.start(new FlumeMasterAdminServer.Processor(this), port,
          "MasterAdminServer");
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }

  @Override
  public CommandStatusThrift getCmdStatus(long cmdid) throws TException {
    CommandStatus cmd = delegate.getCommandStatus(cmdid);
    Command c = cmd.getCommand();
    FlumeMasterCommandThrift fmct = new FlumeMasterCommandThrift(
        c.getCommand(), Arrays.asList(c.getArgs()));
    CommandStatusThrift cst = new CommandStatusThrift(cmd.getCmdID(), cmd
        .getState().toString(), cmd.getMessage(), fmct);
    return cst;
  }
}
