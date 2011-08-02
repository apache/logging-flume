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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.specific.SpecificResponder;
import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.avro.AvroFlumeConfigData;
import com.cloudera.flume.conf.avro.FlumeMasterAdminServerAvro;
import com.cloudera.flume.conf.avro.FlumeMasterCommandAvro;
import com.cloudera.flume.conf.avro.FlumeNodeStatusAvro;
import com.google.common.base.Preconditions;

/**
 * Avro implementation of the MasterAdmin server. This is a stub server
 * responsible only for listening for requests and function signatures which
 * contain Avro-specific types. It delegates all function calls to a
 * MasterAdminServer.
 */
public class MasterAdminServerAvro implements FlumeMasterAdminServerAvro,
    RPCServer {
  // STATIC TYPE CONVERSION METHODS
  /**
   * Convert from a { @link StatusManager.NodeStatus } object to Avro's { @link
   * FlumeNodeStatusAvro }.
   */
  public static FlumeNodeStatusAvro statusToAvro(StatusManager.NodeStatus status) {
    long time = System.currentTimeMillis();
    FlumeNodeStatusAvro out = new FlumeNodeStatusAvro();
    out.state = MasterClientServerAvro.stateToAvro(status.state);
    out.version = status.version;
    out.lastseen = status.lastseen;
    out.lastSeenDeltaMillis = time - status.lastseen;
    out.host = status.host;
    out.physicalNode = status.physicalNode;
    return out;
  }

  /**
   * Convert from a { @link FlumeNodeStatusAvro } object to Flume's native { @link
   * StatusManager.NodeStatus }.
   */
  public static StatusManager.NodeStatus statusFromAvro(
      FlumeNodeStatusAvro status) {
    return new StatusManager.NodeStatus(MasterClientServerAvro
        .stateFromAvro(status.state), status.version, status.lastseen,
        status.host.toString(), status.physicalNode.toString());
  }

  /**
   * Convert from a { @link FlumeMasterCommandAvro } object to a { @link Command
   * } object.
   */
  public static Command commandFromAvro(FlumeMasterCommandAvro cmd) {
    String[] args = new String[(int) cmd.arguments.size()];
    int index = 0;
    for (CharSequence arg : cmd.arguments) {
      args[index] = arg.toString();
      index++;
    }
    return new Command(cmd.command.toString(), args);
  }

  /**
   * Convert from a { @link Command } object to a { @link FlumeMasterCommandAvro
   * } object.
   */
  public static FlumeMasterCommandAvro commandToAvro(Command cmd) {
    FlumeMasterCommandAvro out = new FlumeMasterCommandAvro();
    out.command = cmd.command;
    out.arguments = new ArrayList<CharSequence>();
    for (String s : cmd.args) {
      out.arguments.add(s);
    }
    return out;
  }

  Logger LOG = Logger.getLogger(MasterAdminServerAvro.class);
  final int port;
  final protected MasterAdminServer delegate;
  protected HttpServer server;

  public MasterAdminServerAvro(MasterAdminServer delegate) {
    Preconditions.checkArgument(delegate != null,
        "MasterAdminServer is null in MasterAdminServerAvro!");
    this.delegate = delegate;
    this.port = FlumeConfiguration.get().getConfigAdminPort();
  }

  // STUB FUNCTION SIGNATURES
  @Override
  public Map<CharSequence, AvroFlumeConfigData> getConfigs()
      throws AvroRemoteException {
    Map<CharSequence, AvroFlumeConfigData> out = new HashMap<CharSequence, AvroFlumeConfigData>();
    Map<String, FlumeConfigData> results = this.delegate.getConfigs();
    for (String key : results.keySet()) {
      AvroFlumeConfigData cfg = MasterClientServerAvro.configToAvro(results
          .get(key));
      out.put(key, cfg);
    }
    return out;
  }

  @Override
  public Map<CharSequence, FlumeNodeStatusAvro> getNodeStatuses()
      throws AvroRemoteException {
    Map<String, StatusManager.NodeStatus> statuses = delegate.getNodeStatuses();
    Map<CharSequence, FlumeNodeStatusAvro> ret = new HashMap<CharSequence, FlumeNodeStatusAvro>();
    for (Entry<String, StatusManager.NodeStatus> e : statuses.entrySet()) {
      ret.put(e.getKey(), statusToAvro(e.getValue()));
    }
    return ret;
  }

  @Override
  public Map<CharSequence, List<CharSequence>> getMappings(
      CharSequence physicalNode) throws AvroRemoteException {
    Map<String, List<String>> nativeMappings;
    Map<CharSequence, List<CharSequence>> mappings;

    nativeMappings = delegate.getMappings(physicalNode.toString());
    mappings = new HashMap<CharSequence, List<CharSequence>>();

    for (Entry<String, List<String>> entry : nativeMappings.entrySet()) {

      ArrayList<CharSequence> values = new ArrayList<CharSequence>(entry
          .getValue().size());

      for (String value : entry.getValue()) {
        values.add(value);
      }

      mappings.put(entry.getKey(), values);
    }

    return mappings;
  }

  @Override
  public boolean hasCmdId(long cmdid) throws AvroRemoteException {
    return delegate.hasCmdId(cmdid);
  }

  @Override
  public boolean isFailure(long cmdid) throws AvroRemoteException {
    return delegate.isFailure(cmdid);
  }

  @Override
  public boolean isSuccess(long cmdid) throws AvroRemoteException {
    return delegate.isSuccess(cmdid);
  }

  @Override
  public long submit(FlumeMasterCommandAvro command) throws AvroRemoteException {
    return delegate.submit(commandFromAvro(command));
  }

  // CONTROL FUNCTIONS
  @Override
  public void serve() throws IOException {
    LOG.info(String.format(
        "Starting blocking thread pool server for admin server on port %d...",
        port));
    SpecificResponder res = new SpecificResponder(
        FlumeMasterAdminServerAvro.class, this);
    this.server = new HttpServer(res, port);
    this.server.start();
  }

  @Override
  public void stop() throws IOException {
    LOG.info(String.format("Stopping admin server on port %d...", port));
    this.server.close();
  }
}
