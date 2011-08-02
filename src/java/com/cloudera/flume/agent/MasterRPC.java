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

package com.cloudera.flume.agent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.reporter.ReportEvent;

/**
 * This is a wrapper that manages connections between the flume node and the
 * master. It attempts to encapsulates any thrift related RPC stuff to enable
 * easier substitution with another node-master rpc mechanism (such as Avro or
 * ZK). Also is useful for Mocking out for use in unit tests.
 */
public interface MasterRPC {

  public void open() throws IOException;

  public void close() throws IOException;

  /**
   * This is a hook to allow acks to be sent to the master. This generally will
   * happen from a collector node.
   */
  public AckListener createAckListener();

  // TODO (jon) FlumeConfigData is a thrift class and shouldn't be here
  public FlumeConfigData getConfig(LogicalNode n) throws IOException;

  /**
   * This checks for an ack with a given ackid at the master
   */
  public boolean checkAck(String ackid) throws IOException;

  public boolean heartbeat(LogicalNode n) throws IOException;

  public void acknowledge(String group) throws IOException;

  public List<String> getLogicalNodes(String physNode) throws IOException;
  
  public void putReports(Map<String, ReportEvent> reports) throws IOException;
}
