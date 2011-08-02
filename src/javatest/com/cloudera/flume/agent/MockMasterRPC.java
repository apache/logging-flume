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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.master.MasterAckManager;
import com.cloudera.flume.reporter.ReportEvent;

/**
 * This mocks out enough of the RPC interface for local tests.
 */
public class MockMasterRPC implements MasterRPC {
  static Logger LOG = Logger.getLogger(MockMasterRPC.class);

  final MasterAckManager ackman = new MasterAckManager();

  @Override
  public void acknowledge(String ackid) throws IOException {
    LOG.info("acknowledging " + ackid);
    ackman.acknowledge(ackid);
  }

  @Override
  public boolean checkAck(String ackid) throws IOException {
    boolean ret = ackman.check(ackid);
    LOG.info("checking " + ackid + " ... " + ret);
    return ret;
  }

  public void open() {
  }

  @Override
  public void close() {
  }

  @Override
  public AckListener createAckListener() {
    return null;
  }

  @Override
  public FlumeConfigData getConfig(LogicalNode n) throws IOException {
    return null;
  }

  @Override
  public boolean heartbeat(LogicalNode n) throws IOException {
    return false;
  }

  @Override
  public List<String> getLogicalNodes(String physNode) throws IOException {
    return new ArrayList<String>();
  }

  @Override
  public void putReports(Map<String, ReportEvent> reports) throws IOException {

  }

  @Override
  public HashMap<String, Integer> getChokeMap(String physNode)
      throws IOException {
    return new HashMap<String, Integer>();
  }

}
