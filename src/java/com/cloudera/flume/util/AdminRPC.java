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
import java.util.List;
import java.util.Map;

import com.cloudera.flume.conf.FlumeConfigData;
import com.cloudera.flume.master.Command;
import com.cloudera.flume.master.StatusManager;

/**
 * This interface represents the client side of master control RPC exchange.
 * We implement this interface for each RPC package that Flume uses. We 
 * interpret an IOException as a loss of connection to the master.
 */
public interface AdminRPC {
  public boolean isFailure(long cmdid) throws IOException;

  public boolean isSuccess(long cmdid) throws IOException;

  public long submit(Command command) throws IOException;

  public Map<String, StatusManager.NodeStatus> getNodeStatuses()
    throws IOException;

  public Map<String, FlumeConfigData> getConfigs() throws IOException;

  /**
   * Fetch the current physical / logical node mappings. The keys are physical
   * node names and the values a list of logical nodes currently mapped thereto.
   * 
   * @param physicalNode
   *          The name of the physical node or null to get mappings for all
   *          nodes.
   * @return
   * @throws IOException
   */
  public Map<String, List<String>> getMappings(String physicalNode) throws IOException;

  public boolean hasCmdId(long cmdid) throws IOException;
}
