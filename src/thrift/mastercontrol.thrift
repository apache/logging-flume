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

# Master configuration and administration interface

namespace java com.cloudera.flume.conf.thrift

include "flumeconfig.thrift"

struct FlumeMasterCommand {
  1: string command,
  2: list<string> arguments
}

// Equivalent to StatusManager.NodeStatus
struct FlumeNodeStatus {
  1: flumeconfig.FlumeNodeState state,
  2: i64 version,
  3: i64 lastseen,
  4: string host,
  5: string physicalNode,
}

service FlumeMasterAdminServer {
  i64 submit(1: FlumeMasterCommand command),
  bool isSuccess(1: i64 cmdid),  
  bool isFailure(1: i64 cmdid),  
  map<string, FlumeNodeStatus> getNodeStatuses(),
  map<string, flumeconfig.FlumeConfigData> getConfigs(),
  bool hasCmdId(1: i64 cmdid)
  // TODO (jon) augment with getstate
}

