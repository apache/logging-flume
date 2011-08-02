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

struct FlumeMasterCommandThrift {
  1: string command,
  2: list<string> arguments
}

// Equivalent to StatusManager.NodeStatus
struct FlumeNodeStatusThrift {
  1: flumeconfig.FlumeNodeState state,
  2: i64 version,
  3: i64 lastseen,
  6: i64 lastSeenDeltaMillis,
  4: string host,
  5: string physicalNode,
}

struct CommandStatusThrift {
  1: i64 cmdId,
  2: string state,
  3: string message,
  4: FlumeMasterCommandThrift cmd,
}

service FlumeMasterAdminServer {
  i64 submit(1: FlumeMasterCommandThrift command),
  bool isSuccess(1: i64 cmdid),  
  bool isFailure(1: i64 cmdid),  
  map<string, FlumeNodeStatusThrift> getNodeStatuses(),
  map<string, flumeconfig.ThriftFlumeConfigData> getConfigs(),
  bool hasCmdId(1: i64 cmdid),
  CommandStatusThrift getCmdStatus(1: i64 cmdid),
  map<string, list<string>> getMappings(1: string physicalNode)
  // TODO (jon) augment with getstate
}

