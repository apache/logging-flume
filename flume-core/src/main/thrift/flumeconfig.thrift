/*
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
# flumeconfig.thrift 

# This is the a heartbeating and configuration service that allows
# clients to contact a flume configuration server.

# This allows for dynamically configurable flume nodes (setup sources,
# sinks).  Eventually this can be used by flume nodes to send
# monitoring information.

# TODO (jon) this is really rough right now.

namespace java com.cloudera.flume.conf.thrift

include "flumereportserver.thrift"

typedef i64 Timestamp

enum FlumeNodeState {
  HELLO =0,
  IDLE =1,
  CONFIGURING =2,
  ACTIVE=3,
  ERROR =4,
  LOST=5,
  DECOMMISSIONED=6,
  CLOSING=7
}

struct ThriftFlumeConfigData {
  1: Timestamp timestamp,
  2: string sourceConfig,
  3: string sinkConfig,
  4: i64 sourceVersion,
  5: i64 sinkVersion,
  6: string flowID
}

// TODO (jon) right now sourceId is a name selected by client, 
// likely to be some naming consistency issues.

service ThriftFlumeClientServer {
  // This will get removed from the service
  // returns true if the sourceId's configuration has changed												 
  bool heartbeat(1:string logicalNode, 4:string physicalNode, 5:string host, 2:FlumeNodeState s, 3:i64 timestamp),
  
  // This gets the configuration from the specified sourceId/name 
  ThriftFlumeConfigData getConfig(1:string sourceId),

  list<string> getLogicalNodes(1: string physNode),
  
  //this returns a map from ChokeIds to their respective limits for the given physicalnode
  map<string, i32> getChokeMap(1: string physNode),
  
  // This marks a batch as complete
  void acknowledge(1:string ackid), 
  
  // This is checks to see if a batch is complete
  bool checkAck(1:string ackid),

  // For nodes to send reports to the master
  void putReports(1:map<string, flumereportserver.ThriftFlumeReport> reports)
}

