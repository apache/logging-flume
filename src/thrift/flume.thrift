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
# flume.thrift
#
# This thrift interface and service defines a network transport mechanism to move events 
# from one process/machine to another.  At the moment this mirrors the fields of an event 
# as defined in c.c.f.core.Event.java's code . 
#
# This may change more fields are likely to be added, and the actual format is subject to change.

# The server has two rpc methods 
# -- append: which sends an event to the server,
# -- close: shuts down this client's connection
#
# Currently append is oneway, requiring the thrift server to do flow control.  

namespace java com.cloudera.flume.handlers.thrift

typedef i64 Timestamp

enum Priority { 
  FATAL = 0,
  ERROR = 1,
  WARN = 2,
  INFO = 3,
  DEBUG = 4,
  TRACE = 5
}

enum EventStatus {
  ACK = 0,
  COMMITED = 1,
  ERR = 2
}

struct ThriftFlumeEvent {
  1: Timestamp timestamp,
  2: Priority priority,
  3: binary body,
  4: i64 nanos,
  5: string host,
  6: map<string,binary> fields
}

service ThriftFlumeEventServer {
  oneway void append( 1:ThriftFlumeEvent evt ),

  void close(), 
}

