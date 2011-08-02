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

import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TSaneThreadPoolServer;
import org.apache.thrift.transport.TSaneServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.google.common.base.Preconditions;

/**
 * Simple base class to encapsulate the code required to get a Thrift server up
 * and running.
 */
public class ThriftServer {
  Logger LOG = Logger.getLogger(ThriftServer.class);
  protected TSaneServerSocket serverTransport = null;;
  protected TSaneThreadPoolServer server = null;  
  String description;
  protected int port;

  synchronized public void stop() {
    Preconditions.checkArgument(server != null);
    serverTransport.close();
    server.stop();
  }

  /**
   * Blocks until Thrift server has started and can accept connections
   */
  synchronized protected void start(TProcessor processor, final int port,
      final String description) throws TTransportException {
    this.description = description;
    this.serverTransport = new TSaneServerSocket(port);
    Factory protFactory = new TBinaryProtocol.Factory(true, true);
    server = new TSaneThreadPoolServer(processor, serverTransport, protFactory);
    server.start();
  }
}
