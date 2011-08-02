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

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TSaneThreadPoolServer;
import org.apache.thrift.transport.TSaneServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Simple base class to encapsulate the code required to get a Thrift server up
 * and running.
 * 
 * TODO refactor this class -- seems like some of the start args should move to the constructor
 */
public class ThriftServer {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);

  protected TServerTransport serverTransport = null;;
  protected TSaneThreadPoolServer server = null;  
  String description;
  protected int port;

  protected boolean strictRead = true;
  protected boolean strictWrite = true;

  /**
   * Construct, but do not start, a thrift server. By default the server
   * will create a TBinaryProtocol with strict reads and writes.
   */
  public ThriftServer() {}
  
  /**
   * Construct, but do not start, a thrift server.
   * @param strictRead strict TBinaryProtcol reads
   * @param strictWrite strict TBinaryProtcol writes
   */
  public ThriftServer(boolean strictRead, boolean strictWrite) {
    this.strictRead = strictRead;
    this.strictWrite = strictWrite;
  }

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
    start(processor, description, new TSaneServerSocket(port));
  }

  /**
   * Blocks until Thrift server has started and can accept connections
   */
  synchronized protected void start(TProcessor processor,
      final String description, TServerTransport serverTransport)
    throws TTransportException
  {
    this.description = description;
    this.serverTransport = serverTransport;
    Factory protFactory = new TBinaryProtocol.Factory(strictRead, strictWrite);
    server = new TSaneThreadPoolServer(processor, serverTransport, protFactory);
    server.start();
  }
}
