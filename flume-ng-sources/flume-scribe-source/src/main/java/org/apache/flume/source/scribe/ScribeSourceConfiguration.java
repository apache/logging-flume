/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source.scribe;

import org.apache.flume.Context;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

public class ScribeSourceConfiguration {

  public enum ThriftServerType {
    THSHA_SERVER("ThshaServer"),
    TTHREADED_SELECTOR_SERVER("TThreadedSelectorServer"),
    TTHREADPOOL_SERVER("TThreadPoolServer");

    private String value;

    ThriftServerType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public static ThriftServerType getServerType(String value) throws IllegalArgumentException {
      for(ThriftServerType v : values()) {
        if (v.getValue().equalsIgnoreCase(value)) {
          return v;
        }
      }
      throw new IllegalArgumentException("Cannot find Thrift server type:" + value);
    }
  }

  public static final int DEFAULT_PORT = 1499;
  public static final int DEFAULT_WORKERS = 5;
  public static final ThriftServerType DEFAULT_THRIFT_SERVER_TYPE = ThriftServerType.THSHA_SERVER;
  public static final int DEFAULT_THREADED_SELECTOR_NUM_THREADS = 4;
  public static final long DEFAULT_MAX_READ_BUFFER_BYTES = 16384000;
  public static final int DEFAULT_MAX_THRIFT_FRAME_SIZE_BYTES = 16384000;
  public static final String SCRIBE_CATEGORY = "category";


  public int port;
  public int workerThreadNum;
  public int selectorThreadNum;
  public ThriftServerType thriftServerType;
  public long maxReadBufferBytes;
  public int maxThriftFrameSizeBytes;

  public void configure(Context context) {
    port = context.getInteger("port", DEFAULT_PORT);
    maxReadBufferBytes = context.getLong("maxReadBufferBytes", DEFAULT_MAX_READ_BUFFER_BYTES);
    if (maxReadBufferBytes <= 0) {
      maxReadBufferBytes = DEFAULT_MAX_READ_BUFFER_BYTES;
    }
    maxThriftFrameSizeBytes = context.getInteger("maxThriftFrameSizeBytes",
        DEFAULT_MAX_THRIFT_FRAME_SIZE_BYTES);
    if (maxThriftFrameSizeBytes <= 0) {
      maxThriftFrameSizeBytes = DEFAULT_MAX_THRIFT_FRAME_SIZE_BYTES;
    }
    workerThreadNum = context.getInteger("workerThreads", DEFAULT_WORKERS);
    if (workerThreadNum <= 0) {
      workerThreadNum = DEFAULT_WORKERS;
    }
    String thriftServerTypeStr
        = context.getString("thriftServer", DEFAULT_THRIFT_SERVER_TYPE.getValue());
    thriftServerType = ThriftServerType.getServerType(thriftServerTypeStr);
    selectorThreadNum = context.getInteger("selectorThreads", DEFAULT_THREADED_SELECTOR_NUM_THREADS);
    if (selectorThreadNum <= 0) {
      selectorThreadNum = DEFAULT_THREADED_SELECTOR_NUM_THREADS;
    }
  }

  public THsHaServer.Args createTHsHasServerArg(
      Scribe.Processor processor, int port) throws TTransportException {
    TNonblockingServerTransport transport = new TNonblockingServerSocket(port);
    THsHaServer.Args args = new THsHaServer.Args(transport);
    args.minWorkerThreads(workerThreadNum);
    args.maxWorkerThreads(workerThreadNum);
    args.processor(processor);
    args.transportFactory(new TFramedTransport.Factory(maxThriftFrameSizeBytes));
    args.protocolFactory(new TBinaryProtocol.Factory(false, false));
    args.maxReadBufferBytes = maxReadBufferBytes;
    return args;
  }

  public TThreadedSelectorServer.Args createTThreadedSelectorServerArgs(
      Scribe.Processor processor, int port) throws TTransportException {
    TNonblockingServerTransport transport = new TNonblockingServerSocket(port);
    TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(transport);
    args.selectorThreads(selectorThreadNum);
    args.workerThreads(workerThreadNum);
    args.processor(processor);
    args.transportFactory(new TFramedTransport.Factory(maxThriftFrameSizeBytes));
    args.protocolFactory(new TBinaryProtocol.Factory(false, false));
    args.maxReadBufferBytes = maxReadBufferBytes;
    args.validate();
    return args;
  }

  public TThreadPoolServer.Args createTThreadPoolServerArgs(
      Scribe.Processor processor, int port) throws TTransportException {
    TServerTransport transport = new TServerSocket(port);
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
    if (workerThreadNum < args.minWorkerThreads) {
      args.minWorkerThreads = workerThreadNum;
    }
    args.maxWorkerThreads(workerThreadNum);
    args.processor(processor);
    args.transportFactory(new TFramedTransport.Factory(maxThriftFrameSizeBytes));
    args.protocolFactory(new TBinaryProtocol.Factory(false, false));
    return args;
  }
}
