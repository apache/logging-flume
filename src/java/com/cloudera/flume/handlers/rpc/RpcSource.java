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
package com.cloudera.flume.handlers.rpc;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.avro.AvroEventSource;
import com.cloudera.flume.handlers.thrift.ThriftEventSource;
import com.google.common.base.Preconditions;

/**
 * This is a source that sends events through a RPC, it either uses Avro or
 * Thrift depending upon the EVENT_RPC_TYPE set in the configuration file.
 */
public class RpcSource extends EventSink.Base {
  final public static Logger LOG = Logger.getLogger(RpcSource.class);

  /**
   * This class will build an AvroEventSource or ThriftEventSource depending on
   * the value of EVENT_RPC_TYPE set in the configuration file.
   */
  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length == 1, "usage: rpcSource(port)");

        int port = Integer.parseInt(argv[0]);
        /*
         * I am hoping here that master calls this builder and at that time the
         * FlumeConfiguration is all set.
         */
        if (FlumeConfiguration.get().getEventRPC().equals(
            FlumeConfiguration.RPC_TYPE_THRIFT)) {
          return new ThriftEventSource(port);
        }
        if (FlumeConfiguration.get().getEventRPC().equals(
            FlumeConfiguration.RPC_TYPE_AVRO)) {
          return new AvroEventSource(port);
        }
        LOG.warn("event.rpc.type not defined.  It should be either "
            + "\"THRIFT\" or \"AVRO\". Defaulting to Thrift");
        return new ThriftEventSource(port);
      }
    };
  }
}
