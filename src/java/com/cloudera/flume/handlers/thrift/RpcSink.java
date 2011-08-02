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
package com.cloudera.flume.handlers.thrift;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.avro.AvroEventSink;

/**
 * This is a sink that is used to send events through an RPC, it either uses
 * Avro or Thrift depending upon the EVENT_RPC_TYPE set in the configuration
 * file.
 */
public class RpcSink extends EventSink.Base {
  final public static Logger LOG = Logger.getLogger(RpcSink.class);

  /**
   * This class will build an AvroEventSink or ThriftEventSink depending on the
   * value of EVENT_RPC_TYPE set in the configuration file.
   */
  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        if (args.length > 2) {
          throw new IllegalArgumentException(
              "usage: rpcSink([hostname, [portno]]) ");
        }
        String host = FlumeConfiguration.get().getCollectorHost();
        int port = FlumeConfiguration.get().getCollectorPort();
        if (args.length >= 1) {
          host = args[0];
        }

        if (args.length >= 2) {
          port = Integer.parseInt(args[1]);
        }
        /*
         * I am hoping here that master calls this builder and at that time the
         * FlumeConfiguration is all set.
         */
        if (FlumeConfiguration.get().getEventRPC().equals(
            FlumeConfiguration.RPC_TYPE_THRIFT)) {
          return new ThriftEventSink(host, port);
        }
        if (FlumeConfiguration.get().getEventRPC().equals(
            FlumeConfiguration.RPC_TYPE_AVRO)) {
          return new AvroEventSink(host, port);
        }
        LOG.warn("event.rpc.type not defined.  It should be either"
            + " \"THRIFT\" or \"AVRO\". Defaulting to Thrift");
        return new ThriftEventSink(host, port);
      }
    };
  }
}
