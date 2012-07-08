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
package com.cloudera.flume.handlers.syslog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.google.common.base.Preconditions;

/**
 * This generates a syslog output sink. This is used primarly for testing the
 * syslogTcpSources, but could be used to actually write data to existing syslog
 * servers.
 */
public class SyslogTcpSink extends EventSink.Base {

  String dsthost;
  int port;
  Socket sock = null;

  public SyslogTcpSink(String host, int port) {
    Preconditions.checkNotNull(host);
    Preconditions.checkArgument(port > 0);
    this.dsthost = host;
    this.port = port;
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    byte[] data = SyslogWireExtractor.formatEventToBytes(e);
    sock.getOutputStream().write(data);
    super.append(e);
  }

  @Override
  public void close() throws IOException {
    sock.close();
    sock = null;
  }

  @Override
  public void open() throws IOException {
    sock = new Socket();
    sock.connect(new InetSocketAddress(dsthost, port));
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {

      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length > 0 && argv.length <= 2,
            "usage: syslogTcp(host[, port=514])");

        int port = SyslogTcpSource.SYSLOG_TCP_PORT;
        String host = argv[0];

        if (argv.length > 1) {
          port = Integer.parseInt(argv[1]);
        }

        return new SyslogTcpSink(host, port);
      }

    };
  }
}
