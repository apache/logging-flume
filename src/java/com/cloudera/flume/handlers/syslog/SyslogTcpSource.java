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

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.text.EventExtractException;
import com.google.common.base.Preconditions;

/**
 * First cut at a tcp source listening for tcp-based syslog data. This is only
 * good for a single connection.
 * 
 * syslog entries sent over tcp are simply delimited by '\n' characters and are
 * otherwise identical in format to udp-based syslog data.
 */
public class SyslogTcpSource extends EventSource.Base {
  public final static int SYSLOG_TCP_PORT = 514;
  ServerSocket sock = null;
  int port = SYSLOG_TCP_PORT; // this is syslog-ng's default tcp port.
  DataInputStream is;
  long rejects = 0;

  public SyslogTcpSource(int port) {
    this.port = port;
  }

  public SyslogTcpSource() {
  }

  @Override
  public void close() throws IOException {
    sock.close();
    sock = null;
  }

  @Override
  public Event next() throws IOException {
    Event e = null;
    while (true) {
      try {
        e = SyslogWireExtractor.extractEvent(is);
        return e;
      } catch (EventExtractException ex) {
        rejects++;
      }
    }
  }

  @Override
  public void open() throws IOException {
    Preconditions.checkState(sock == null);
    sock = new ServerSocket(port);
    Socket client = sock.accept();
    is = new DataInputStream(client.getInputStream());
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(String... argv) {
        int port = SYSLOG_TCP_PORT; // default udp port, need root permissions
        // for this.
        if (argv.length > 1) {
          throw new IllegalArgumentException("usage: syslogTcp1([port no]) ");
        }

        if (argv.length == 1) {
          port = Integer.parseInt(argv[0]);
        }

        return new SyslogTcpSource(port);
      }

    };
  }

}
