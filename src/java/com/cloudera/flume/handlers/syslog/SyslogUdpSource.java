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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.text.EventExtractException;
import com.cloudera.util.ByteBufferInputStream;

/**
 * This handles syslog wire data from udp. Each packet is one log event and is
 * formated like so (rfc 5426):
 * 
 * '<' PRI '>' VERSION ' ' stuff from a line in a file
 * 
 * VERSION Doesn't seem to appear in my logs, so I will just append it to "stuff". 
 * 
 * Where PRI is a decimal number from 0-199
 * 
 */
public class SyslogUdpSource extends EventSource.Base {
  final static Logger LOG = Logger.getLogger(EventSource.class.getName());
  final public static int SYSLOG_UDP_PORT = 514;
  int port = SYSLOG_UDP_PORT; // default udp syslog port
  int maxsize = 1 << 16; // 64k is max allowable in RFC 5426

  long rejects = 0;
  DatagramSocket sock;

  public SyslogUdpSource() {
  }

  public SyslogUdpSource(int port) {
    this.port = port;
  }

  @Override
  public void close() throws IOException {
    LOG.info("closing SyslogUdpSource on port " + port);
    if (sock == null) {
      LOG.warn("double close of SyslogUdpSocket on udp:" + port
          + " , (this is ok but odd)");
      return;
    }

    sock.close();
  }

  @Override
  public Event next() throws IOException {
    byte[] buf = new byte[maxsize];
    DatagramPacket pkt = new DatagramPacket(buf, maxsize);
    Event e = null;
    do { // loop until we get a valid packet, drop bad ones.
      sock.receive(pkt);

      ByteBuffer bb = ByteBuffer.wrap(buf, 0, pkt.getLength());
      ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
      DataInputStream in = new DataInputStream(bbis);
      try {
        e = SyslogWireExtractor.extractEvent(in);
      } catch (EventExtractException ex) {
        rejects++;
        LOG.warn(rejects + " rejected packets. packet: " + pkt, ex);
        LOG.debug("raw bytes " + Arrays.toString(pkt.getData()));
        // TODO (jon) maybe have a hook here to do something with rejects
      }

      // need a sane way to fall out of his loop.
    } while (e == null);

    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void open() throws IOException {
    sock = new DatagramSocket(port);
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(String... argv) {
        int port = SYSLOG_UDP_PORT; // default udp port, need root permissions
        // for this.
        if (argv.length > 1) {
          throw new IllegalArgumentException("usage: syslogUdp([port no]) ");
        }

        if (argv.length == 1) {
          port = Integer.parseInt(argv[0]);
        }

        return new SyslogUdpSource(port);
      }

    };
  }

}
