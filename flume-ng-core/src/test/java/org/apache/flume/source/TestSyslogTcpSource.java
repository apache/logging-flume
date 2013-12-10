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
package org.apache.flume.source;

import com.google.common.base.Charsets;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class TestSyslogTcpSource {
  private static final org.slf4j.Logger logger =
    LoggerFactory.getLogger(TestSyslogTcpSource.class);
  private SyslogTcpSource source;
  private Channel channel;
  private static final int TEST_SYSLOG_PORT = 0;
  private final DateTime time = new DateTime();
  private final String stamp1 = time.toString();
  private final String host1 = "localhost.localdomain";
  private final String data1 = "test syslog data";
  private final String bodyWithTandH = "<10>" + stamp1 + " " + host1 + " " +
      data1 + "\n";

  private void init(boolean keepFields){
    source = new SyslogTcpSource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
    Context context = new Context();
    context.put("port", String.valueOf(TEST_SYSLOG_PORT));
    context.put("keepFields", String.valueOf(keepFields));

    source.configure(context);

  }
  /** Tests the keepFields configuration parameter (enabled or disabled)
   using SyslogTcpSource.*/
  private void runKeepFieldsTest(boolean keepFields) throws IOException {
    init(keepFields);
    source.start();
    // Write some message to the syslog port
    Socket syslogSocket;
    for (int i = 0; i < 10 ; i++) {
      syslogSocket = new Socket(
        InetAddress.getLocalHost(), source.getSourcePort());
      syslogSocket.getOutputStream().write(bodyWithTandH.getBytes());
      syslogSocket.close();
    }

    List<Event> channelEvents = new ArrayList<Event>();
    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 10; i++) {
      Event e = channel.take();
      if (e == null) {
        throw new NullPointerException("Event is null");
      }
      channelEvents.add(e);
    }

    try {
      txn.commit();
    } catch (Throwable t) {
      txn.rollback();
    } finally {
      txn.close();
    }

    source.stop();
    for (Event e : channelEvents) {
      Assert.assertNotNull(e);
      String str = new String(e.getBody(), Charsets.UTF_8);
      logger.info(str);
      if (keepFields) {
        Assert.assertArrayEquals(bodyWithTandH.trim().getBytes(),
          e.getBody());
      } else if (!keepFields) {
        Assert.assertArrayEquals(data1.getBytes(), e.getBody());
      }
    }
  }

  @Test
  public void testKeepFields () throws IOException {
    runKeepFieldsTest(true);
  }

  @Test
  public void testRemoveFields() throws IOException{
      runKeepFieldsTest(false);
    }
  }

