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

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SyslogAppender;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.SyslogUtils;

public class TestSyslogUdpSource {
  private SyslogUDPSource source;
  private Channel channel;
  private static final int TEST_SYSLOG_PORT = 14455;

  @Before
  public void setUp() {
    source = new SyslogUDPSource(); //SyslogTcpSource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
    Context context = new Context();
    context.put("port", String.valueOf(TEST_SYSLOG_PORT));
    source.configure(context);
  }

  @Test
  public void testAppend() throws InterruptedException {
    Logger logger = Logger.getLogger(getClass());
    // use the Apache syslog appender to write to syslog source
    SyslogAppender appender = new SyslogAppender(null,
        "localhost:"+TEST_SYSLOG_PORT, SyslogAppender.LOG_FTP);
    logger.addAppender(appender);
    Event e = null;
    Event e2 = null;

    source.start();

    // write to syslog
    logger.info("test flume syslog");
    logger.info("");

    Transaction txn = channel.getTransaction();
    try {
      txn.begin();
      e = channel.take();
      e2 = channel.take();
      txn.commit();
    } finally {
      txn.close();
    }

    source.stop();
    logger.removeAppender(appender);

    Assert.assertNotNull(e);
    Assert.assertEquals(String.valueOf(SyslogAppender.LOG_FTP / 8),
        e.getHeaders().get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertArrayEquals(e.getBody(), "test flume syslog".getBytes());

    Assert.assertNotNull(e2);
    Assert.assertEquals(String.valueOf(SyslogAppender.LOG_FTP / 8),
        e2.getHeaders().get(SyslogUtils.SYSLOG_FACILITY));
    Assert.assertArrayEquals(e2.getBody(), "".getBytes());
  }

}
