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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestExecSource {

  private AbstractSource source;

  @Before
  public void setUp() {
    source = new ExecSource();
  }

  @Test
  public void testProcess() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    Channel channel = new MemoryChannel();
    Context context = new Context();

    context.put("command", "cat /etc/passwd");
    context.put("keep-alive", "1");
    context.put("capacity", "1000");
    context.put("transactionCapacity", "1000");
    Configurables.configure(source, context);
    Configurables.configure(channel, context);

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));

    source.start();
    Transaction transaction = channel.getTransaction();

    transaction.begin();
    Event event;

    FileOutputStream outputStream = new FileOutputStream(
        "/tmp/flume-execsource." + Thread.currentThread().getId());

    while ((event = channel.take()) != null) {
      outputStream.write(event.getBody());
      outputStream.write('\n');
    }

    outputStream.close();
    transaction.commit();
    transaction.close();

    source.stop();

    File file1 = new File("/tmp/flume-execsource."
        + Thread.currentThread().getId());
    File file2 = new File("/etc/passwd");
    Assert.assertEquals(FileUtils.checksumCRC32(file1),
        FileUtils.checksumCRC32(file2));
    FileUtils.forceDelete(file1);
  }

}
