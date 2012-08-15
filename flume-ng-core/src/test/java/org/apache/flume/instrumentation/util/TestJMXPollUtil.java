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
package org.apache.flume.instrumentation.util;

import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestJMXPollUtil {

  Channel memChannel = new MemoryChannel();
  Channel pmemChannel = new PseudoTxnMemoryChannel();

  @Test
  public void testJMXPoll() {
    memChannel.setName("memChannel");
    pmemChannel.setName("pmemChannel");
    Context c = new Context();
    Configurables.configure(memChannel, c);
    Configurables.configure(pmemChannel, c);
    memChannel.start();
    pmemChannel.start();
    Transaction txn = memChannel.getTransaction();
    txn.begin();
    memChannel.put(EventBuilder.withBody("blah".getBytes()));
    memChannel.put(EventBuilder.withBody("blah".getBytes()));
    txn.commit();
    txn.close();

    txn = memChannel.getTransaction();
    txn.begin();
    memChannel.take();
    txn.commit();
    txn.close();


    Transaction txn2 = pmemChannel.getTransaction();
    txn2.begin();
    pmemChannel.put(EventBuilder.withBody("blah".getBytes()));
    pmemChannel.put(EventBuilder.withBody("blah".getBytes()));
    txn2.commit();
    txn2.close();

    txn2 = pmemChannel.getTransaction();
    txn2.begin();
    pmemChannel.take();
    txn2.commit();
    txn2.close();

    Map<String, Map<String, String>> mbeans = JMXPollUtil.getAllMBeans();
    Assert.assertNotNull(mbeans);
    Map<String, String> memBean = mbeans.get("CHANNEL.memChannel");
    Assert.assertNotNull(memBean);
    JMXTestUtils.checkChannelCounterParams(memBean);
    Map<String, String> pmemBean = mbeans.get("CHANNEL.pmemChannel");
    Assert.assertNotNull(pmemBean);
    JMXTestUtils.checkChannelCounterParams(pmemBean);
    memChannel.stop();
    pmemChannel.stop();
  }
}
