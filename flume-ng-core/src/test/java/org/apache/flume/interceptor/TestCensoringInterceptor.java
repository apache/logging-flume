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
package org.apache.flume.interceptor;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCensoringInterceptor {

  Logger logger =
      LoggerFactory.getLogger(TestCensoringInterceptor.class);

  @Test
  public void testCensor() {

    MemoryChannel memCh = new MemoryChannel();
    memCh.configure(new Context());
    memCh.start();

    ChannelSelector cs = new ReplicatingChannelSelector();
    cs.setChannels(Lists.<Channel>newArrayList(memCh));
    ChannelProcessor cp = new ChannelProcessor(cs);

    // source config
    Map<String, String> cfgMap = Maps.newHashMap();
    cfgMap.put("interceptors", "a");
    String builderClass = CensoringInterceptor.Builder.class.getName();
    cfgMap.put("interceptors.a.type", builderClass);
    Context ctx = new Context(cfgMap);

    // setup
    cp.configure(ctx);
    cp.initialize();

    Map<String, String> headers = Maps.newHashMap();
    String badWord = "scribe";
    headers.put("Bad-Words", badWord);
    Event event1 = EventBuilder.withBody("test", Charsets.UTF_8, headers);
    Assert.assertEquals(badWord, event1.getHeaders().get("Bad-Words"));
    cp.processEvent(event1);

    Transaction tx = memCh.getTransaction();
    tx.begin();

    Event event1a = memCh.take();
    Assert.assertNull(event1a.getHeaders().get("Bad-Words"));

    tx.commit();
    tx.close();

    // cleanup / shutdown
    cp.close();
    memCh.stop();
  }
}
