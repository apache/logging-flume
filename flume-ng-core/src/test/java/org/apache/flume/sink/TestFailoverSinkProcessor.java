/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.sink;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.SinkRunner;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.Test;

public class TestFailoverSinkProcessor {

  // a simple sink for predictable testing purposes that fails after
  // a given number of events have been consumed
  class ConsumeXSink implements Sink {
    volatile int remaining;
    private LifecycleState state;
    private String name;
    private Channel channel;
    private Integer written;

    public ConsumeXSink(int consumeCount) {
      remaining = consumeCount;
      written = 0;
    }

    @Override
    public void start() {
      state = LifecycleState.START;
    }

    @Override
    public void stop() {
      state = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getLifecycleState() {
      return state;
    }

    @Override
    public void setName(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void setChannel(Channel channel) {
      this.channel = channel;
    }

    @Override
    public Channel getChannel() {
      return channel;
    }

    public synchronized void setRemaining(int remaining) {
      this.remaining = remaining;
    }

    @Override
    public Status process() throws EventDeliveryException {
      synchronized (this) {
        if (remaining <= 0) {
          throw new EventDeliveryException("can't consume more");
        }
      }

      Transaction tx = channel.getTransaction();
      tx.begin();
      Event e = channel.take();
      tx.commit();
      tx.close();

      if (e != null) {
        synchronized (this) {
          remaining--;
        }
        written++;
      }

      return Status.READY;
    }

    public Integer getWritten() {
      return written;
    }
  }

  /**
   * Test failover by feeding events to the channel and verifying at various
   * stages that the number of events consumed by each sink matches expected
   * failover patterns
   *
   * @throws InterruptedException
   */
  @Test
  public void testFailover() throws InterruptedException {
    Channel ch = new MemoryChannel();

    ConsumeXSink s1 = new ConsumeXSink(10);
    s1.setChannel(ch);
    s1.setName("s1");
    ConsumeXSink s2 = new ConsumeXSink(50);
    s2.setChannel(ch);
    s2.setName("s2");
    ConsumeXSink s3 = new ConsumeXSink(100);
    s3.setChannel(ch);
    s3.setName("s3");

    Context context = new Context();
    Configurables.configure(s1, context);
    Configurables.configure(s2, context);
    Configurables.configure(s3, context);
    Configurables.configure(ch, context);
    ch.start();
    List<Sink> sinks = new LinkedList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);
    SinkGroup group = new SinkGroup(sinks);
    Map<String, String> params = new HashMap<String, String>();
    params.put("sinks", "s1 s2 s3");
    params.put("processor.type", "failover");
    params.put("processor.priority.s1", "3");
    params.put("processor.priority.s2", "2");
    params.put("processor.priority.s3", "1");
    params.put("processor.maxpenalty", "10000");
    context.putAll(params);
    Configurables.configure(group, context);
    SinkRunner runner = new SinkRunner(group.getProcessor());
    runner.start();
    Assert.assertEquals(LifecycleState.START, s1.getLifecycleState());
    Assert.assertEquals(LifecycleState.START, s2.getLifecycleState());
    Assert.assertEquals(LifecycleState.START, s3.getLifecycleState());
    for (int i = 0; i < 15; i++) {
      Transaction tx = ch.getTransaction();
      tx.begin();
      ch.put(EventBuilder.withBody("test".getBytes()));
      tx.commit();
      tx.close();
    }
    Thread.sleep(100);

    Assert.assertEquals(new Integer(10), s1.getWritten());
    Assert.assertEquals(new Integer(5), s2.getWritten());
    for (int i = 0; i < 50; i++) {
      Transaction tx = ch.getTransaction();
      tx.begin();
      ch.put(EventBuilder.withBody("test".getBytes()));
      tx.commit();
      tx.close();
    }
    Thread.sleep(100);

    Assert.assertEquals(new Integer(50), s2.getWritten());
    Assert.assertEquals(new Integer(5), s3.getWritten());
    // test rollover to recovered servers
    s2.setRemaining(20);

    // get us past the retry time for the failed sink
    Thread.sleep(5000);

    for (int i = 0; i < 100; i++) {
      Transaction tx = ch.getTransaction();
      tx.begin();
      ch.put(EventBuilder.withBody("test".getBytes()));
      tx.commit();
      tx.close();
    }
    Thread.sleep(1000);

    Assert.assertEquals(new Integer(10), s1.getWritten());
    Assert.assertEquals(new Integer(70), s2.getWritten());
    Assert.assertEquals(new Integer(85), s3.getWritten());

    runner.stop();
    ch.stop();
  }

}
