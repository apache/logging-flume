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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.channel.AbstractChannel;
import org.junit.Test;

public class TestLoadBalancingSinkProcessor {

  private Context getContext(String selectorType, boolean backoff) {
    Map<String, String> p = new HashMap<String, String>();
    p.put("selector", selectorType);
    p.put("backoff", String.valueOf(backoff));
    Context ctx = new Context(p);

    return ctx;
  }

  private Context getContext(String selectorType) {
    Map<String, String> p = new HashMap<String, String>();
    p.put("selector", selectorType);
    Context ctx = new Context(p);

    return ctx;
  }

  private LoadBalancingSinkProcessor getProcessor(
      String selectorType, List<Sink> sinks, boolean backoff) {
    return getProcessor(sinks, getContext(selectorType, backoff));
  }

  private LoadBalancingSinkProcessor getProcessor(List<Sink> sinks, Context ctx)
  {
    LoadBalancingSinkProcessor lbsp = new LoadBalancingSinkProcessor();
    lbsp.setSinks(sinks);
    lbsp.configure(ctx);
    lbsp.start();

    return lbsp;
  }

  @Test
  public void testDefaultConfiguration() throws Exception {
    // If no selector is specified, the round-robin selector should be used
    Channel ch = new MockChannel();
    int n = 100;
    int numEvents = 3*n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);

    MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    LoadBalancingSinkProcessor lbsp = getProcessor(sinks, new Context());

    Status s = Status.READY;
    while (s != Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertTrue(s1.getEvents().size() == n);
    Assert.assertTrue(s2.getEvents().size() == n);
    Assert.assertTrue(s3.getEvents().size() == n);

  }

  @Test
  public void testRandomOneActiveSink() throws Exception {
    Channel ch = new MockChannel();
    int n = 10;
    int numEvents = n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    // s1 always fails
    s1.setFail(true);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);


    MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    // s3 always fails
    s3.setFail(true);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    LoadBalancingSinkProcessor lbsp = getProcessor("random", sinks, false);

    Sink.Status s = Sink.Status.READY;
    while (s != Sink.Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertTrue(s1.getEvents().size() == 0);
    Assert.assertTrue(s2.getEvents().size() == n);
    Assert.assertTrue(s3.getEvents().size() == 0);
  }

  @Test
  public void testRandomBackoff() throws Exception {
    Channel ch = new MockChannel();
    int n = 100;
    int numEvents = n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    // s1 always fails
    s1.setFail(true);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);

    MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    // s3 always fails
    s3.setFail(true);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    LoadBalancingSinkProcessor lbsp = getProcessor("random", sinks, true);

    // TODO: there is a remote possibility that s0 or s2
    // never get hit by the random assignment
    // and thus not backoffed, causing the test to fail
    for(int i=0; i < 50; i++) {
      // a well behaved runner would always check the return.
      lbsp.process();
    }
    Assert.assertEquals(50, s2.getEvents().size());
    s2.setFail(true);
    s1.setFail(false); // s1 should still be backed off
    try {
      lbsp.process();
      // nothing should be able to process right now
      Assert.fail("Expected EventDeliveryException");
    } catch (EventDeliveryException e) {
      // this is expected
    }
    Thread.sleep(2100); // wait for s1 to no longer be backed off
    Sink.Status s = Sink.Status.READY;
    while (s != Sink.Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertEquals(50, s1.getEvents().size());
    Assert.assertEquals(50, s2.getEvents().size());
    Assert.assertEquals(0, s3.getEvents().size());
  }

  @Test
  public void testRandomPersistentFailure() throws Exception {
    Channel ch = new MockChannel();
    int n = 100;
    int numEvents = 3*n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);

    // s2 always fails
    s2.setFail(true);

    MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    LoadBalancingSinkProcessor lbsp = getProcessor("random",sinks, false);

    Status s = Status.READY;
    while (s != Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertTrue(s2.getEvents().size() == 0);
    Assert.assertTrue(s1.getEvents().size() + s3.getEvents().size() == 3*n);
  }

  @Test
  public void testRandomNoFailure() throws Exception {

    Channel ch = new MockChannel();
    int n = 10000;
    int numEvents = n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);

    MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    MockSink s4 = new MockSink(4);
    s4.setChannel(ch);

    MockSink s5 = new MockSink(5);
    s5.setChannel(ch);

    MockSink s6 = new MockSink(6);
    s6.setChannel(ch);

    MockSink s7 = new MockSink(7);
    s7.setChannel(ch);

    MockSink s8 = new MockSink(8);
    s8.setChannel(ch);

    MockSink s9 = new MockSink(9);
    s9.setChannel(ch);

    MockSink s0 = new MockSink(0);
    s0.setChannel(ch);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);
    sinks.add(s4);
    sinks.add(s5);
    sinks.add(s6);
    sinks.add(s7);
    sinks.add(s8);
    sinks.add(s9);
    sinks.add(s0);

    LoadBalancingSinkProcessor lbsp = getProcessor("random",sinks, false);

    Status s = Status.READY;
    while (s != Status.BACKOFF) {
      s = lbsp.process();
    }

    Set<Integer> sizeSet = new HashSet<Integer>();
    int sum = 0;
    for (Sink ms : sinks) {
      int count = ((MockSink) ms).getEvents().size();
      sum += count;
      sizeSet.add(count);
    }

    // Assert that all the events were accounted for
    Assert.assertEquals(n, sum);

    // Assert that at least two sinks came with different event sizes.
    // This makes sense if the total number of events is evenly divisible by
    // the total number of sinks. In which case the round-robin policy will
    // end up causing all sinks to get the same number of events where as
    // the random policy will have very low probability of doing that.
    Assert.assertTrue("Miraculous distribution", sizeSet.size() > 1);
  }



  @Test
  public void testRoundRobinOneActiveSink() throws Exception {
    Channel ch = new MockChannel();
    int n = 10;
    int numEvents = n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    // s1 always fails
    s1.setFail(true);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);


    MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    // s3 always fails
    s3.setFail(true);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    LoadBalancingSinkProcessor lbsp = getProcessor("round_robin", sinks, false);

    Sink.Status s = Sink.Status.READY;
    while (s != Sink.Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertTrue(s1.getEvents().size() == 0);
    Assert.assertTrue(s2.getEvents().size() == n);
    Assert.assertTrue(s3.getEvents().size() == 0);
  }

  @Test
  public void testRoundRobinPersistentFailure() throws Exception {
    Channel ch = new MockChannel();
    int n = 100;
    int numEvents = 3*n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);

    // s2 always fails
    s2.setFail(true);

    MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    LoadBalancingSinkProcessor lbsp = getProcessor("round_robin",sinks, false);

    Status s = Status.READY;
    while (s != Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertTrue(s1.getEvents().size() == n);
    Assert.assertTrue(s2.getEvents().size() == 0);
    Assert.assertTrue(s3.getEvents().size() == 2*n);
  }

  // test that even if the sink recovers immediately that it is kept out of commission briefly
  // test also verifies that when a sink fails, events are balanced over remaining sinks
  @Test
  public void testRoundRobinBackoffInitialFailure() throws EventDeliveryException {
    Channel ch = new MockChannel();
    int n = 100;
    int numEvents = 3*n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);

      MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    LoadBalancingSinkProcessor lbsp = getProcessor("round_robin",sinks, true);

    Status s = Status.READY;
    for (int i = 0; i < 3 && s != Status.BACKOFF; i++) {
      s = lbsp.process();
    }
    s2.setFail(true);
    for (int i = 0; i < 3 && s != Status.BACKOFF; i++) {
      s = lbsp.process();
    }
    s2.setFail(false);
    while (s != Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertEquals((3 * n) / 2, s1.getEvents().size());
    Assert.assertEquals(1, s2.getEvents().size());
    Assert.assertEquals((3 * n) /2 - 1, s3.getEvents().size());
  }

  @Test
  public void testRoundRobinBackoffIncreasingBackoffs() throws EventDeliveryException, InterruptedException {
    Channel ch = new MockChannel();
    int n = 100;
    int numEvents = 3*n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);
    s2.setFail(true);

      MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    LoadBalancingSinkProcessor lbsp = getProcessor("round_robin",sinks, true);

    Status s = Status.READY;
    for (int i = 0; i < 3 && s != Status.BACKOFF; i++) {
      s = lbsp.process();
    }
    Assert.assertEquals(0, s2.getEvents().size());
    Thread.sleep(2100);
    // this should let the sink come out of backoff and get backed off  for a longer time
    for (int i = 0; i < 3 && s != Status.BACKOFF; i++) {
      s = lbsp.process();
    }
    Assert.assertEquals(0, s2.getEvents().size());
    s2.setFail(false);
    Thread.sleep(2100);
    // this time it shouldn't come out of backoff yet as the timeout isn't over
    for (int i = 0; i < 3 && s != Status.BACKOFF; i++) {
      s = lbsp.process();
    }
    Assert.assertEquals(0, s2.getEvents().size());
    // after this s2 should be receiving events agains
    Thread.sleep(2100);
    while (s != Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertEquals( n + 2, s1.getEvents().size());
    Assert.assertEquals( n - 3, s2.getEvents().size());
    Assert.assertEquals( n + 1, s3.getEvents().size());
  }

  @Test
  public void testRoundRobinBackoffFailureRecovery() throws EventDeliveryException, InterruptedException {
    Channel ch = new MockChannel();
    int n = 100;
    int numEvents = 3*n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);
    s2.setFail(true);

      MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    LoadBalancingSinkProcessor lbsp = getProcessor("round_robin",sinks, true);

    Status s = Status.READY;
    for (int i = 0; i < 3 && s != Status.BACKOFF; i++) {
      s = lbsp.process();
    }
    s2.setFail(false);
    Thread.sleep(2001);
    while (s != Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertEquals(n + 1, s1.getEvents().size());
    Assert.assertEquals(n - 1,  s2.getEvents().size());
    Assert.assertEquals(n, s3.getEvents().size());
  }


  @Test
  public void testRoundRobinNoFailure() throws Exception {

    Channel ch = new MockChannel();
    int n = 100;
    int numEvents = 3*n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);

    MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    LoadBalancingSinkProcessor lbsp = getProcessor("round_robin",sinks, false);

    Status s = Status.READY;
    while (s != Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertTrue(s1.getEvents().size() == n);
    Assert.assertTrue(s2.getEvents().size() == n);
    Assert.assertTrue(s3.getEvents().size() == n);
  }

  @Test
  public void testCustomSelector() throws Exception {
    Channel ch = new MockChannel();
    int n = 10;
    int numEvents = n;
    for (int i = 0; i < numEvents; i++) {
      ch.put(new MockEvent("test" + i));
    }

    MockSink s1 = new MockSink(1);
    s1.setChannel(ch);

    // s1 always fails
    s1.setFail(true);

    MockSink s2 = new MockSink(2);
    s2.setChannel(ch);

    MockSink s3 = new MockSink(3);
    s3.setChannel(ch);

    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(s1);
    sinks.add(s2);
    sinks.add(s3);

    // This selector will result in all events going to s2
    Context ctx = getContext(FixedOrderSelector.class.getCanonicalName());
    ctx.put("selector." + FixedOrderSelector.SET_ME, "foo");
    LoadBalancingSinkProcessor lbsp = getProcessor(sinks, ctx);

    Sink.Status s = Sink.Status.READY;
    while (s != Sink.Status.BACKOFF) {
      s = lbsp.process();
    }

    Assert.assertTrue(s1.getEvents().size() == 0);
    Assert.assertTrue(s2.getEvents().size() == n);
    Assert.assertTrue(s3.getEvents().size() == 0);
  }

  private static class MockSink extends AbstractSink {

    private final int id;

    private List<Event> events = new ArrayList();

    private boolean fail = false;

    private MockSink(int id) {
      this.id = id;
    }

    List<Event> getEvents() {
      return events;
    }

    int getId() {
      return id;
    }

    void setFail(boolean bFail) {
      fail = bFail;
    }

    @Override
    public Status process() throws EventDeliveryException {
      if (fail) {
        throw new EventDeliveryException("failed");
      }
      Event e = this.getChannel().take();
      if (e == null)
        return Status.BACKOFF;

      events.add(e);
      return Status.READY;
    }
  }

  private static class MockChannel extends AbstractChannel {

    private List<Event> events = new ArrayList<Event>();

    @Override
    public void put(Event event) throws ChannelException {
      events.add(event);
    }

    @Override
    public Event take() throws ChannelException {
      if (events.size() > 0) {
        return events.remove(0);
      }
      return null;
    }

    @Override
    public Transaction getTransaction() {
      return null;
    }

  }

  private static class MockEvent implements Event {

    private static final Map<String, String> EMPTY_HEADERS =
        Collections.unmodifiableMap(new HashMap<String, String>());

    private byte[] body;

    MockEvent(String str) {
      this.body = str.getBytes();
    }

    @Override
    public Map<String, String> getHeaders() {
      return EMPTY_HEADERS;
    }

    @Override
    public void setHeaders(Map<String, String> headers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBody() {
      return body;
    }

    @Override
    public void setBody(byte[] body) {
      this.body = body;
    }
  }
}
