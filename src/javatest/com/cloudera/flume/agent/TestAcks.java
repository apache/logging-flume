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
package com.cloudera.flume.agent;

import java.io.IOException;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.debug.ConsoleEventSink;
import com.cloudera.flume.handlers.endtoend.AckChecksumChecker;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.endtoend.CollectorAckListener;

/**
 * This tests the flow of messages and acks through a mock'ed out master
 */
public class TestAcks {

  // check that agents properly manage a list of outstanding acks
  @Test
  public void testAckAgent() throws IOException, TException {
    // mock master rpc interface
    MockMasterRPC svr = new MockMasterRPC();

    // agent side, inject ack messages
    WALAckManager aac = new WALAckManager(svr, new AckListener.Empty(),
        FlumeConfiguration.get().getAgentAckedRetransmit());
    byte[] tag = "canned tag".getBytes();
    AckChecksumInjector<EventSink> inj = new AckChecksumInjector<EventSink>(
        new ConsoleEventSink(), tag, aac.getAgentAckQueuer());

    // send a bunch of messages and then close out.
    inj.open();
    for (int i = 0; i < 10; i++) {
      Event e = new EventImpl(("This is a test " + i).getBytes());
      inj.append(e);
    }
    inj.close();

    // agent should have pending stuff.
    System.out.println("Agent: Pending " + aac.pending);
    Assert.assertEquals(1, aac.pending.size());

    // periodically trigger master check (this one doesn't change agent's state)
    aac.checkAcks();
    Assert.assertEquals(1, aac.pending.size());

    // simulate the collector by sending "acknowledge" message to master, agent
    // doesn't know about this.
    String ackid = aac.pending.keys().nextElement();
    svr.acknowledge(ackid);
    Assert.assertEquals(1, aac.pending.size());
    System.out.println("Agent: (still) pending " + aac.pending);

    // agent checks and learns of new acks. agent learns that it no longer have
    // to worry about this ack.
    aac.checkAcks();
    Assert.assertEquals(0, aac.pending.size());
    System.out.println("Agent: No more pending " + aac.pending);
  }

  // check that collectors/receivers properly notify master of received complete
  // acks.
  @Test
  public void testAckAgentCollector() throws IOException, TException {
    // mock master rpc interface
    MockMasterRPC svr = new MockMasterRPC();

    // collector side, receive acks messages, checks acks, and notifies master.
    AckChecksumChecker<EventSink> chk = new AckChecksumChecker<EventSink>(
        new ConsoleEventSink(), new CollectorAckListener(svr));

    // agent side, inject ack messages
    WALAckManager aac = new WALAckManager(svr, new AckListener.Empty(),
        FlumeConfiguration.get().getAgentAckedRetransmit());
    byte[] tag = "canned tag".getBytes();
    AckChecksumInjector<EventSink> inj = new AckChecksumInjector<EventSink>(
        chk, tag, aac.getAgentAckQueuer());

    // send a bunch of messages and then close out.
    inj.open();
    for (int i = 0; i < 10; i++) {
      Event e = new EventImpl(("This is a test " + i).getBytes());
      inj.append(e);
    }
    inj.close();

    // agent should have pending stuff.
    System.out.println("Agent: Pending " + aac.pending);
    Assert.assertEquals(1, aac.pending.size());

    // periodically trigger master check (this one doesn't change agent's state)
    aac.checkAcks();
    System.out.println("Agent: No more pending " + aac.pending);
    Assert.assertEquals(0, aac.pending.size());
  }

  // error cases. Make sure things eventually make it.
}
