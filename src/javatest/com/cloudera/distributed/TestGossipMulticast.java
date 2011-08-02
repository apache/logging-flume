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
package com.cloudera.distributed;

import java.io.IOException;

import org.apache.log4j.Logger;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestGossipMulticast {
  Logger LOG = Logger.getLogger(TestGossipMulticast.class);
  
  @Test
  public void testGossip() throws InterruptedException, IOException {
    int count = 10;
    Group group1 = new Group();
    GossipMulticast[] gossips = new GossipMulticast[count];
    for (int i=0;i<count;++i) {
      group1.addNode(new TCPNodeId("localhost",23456+i));      
    }
    LOG.info("Starting");
    for (int i=0;i<count;++i) {
      gossips[i] = new GossipMulticast(group1,new TCPNodeId("localhost",23456+i));
      gossips[i].start();
    }
    gossips[0].sendToGroup(group1, "Msg 0".getBytes());
    gossips[1].sendToGroup(group1, "Msg 1".getBytes());
    gossips[0].sendToGroup(group1, "Msg 2".getBytes());

    gossips[5].sendToGroup(group1, "Msg 3".getBytes());
    gossips[4].sendToGroup(group1, "Msg 4".getBytes());
    gossips[6].sendToGroup(group1, "Msg 5".getBytes());
    LOG.info("Sleeping");
    Thread.sleep(15000);
    for (GossipMulticast g : gossips) {
      LOG.info("Stopping " + g.node.toString());
      g.stop();           
    }        
    
    for (GossipMulticast g : gossips) {
      LOG.info("Gossip " + g.node.toString() + " got " 
          + g.msgQueue.size() + " messages");  
      assertEquals(6, g.msgQueue.size());
    }
  }
  
  /**
   * Check that repeated starts and stops don't throw an exception
   */
  @Test
  public void testStartAndStop() throws IOException, InterruptedException {
    Group group = new Group();
    TCPNodeId node = new TCPNodeId("localhost", 24567);
    group.addNode(node);
    GossipMulticast mcast = new GossipMulticast(group, node);
    for (int i=0;i<10;++i) {
      mcast.start();
      mcast.stop();      
    }
  }        
}
