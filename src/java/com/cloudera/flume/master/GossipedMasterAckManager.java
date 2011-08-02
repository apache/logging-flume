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
package com.cloudera.flume.master;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.cloudera.distributed.GossipMulticast;
import com.cloudera.distributed.Group;
import com.cloudera.distributed.MessageReceiver;
import com.cloudera.distributed.TCPNodeId;
import com.cloudera.distributed.GossipMulticast.GossipMessage;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.Pair;

/**
 * An implementation of an AckManager that uses Gossip to send acknowledgements
 * to other servers.
 */
public class GossipedMasterAckManager extends MasterAckManager 
implements MessageReceiver<GossipMulticast.GossipMessage> {
  Logger LOG = Logger.getLogger(GossipedMasterAckManager.class);
  GossipMulticast gossip;
  final Group group;
  final int port;
  
  GossipedMasterAckManager(FlumeConfiguration conf) {    
    this(conf.getConfigMasterGossipHostsList(),    
        conf.getMasterGossipPort());    
  }
  
  GossipedMasterAckManager(List<Pair<String,Integer>> peers, int port) {
    group = new Group();
    for (Pair<String,Integer> p : peers) {
      group.addNode(new TCPNodeId(p.getLeft(),p.getRight()));
    }
    this.port = port;
    // TODO(henry) choose the interface from config
    gossip = new GossipMulticast(group, new TCPNodeId("0.0.0.0", port));
  }
  
  /**
   * Start the underlying gossip server (and throw an exception if it fails)
   */
  synchronized public void start() throws IOException {
    gossip.registerReceiver(this);
    gossip.start();    
  }
  
  /**
   * Stop the underlying gossip server
   */
  synchronized public void stop() {
    try {
      gossip.stop();
    } catch (InterruptedException e) {
      LOG.warn("Gossip interrupted while stopping in GossipedAckManager", e);
    }
  }
  
  synchronized public void acknowledge(String ackid) {
    super.acknowledge(ackid);
    gossip.sendToGroup(group, ackid.getBytes());    
  }

  @Override
  synchronized public void receiveMessage(GossipMessage msg) {
    String ackid = new String(msg.getContents());
    LOG.info("Received ACK at " + port + " from gossip " + ackid);
    super.acknowledge(new String(msg.getContents()));
  }  
}
