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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.Logger;

import com.cloudera.util.Clock;
import com.cloudera.util.Pair;

public class TestGossipedAckManager {
  Logger LOG = Logger.getLogger(TestGossipedAckManager.class);
  
  @Test
  public void testGAM() throws InterruptedException, IOException {
    List<Pair<String,Integer>> peers = new ArrayList<Pair<String,Integer>>();    
    int count = 10;
    ArrayList<GossipedMasterAckManager> managers = new ArrayList<GossipedMasterAckManager>(count);
    for (int i=0;i<count;++i) {
      peers.add(new Pair<String, Integer>("localhost", 57890+i));      
    }
    for (int i=0;i<count;++i) {
      managers.add(new GossipedMasterAckManager(peers, 57890+i));
      managers.get(i).start();
    }
    
    Random random = new Random();
    int msgcount = 50;
    for (int i=0;i<msgcount;++i) {
      managers.get(random.nextInt(count)).acknowledge("hello world " + i);
    }
    Clock.sleep(30000);    
    
    for (GossipedMasterAckManager a : managers) {
      LOG.info(" got " + a.getPending().size() + " acks");
      assertEquals(a.getPending().size(), msgcount);
      a.stop();
    }      
  }
  
  /**
   * Test that start and stop repeatedly works ok
   */
  @Test
  public void testGAMStartStop() throws InterruptedException, IOException {
      List<Pair<String,Integer>> peers = new ArrayList<Pair<String,Integer>>();    
      int count = 10; 
      for (int i=0;i<count;++i) {
        peers.add(new Pair<String, Integer>("localhost", 57890+i));      
      }
      GossipedMasterAckManager ackman = new GossipedMasterAckManager(peers, 57890);
      for (int i=0;i<5;++i) {
        LOG.info("Start " + i);
        ackman.start();                
        LOG.info("Stop");
        ackman.stop();
        LOG.info("Stopped");
      }       
    }
}
