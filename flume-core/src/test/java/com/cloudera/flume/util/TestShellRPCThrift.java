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

package com.cloudera.flume.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.cloudera.flume.conf.thrift.FlumeNodeState;
import com.cloudera.flume.conf.thrift.FlumeMasterCommandThrift;
import com.cloudera.flume.conf.thrift.FlumeNodeStatusThrift;
import com.cloudera.flume.master.Command;
import com.cloudera.flume.master.MasterAdminServerThrift;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.flume.master.StatusManager.NodeStatus;

/**
 * Tests to ensure the RPC types are converted properly for the shell
 * client.
 *
 */
public class TestShellRPCThrift {
  @Test
  public void testThriftStatusConversion() {
    FlumeNodeStatusThrift start = new FlumeNodeStatusThrift();
    start.host = "HOST";
    long time = System.currentTimeMillis();
    start.lastseen = time;
    start.lastSeenDeltaMillis = time;
    start.physicalNode = "PHYSICAL_NODE";
    start.state = FlumeNodeState.ACTIVE;
    
    NodeStatus middle = MasterAdminServerThrift.statusFromThrift(start);
    
    assertEquals("HOST", middle.host);
    assertEquals(start.lastseen, middle.lastseen);
    assertEquals("PHYSICAL_NODE", middle.physicalNode);
    assertEquals(NodeState.ACTIVE, middle.state);
    
    FlumeNodeStatusThrift end = MasterAdminServerThrift.statusToThrift(middle);
    assertEquals(end.host, start.host);
    assertEquals(end.lastseen, start.lastseen);
    assertEquals(end.physicalNode, start.physicalNode);
    assertEquals(end.state, start.state);
  }

  @Test
  public void testThriftCommandConversion() {
    Command start = new Command("here", "is", "a", "command");
    FlumeMasterCommandThrift middle = MasterAdminServerThrift.commandToThrift(start);
    assertEquals("here", middle.command.toString());
    assertEquals(3, middle.arguments.size());
    int index = 0;
    for (String s: middle.arguments) {
      switch(index) {
      case 0: 
        assertEquals("is", s);
        break;
      case 1:
        assertEquals("a", s);
        break;
      case 2:
        assertEquals("command", s);
        break;
      }
      index++;
    }
    Command end = MasterAdminServerThrift.commandFromThrift(middle);
    assertEquals("here", end.getCommand());
    assertEquals(3, end.getArgs().length);
    assertTrue(Arrays.equals(start.getArgs(), end.getArgs()));
  }
  
  @Test
  public void testEmptyArgsCommandThrift() {
    Command start = new Command("ls");
    FlumeMasterCommandThrift middle = MasterAdminServerThrift.commandToThrift(start);
    assertEquals("ls", middle.command.toString());
    assertEquals(0, middle.arguments.size());
    Command end = MasterAdminServerThrift.commandFromThrift(middle);
    assertEquals("ls", end.getCommand());
    assertEquals(0, end.getArgs().length);
  }
}
