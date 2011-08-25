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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.google.common.collect.Lists;

/**
 * Test the spawn and decommission calls of a logical node manager.
 */
public class TestLogicalNodeManager {

  @Test
  public void testSpawnDecomission() throws IOException, FlumeSpecException,
      InterruptedException {
    LogicalNodeManager lnm = new LogicalNodeManager("local");
    lnm.spawn(LogicalNodeContext.testingContext(), "foo1", null);
    lnm.spawn(LogicalNodeContext.testingContext(),"foo2", null);
    lnm.spawn(LogicalNodeContext.testingContext(),"foo3", null);

    assertEquals(3, lnm.getNodes().size());

    lnm.decommission("foo1");
    lnm.decommission("foo2");
    lnm.decommission("foo3");
    assertEquals(0, lnm.getNodes().size());
  }

  @Test(expected = IOException.class)
  public void tesIllegalDecommission() throws IOException, InterruptedException {
    LogicalNodeManager lnm = new LogicalNodeManager("local");
    lnm.decommission("nonexist");
  }

  @Test
  public void testSpawnDecomissionAllBut() throws IOException,
      FlumeSpecException, InterruptedException {
    LogicalNodeManager lnm = new LogicalNodeManager("local");
    lnm.spawn(LogicalNodeContext.testingContext(),"foo1", null);
    lnm.spawn(LogicalNodeContext.testingContext(),"foo2", null);
    lnm.spawn(LogicalNodeContext.testingContext(),"foo3", null);

    assertEquals(3, lnm.getNodes().size());

    List<String> nodes = Lists.newArrayList("foo1", "foo2", "foo3");
    lnm.decommissionAllBut(nodes);
    assertEquals(3, lnm.getNodes().size());

    nodes.remove(0);
    lnm.decommissionAllBut(nodes);
    assertEquals(2, lnm.getNodes().size());

    nodes.remove(0);
    lnm.decommissionAllBut(nodes);
    assertEquals(1, lnm.getNodes().size());

    nodes.remove(0);
    lnm.decommissionAllBut(nodes);
    assertEquals(0, lnm.getNodes().size());
  }

}
