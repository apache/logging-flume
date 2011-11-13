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

package org.apache.flume.node;

import org.apache.flume.SourceRunner;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.nodemanager.AbstractLogicalNodeManager;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestFlumeNode {

  private FlumeNode node;

  @Before
  public void setUp() {
    node = new FlumeNode();

    node.setName("test-node");
    node.setNodeManager(new EmptyLogicalNodeManager());
  }

  @Ignore("Fails given recent changes to configuration system")
  @Test
  public void testLifecycle() throws InterruptedException, LifecycleException {
    node.start();
    boolean reached = LifecycleController.waitForOneOf(node,
        LifecycleState.START_OR_ERROR, 5000);

    Assert.assertTrue("Matched a known state", reached);
    Assert.assertEquals(LifecycleState.START, node.getLifecycleState());

    node.stop();
    reached = LifecycleController.waitForOneOf(node,
        LifecycleState.STOP_OR_ERROR, 5000);

    Assert.assertTrue("Matched a known state", reached);
    Assert.assertEquals(LifecycleState.STOP, node.getLifecycleState());
  }

  @Ignore("Fails given recent changes to configuration system")
  @Test
  public void testAddNodes() throws InterruptedException, LifecycleException {
    node.start();
    boolean reached = LifecycleController.waitForOneOf(node,
        LifecycleState.START_OR_ERROR, 5000);

    Assert.assertTrue("Matched a known state", reached);
    Assert.assertEquals(LifecycleState.START, node.getLifecycleState());

    SourceRunner n1 = SourceRunner.forSource(new SequenceGeneratorSource());

    node.getNodeManager().add(n1);

    node.stop();
    reached = LifecycleController.waitForOneOf(node,
        LifecycleState.STOP_OR_ERROR, 5000);

    Assert.assertTrue("Matched a known state", reached);
    Assert.assertEquals(LifecycleState.STOP, node.getLifecycleState());
  }

  public static class EmptyLogicalNodeManager extends
      AbstractLogicalNodeManager {

    private LifecycleState lifecycleState;

    public EmptyLogicalNodeManager() {
      lifecycleState = LifecycleState.IDLE;
    }

    @Override
    public void start() {
      lifecycleState = LifecycleState.START;
    }

    @Override
    public void stop() {
      lifecycleState = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getLifecycleState() {
      return lifecycleState;
    }

  }

}
