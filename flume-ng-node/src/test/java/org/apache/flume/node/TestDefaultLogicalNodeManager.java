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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.nodemanager.DefaultLogicalNodeManager;
import org.apache.flume.source.PollableSourceRunner;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultLogicalNodeManager {

  private NodeManager nodeManager;

  @Before
  public void setUp() {
    nodeManager = new DefaultLogicalNodeManager();
  }

  @Test
  public void testLifecycle() throws LifecycleException, InterruptedException {
    nodeManager.start();
    Assert.assertTrue("Node manager didn't reach START or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.START_OR_ERROR, 5000));

    nodeManager.stop();
    Assert.assertTrue("Node manager didn't reach STOP or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.STOP_OR_ERROR, 5000));
  }

  @Test
  public void testLifecycleWithNodes() throws LifecycleException,
      InterruptedException {

    nodeManager.start();
    Assert.assertTrue("Node manager didn't reach START or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.START_OR_ERROR, 5000));

    for (int i = 0; i < 3; i++) {
      SequenceGeneratorSource source = new SequenceGeneratorSource();
      List<Channel> channels = new ArrayList<Channel>();
      channels.add(new MemoryChannel());
      ChannelSelector rcs = new ReplicatingChannelSelector();
      rcs.setChannels(channels);

      source.setChannelProcessor(new ChannelProcessor(rcs));

      PollableSourceRunner sourceRunner = new PollableSourceRunner();
      sourceRunner.setSource(source);

      nodeManager.add(sourceRunner);
    }

    Thread.sleep(5000);

    nodeManager.stop();
    Assert.assertTrue("Node manager didn't reach STOP or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.STOP_OR_ERROR, 5000));
  }

  @Test
  public void testNodeStartStops() throws LifecycleException,
      InterruptedException {

    Set<LifecycleAware> testNodes = new HashSet<LifecycleAware>();

    for (int i = 0; i < 30; i++) {
      SequenceGeneratorSource source = new SequenceGeneratorSource();
      List<Channel> channels = new ArrayList<Channel>();
      channels.add(new MemoryChannel());
      ChannelSelector rcs = new ReplicatingChannelSelector();
      rcs.setChannels(channels);

      source.setChannelProcessor(new ChannelProcessor(rcs));

      PollableSourceRunner sourceRunner = new PollableSourceRunner();
      sourceRunner.setSource(source);

      testNodes.add(sourceRunner);
    }

    nodeManager.start();
    Assert.assertTrue("Node manager didn't reach START or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.START_OR_ERROR, 5000));

    for (LifecycleAware node : testNodes) {
      nodeManager.add(node);
    }

    Thread.sleep(5000);

    nodeManager.stop();
    Assert.assertTrue("Node manager didn't reach STOP or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.STOP_OR_ERROR, 5000));
  }

  @Test
  public void testErrorNode() throws LifecycleException, InterruptedException {

    Set<LifecycleAware> testNodes = new HashSet<LifecycleAware>();

    for (int i = 0; i < 30; i++) {
      SequenceGeneratorSource source = new SequenceGeneratorSource();
      List<Channel> channels = new ArrayList<Channel>();
      channels.add(new MemoryChannel());
      ChannelSelector rcs = new ReplicatingChannelSelector();
      rcs.setChannels(channels);

      source.setChannelProcessor(new ChannelProcessor(rcs));

      PollableSourceRunner sourceRunner = new PollableSourceRunner();
      sourceRunner.setSource(source);

      testNodes.add(sourceRunner);
    }

    nodeManager.start();
    Assert.assertTrue("Node manager didn't reach START or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.START_OR_ERROR, 5000));

    for (LifecycleAware node : testNodes) {
      nodeManager.add(node);
    }

    Thread.sleep(5000);

    nodeManager.stop();
    Assert.assertTrue("Node manager didn't reach STOP or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.STOP_OR_ERROR, 5000));
  }

}
