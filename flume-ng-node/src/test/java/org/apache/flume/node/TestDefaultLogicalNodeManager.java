package org.apache.flume.node;

import java.util.HashSet;
import java.util.Set;

import org.apache.flume.LogicalNode;
import org.apache.flume.channel.MemoryChannel;
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
      LogicalNode node = new LogicalNode();

      SequenceGeneratorSource source = new SequenceGeneratorSource();
      source.setChannel(new MemoryChannel());

      PollableSourceRunner sourceChannelAdapter = new PollableSourceRunner();
      sourceChannelAdapter.setSource(source);

      node.setName("test-node-" + i);
      node.setSourceRunner(sourceChannelAdapter);

      nodeManager.add(node);
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

    Set<LogicalNode> testNodes = new HashSet<LogicalNode>();

    for (int i = 0; i < 30; i++) {
      LogicalNode node = new LogicalNode();

      SequenceGeneratorSource source = new SequenceGeneratorSource();
      source.setChannel(new MemoryChannel());

      PollableSourceRunner sourceChannelAdapter = new PollableSourceRunner();
      sourceChannelAdapter.setSource(source);

      node.setName("test-node-" + i);
      node.setSourceRunner(sourceChannelAdapter);

      testNodes.add(node);
    }

    nodeManager.start();
    Assert.assertTrue("Node manager didn't reach START or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.START_OR_ERROR, 5000));

    for (LogicalNode node : testNodes) {
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

    Set<LogicalNode> testNodes = new HashSet<LogicalNode>();

    for (int i = 0; i < 30; i++) {
      LogicalNode node = new LogicalNode();

      SequenceGeneratorSource source = new SequenceGeneratorSource();
      source.setChannel(new MemoryChannel());

      PollableSourceRunner sourceChannelAdapter = new PollableSourceRunner();
      sourceChannelAdapter.setSource(source);

      node.setName("test-node-" + i);
      node.setSourceRunner(sourceChannelAdapter);

      testNodes.add(node);
    }

    nodeManager.start();
    Assert.assertTrue("Node manager didn't reach START or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.START_OR_ERROR, 5000));

    for (LogicalNode node : testNodes) {
      nodeManager.add(node);
    }

    Thread.sleep(5000);

    nodeManager.stop();
    Assert.assertTrue("Node manager didn't reach STOP or ERROR",
        LifecycleController.waitForOneOf(nodeManager,
            LifecycleState.STOP_OR_ERROR, 5000));
  }

}
