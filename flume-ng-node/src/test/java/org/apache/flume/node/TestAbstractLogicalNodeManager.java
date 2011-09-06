package org.apache.flume.node;

import junit.framework.Assert;

import org.apache.flume.LogicalNode;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.nodemanager.AbstractLogicalNodeManager;
import org.apache.flume.sink.NullSink;
import org.apache.flume.sink.PollableSinkRunner;
import org.apache.flume.source.PollableSourceRunner;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAbstractLogicalNodeManager {

  private static final Logger logger = LoggerFactory
      .getLogger(TestAbstractLogicalNodeManager.class);

  private AbstractLogicalNodeManager nodeManager;

  @Before
  public void setUp() {
    nodeManager = new AbstractLogicalNodeManager() {

      private LifecycleState lifecycleState = LifecycleState.IDLE;

      @Override
      public void stop() {

        for (LogicalNode node : getNodes()) {
          node.stop();

          boolean reached = false;

          try {
            reached = LifecycleController.waitForOneOf(node,
                LifecycleState.STOP_OR_ERROR);
          } catch (InterruptedException e) {
            // Do nothing.
          }

          if (!reached) {
            logger.error(
                "Unable to stop logical node:{} This *will* cause failures.",
                node);
          }

          if (node.getLifecycleState().equals(LifecycleState.ERROR)) {
            lifecycleState = LifecycleState.ERROR;
          }
        }

        lifecycleState = LifecycleState.STOP;
      }

      @Override
      public void start() {

        for (LogicalNode node : getNodes()) {
          node.start();

          boolean reached = false;

          try {
            reached = LifecycleController.waitForOneOf(node,
                LifecycleState.START_OR_ERROR);
          } catch (InterruptedException e) {
            // Do nothing.
          }

          if (!reached) {
            logger.error(
                "Unable to stop logical node:{} This *will* cause failures.",
                node);
          }

          if (node.getLifecycleState().equals(LifecycleState.ERROR)) {
            lifecycleState = LifecycleState.ERROR;
          }
        }

        lifecycleState = LifecycleState.START;
      }

      @Override
      public LifecycleState getLifecycleState() {
        return lifecycleState;
      }
    };
  }

  @Test
  public void testEmptyLifecycle() throws LifecycleException,
      InterruptedException {

    nodeManager.start();
    boolean reached = LifecycleController.waitForOneOf(nodeManager,
        LifecycleState.START_OR_ERROR);

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.START, nodeManager.getLifecycleState());

    nodeManager.stop();
    reached = LifecycleController.waitForOneOf(nodeManager,
        LifecycleState.STOP_OR_ERROR);

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.STOP, nodeManager.getLifecycleState());
  }

  @Test
  public void testLifecycle() throws LifecycleException, InterruptedException {

    LogicalNode node = new LogicalNode();
    node.setName("test");

    PollableSourceRunner sourceRunner = new PollableSourceRunner();
    sourceRunner.setSource(new SequenceGeneratorSource());

    PollableSinkRunner sinkRunner = new PollableSinkRunner();
    sinkRunner.setSink(new NullSink());

    node.setSourceRunner(sourceRunner);
    node.setSinkRunner(sinkRunner);

    nodeManager.add(node);

    nodeManager.start();
    boolean reached = LifecycleController.waitForOneOf(nodeManager,
        LifecycleState.START_OR_ERROR);

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.START, nodeManager.getLifecycleState());

    nodeManager.stop();
    reached = LifecycleController.waitForOneOf(nodeManager,
        LifecycleState.STOP_OR_ERROR);

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.STOP, nodeManager.getLifecycleState());
  }

  @Test
  public void testRapidLifecycleFlapping() throws LifecycleException,
      InterruptedException {

    LogicalNode node = new LogicalNode();

    SequenceGeneratorSource source = new SequenceGeneratorSource();
    source.setChannel(new MemoryChannel());

    PollableSourceRunner sourceRunner = new PollableSourceRunner();
    sourceRunner.setSource(source);

    PollableSinkRunner sinkRunner = new PollableSinkRunner();
    sinkRunner.setSink(new NullSink());

    node.setName("test");
    node.setSourceRunner(sourceRunner);
    node.setSinkRunner(sinkRunner);

    nodeManager.add(node);

    for (int i = 0; i < 10; i++) {
      nodeManager.start();
      boolean reached = LifecycleController.waitForOneOf(nodeManager,
          LifecycleState.START_OR_ERROR);

      Assert.assertTrue(reached);
      Assert
          .assertEquals(LifecycleState.START, nodeManager.getLifecycleState());

      nodeManager.stop();
      reached = LifecycleController.waitForOneOf(nodeManager,
          LifecycleState.STOP_OR_ERROR);

      Assert.assertTrue(reached);
      Assert.assertEquals(LifecycleState.STOP, nodeManager.getLifecycleState());
    }
  }

}
