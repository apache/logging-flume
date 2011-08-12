package org.apache.flume.node;

import junit.framework.Assert;

import org.apache.flume.core.Context;
import org.apache.flume.core.LogicalNode;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.nodemanager.AbstractLogicalNodeManager;
import org.apache.flume.sink.NullSink;
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
      public void stop(Context context) throws LifecycleException,
          InterruptedException {

        for (LogicalNode node : getNodes()) {
          node.stop(context);

          boolean reached = LifecycleController
              .waitForOneOf(node, new LifecycleState[] { LifecycleState.STOP,
                  LifecycleState.ERROR });

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
      public void start(Context context) throws LifecycleException,
          InterruptedException {

        for (LogicalNode node : getNodes()) {
          node.start(context);

          boolean reached = LifecycleController
              .waitForOneOf(node, new LifecycleState[] { LifecycleState.START,
                  LifecycleState.ERROR });

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
    Context context = new Context();

    nodeManager.start(context);
    boolean reached = LifecycleController.waitForOneOf(nodeManager,
        new LifecycleState[] { LifecycleState.START, LifecycleState.ERROR });

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.START, nodeManager.getLifecycleState());

    nodeManager.stop(context);
    reached = LifecycleController.waitForOneOf(nodeManager,
        new LifecycleState[] { LifecycleState.STOP, LifecycleState.ERROR });

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.STOP, nodeManager.getLifecycleState());
  }

  @Test
  public void testLifecycle() throws LifecycleException, InterruptedException {

    Context context = new Context();

    LogicalNode node = new LogicalNode();
    node.setName("test");
    node.setSource(new SequenceGeneratorSource());
    node.setSink(new NullSink());

    nodeManager.add(node);

    nodeManager.start(context);
    boolean reached = LifecycleController.waitForOneOf(nodeManager,
        new LifecycleState[] { LifecycleState.START, LifecycleState.ERROR });

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.START, nodeManager.getLifecycleState());

    nodeManager.stop(context);
    reached = LifecycleController.waitForOneOf(nodeManager,
        new LifecycleState[] { LifecycleState.STOP, LifecycleState.ERROR });

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.STOP, nodeManager.getLifecycleState());
  }

  @Test
  public void testRapidLifecycleFlapping() throws LifecycleException,
      InterruptedException {

    LogicalNode node = new LogicalNode();
    node.setName("test");
    node.setSource(new SequenceGeneratorSource());
    node.setSink(new NullSink());

    nodeManager.add(node);

    for (int i = 0; i < 10; i++) {
      Context context = new Context();

      nodeManager.start(context);
      boolean reached = LifecycleController.waitForOneOf(nodeManager,
          new LifecycleState[] { LifecycleState.START, LifecycleState.ERROR });

      Assert.assertTrue(reached);
      Assert
          .assertEquals(LifecycleState.START, nodeManager.getLifecycleState());

      nodeManager.stop(context);
      reached = LifecycleController.waitForOneOf(nodeManager,
          new LifecycleState[] { LifecycleState.STOP, LifecycleState.ERROR });

      Assert.assertTrue(reached);
      Assert.assertEquals(LifecycleState.STOP, nodeManager.getLifecycleState());
    }
  }

}
