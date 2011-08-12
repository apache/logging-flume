package org.apache.flume;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.Context;
import org.apache.flume.LogicalNode;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLogicalNode {

  private static final Logger logger = LoggerFactory
      .getLogger(TestLogicalNode.class);

  private LogicalNode node;

  @Before
  public void setUp() {
    node = new LogicalNode();

    node.setName("test-node-n1");
    node.setSource(new SequenceGeneratorSource());
    node.setSink(new NullSink());
  }

  @Test
  public void testLifecycle() throws LifecycleException, InterruptedException {
    Context context = new Context();

    node.start(context);
    boolean reached = LifecycleController.waitForOneOf(node,
        new LifecycleState[] { LifecycleState.START, LifecycleState.ERROR },
        5000);

    Assert.assertTrue("Matched a lifecycle state", reached);
    Assert.assertEquals(LifecycleState.START, node.getLifecycleState());

    node.stop(context);
    reached = LifecycleController.waitForOneOf(node, new LifecycleState[] {
        LifecycleState.STOP, LifecycleState.ERROR }, 5000);

    Assert.assertTrue("Matched a lifecycle state", reached);
    Assert.assertEquals(LifecycleState.STOP, node.getLifecycleState());
  }

  @Test
  public void testMultipleNodes() throws LifecycleException,
      InterruptedException {

    final AtomicInteger successfulThread = new AtomicInteger(0);
    final CountDownLatch finishedThreads = new CountDownLatch(10);

    for (int i = 0; i < 10; i++) {
      final int j = i;

      new Thread("test-node-runner-" + i) {

        @Override
        public void run() {
          Context context = new Context();
          LogicalNode node = new LogicalNode();

          node.setName("test-node-" + j);
          node.setSource(new SequenceGeneratorSource());
          node.setSink(new NullSink());

          try {
            node.start(context);

            boolean reached = LifecycleController.waitForOneOf(node,
                new LifecycleState[] { LifecycleState.START,
                    LifecycleState.ERROR }, 5000);

            Assert.assertTrue("Matched a lifecycle state", reached);
            Assert.assertEquals(LifecycleState.START, node.getLifecycleState());

            Thread.sleep(500);

            node.stop(context);
            reached = LifecycleController.waitForOneOf(node,
                new LifecycleState[] { LifecycleState.STOP,
                    LifecycleState.ERROR }, 5000);

            Assert.assertTrue("Matched a lifecycle state", reached);
            Assert.assertEquals(LifecycleState.STOP, node.getLifecycleState());

            successfulThread.incrementAndGet();
          } catch (LifecycleException e) {
            logger.debug("Exception follows", e);
          } catch (InterruptedException e) {
            logger.debug("Exception follows", e);
          }

          finishedThreads.countDown();
        }
      }.start();

    }

    finishedThreads.await();

    Assert.assertEquals(10, successfulThread.get());
  }
}
