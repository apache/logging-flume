package org.apache.flume.node.nodemanager;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.LogicalNode;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryBasedLogicalNodeManager extends AbstractLogicalNodeManager {

  private static final Logger logger = LoggerFactory
      .getLogger(MemoryBasedLogicalNodeManager.class);

  private LifecycleState lifecycleState;
  private ExecutorService executorService;

  public MemoryBasedLogicalNodeManager() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public boolean add(final LogicalNode node) {
    if (super.add(node)) {
      final Context context = new Context();

      executorService.submit(new Callable<Exception>() {

        @Override
        public Exception call() throws Exception {
          node.start(context);

          boolean reached = LifecycleController
              .waitForOneOf(node, new LifecycleState[] { LifecycleState.START,
                  LifecycleState.ERROR });

          if (!reached) {
            
          }

          return null;
        }
      });

      return true;
    }

    return false;
  }

  @Override
  public void start(Context context) throws LifecycleException,
      InterruptedException {

    executorService = Executors.newFixedThreadPool(5);

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop(Context context) throws LifecycleException,
      InterruptedException {

    executorService.shutdown();

    while (!executorService.isTerminated()) {
      logger.debug("Waiting for spawn / despawn service to stop");
      try {
        executorService.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger
            .warn("Interrupted while waiting for spawn / despawn service to stop. Interrupting outstanding jobs may cause problems! Report this to the developers!");

        executorService.shutdownNow();
        lifecycleState = LifecycleState.ERROR;
      }
    }

    if (!lifecycleState.equals(LifecycleState.ERROR)) {
      lifecycleState = LifecycleState.STOP;
    }
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

}
