package org.apache.flume.lifecycle;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LifecycleController {

  private static final Logger logger = LoggerFactory
      .getLogger(LifecycleController.class);
  private static final long shortestSleepDuration = 50;
  private static final int maxNumberOfChecks = 5;

  public static boolean waitForState(LifecycleAware delegate,
      LifecycleState state) throws InterruptedException {

    return waitForState(delegate, state, 0);
  }

  public static boolean waitForState(LifecycleAware delegate,
      LifecycleState state, long timeout) throws InterruptedException {

    return waitForOneOf(delegate, new LifecycleState[] { state }, timeout);
  }

  public static boolean waitForOneOf(LifecycleAware delegate,
      LifecycleState[] states) throws InterruptedException {

    return waitForOneOf(delegate, states, 0);
  }

  public static boolean waitForOneOf(LifecycleAware delegate,
      LifecycleState[] states, long timeout) throws InterruptedException {

    if (logger.isDebugEnabled()) {
      logger.debug("Waiting for state {} for delegate:{} up to {}ms",
          new Object[] { states, delegate, timeout });
    }

    long sleepInterval = Math.max(shortestSleepDuration, timeout
        / maxNumberOfChecks);
    long deadLine = System.currentTimeMillis() + timeout;

    do {
      for (LifecycleState state : states) {
        if (delegate.getLifecycleState().equals(state)) {
          return true;
        }
      }

      logger.debug("Still want one of states:{} sleeping:{}ms", states,
          sleepInterval);
      Thread.sleep(sleepInterval);
    } while (timeout == 0 || System.currentTimeMillis() < deadLine);

    logger.debug("Didn't see state within timeout {}ms", timeout);

    return false;
  }

  public static void stopAll(List<LifecycleAware> services)
      throws InterruptedException {

    for (LifecycleAware service : services) {
      waitForOneOf(service, LifecycleState.STOP_OR_ERROR);
    }
  }

}
