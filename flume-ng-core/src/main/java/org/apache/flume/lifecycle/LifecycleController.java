package org.apache.flume.lifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LifecycleController {

  private static final Logger logger = LoggerFactory
      .getLogger(LifecycleController.class);
  private static final long shortestSleepDuration = 50;
  private static final int maxNumberOfChecks = 5;

  /*
   * public static boolean waitForState(LifecycleAware delegate, LifecycleState
   * state, long timeout) throws InterruptedException {
   * 
   * logger.debug("Waiting for state {} for delegate:{} up to {}ms", new
   * Object[] { state, delegate, timeout });
   * 
   * long sleepInterval = Math.max(shortestSleepDuration, timeout /
   * maxNumberOfChecks); long deadLine = System.currentTimeMillis() + timeout;
   * 
   * do { if (delegate.getLifecycleState().equals(state)) { return true; }
   * 
   * logger.debug("Still want state:{} sleeping:{}ms", state, sleepInterval);
   * Thread.sleep(sleepInterval); } while (System.currentTimeMillis() <
   * deadLine);
   * 
   * logger.debug("Didn't see state within timeout {}ms", timeout);
   * 
   * return false; }
   */

  public static boolean waitForState(LifecycleAware delegate,
      LifecycleState state, long timeout) throws InterruptedException {

    return waitForOneOf(delegate, new LifecycleState[] { state }, timeout);
  }

  public static boolean waitForOneOf(LifecycleAware delegate,
      LifecycleState[] states, long timeout) throws InterruptedException {

    logger.debug("Waiting for state {} for delegate:{} up to {}ms",
        new Object[] { states, delegate, timeout });

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
    } while (System.currentTimeMillis() < deadLine);

    logger.debug("Didn't see state within timeout {}ms", timeout);

    return false;
  }

}
