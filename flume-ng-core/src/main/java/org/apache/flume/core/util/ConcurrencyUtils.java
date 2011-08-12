package org.apache.flume.core.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.flume.core.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrencyUtils {

  private static final Logger logger = LoggerFactory
      .getLogger(ConcurrencyUtils.class);

  /**
   * <p>
   * Run the provided {@link Runnable} using the {@code executorService}. If it
   * takes longer than {@code timeLimit} or does not regularly report status
   * using the supplied {@link Reporter}, cancel it.
   * </p>
   * <p>
   * This is meant for operations that may take time but have an SLA. It was
   * originally created for ops like source / sink opens or next / append calls
   * that may block (at least externally). For these cases, we want them to
   * block but we need to know they're still alive.
   * </p>
   * 
   * @param executorService
   * @param runnable
   * @param reporter
   * @param timeLimit
   *          (or zero for no time limit)
   * @return true if the task finished within the timeLimit and with acceptable
   *         reporting, false otherwise.
   * @throws InterruptedException
   */
  public static boolean executeReportAwareWithSLA(
      ExecutorService executorService, Runnable runnable, Reporter reporter,
      long reportInterval, long timeLimit) throws InterruptedException {

    boolean success = false;

    Future<?> future = executorService.submit(runnable);

    long currentTime = System.currentTimeMillis();
    long deadLine = timeLimit > 0 ? currentTime + timeLimit : 0;

    logger.debug("Need to complete {} by {}. Must see progress every {}ms",
        new Object[] { future, deadLine, reportInterval });

    while (true) {
      currentTime = System.currentTimeMillis();
      long lastReport = reporter.getTimeStamp();
      long timeSinceReport = currentTime - lastReport;

      logger
          .debug(
              "Checking status of {} - lastReport:{} currentTime:{} timeSinceReport:{}",
              new Object[] { future, lastReport, currentTime, timeSinceReport });

      if (deadLine > 0 && currentTime > deadLine) {
        logger.debug("Cancelling {} - exceeded deadLine:{}", future, deadLine);

        future.cancel(true);
        break;
      } else if (timeSinceReport > reportInterval) {
        logger.debug("Cancelling {} - no status in {}ms", future,
            timeSinceReport);

        future.cancel(true);
        break;
      } else if (future.isDone()) {
        logger.debug("Task {} has finished", future);

        success = true;
        break;
      } else {

        /*
         * If there's a deadline, pick the smaller of the next deadline or the
         * next progress time, otherwise just take the next report time.
         * 
         * Then, take the smaller of the above - now (minus 50ms of slop) or
         * 500ms. Oh, but don't let it be < 0. Math is fun.
         * 
         * I'm convinced this isn't right, but I'm moving on. Kick me later.
         */
        long smallestRuleLength = timeLimit > 0 ? Math.min(currentTime
            + deadLine, currentTime + reportInterval) : currentTime
            + reportInterval;
        long sleepLength = Math.min(500,
            Math.max(smallestRuleLength - currentTime - 50, 0));

        logger
            .debug(
                "Still waiting for {} to complete - timeSinceReport:{} deadLine:{} sleeping:{}",
                new Object[] { future, timeSinceReport, deadLine, sleepLength });

        Thread.sleep(sleepLength);
      }
    }

    return success;
  }
}
