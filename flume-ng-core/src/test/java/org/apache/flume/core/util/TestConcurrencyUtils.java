package org.apache.flume.core.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flume.core.Reporter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestConcurrencyUtils {

  private static final Logger logger = LoggerFactory
      .getLogger(TestConcurrencyUtils.class);

  private ExecutorService executorService;

  @Before
  public void setUp() {
    executorService = Executors.newSingleThreadExecutor();
  }

  @After
  public void tearDown() {
    executorService.shutdownNow();
  }

  /**
   * Run a normal task that reports progress AND finishes within the deadline.
   */
  @Test
  public void testNormalRun() throws InterruptedException {
    final Reporter reporter = new Reporter();

    Runnable r = new Runnable() {

      @Override
      public void run() {
        try {
          for (int i = 0; i < 3; i++) {
            reporter.progress();
            Thread.sleep(100);
          }
        } catch (InterruptedException e) {
          logger.error("Unexcepted interruption of thread.", e);
        }
      }
    };

    boolean finished = ConcurrencyUtils.executeReportAwareWithSLA(
        executorService, r, reporter, 200, 3000);

    Assert.assertTrue(finished);
  }

  /**
   * Run a task that fails to report status but completes within the deadline.
   */
  @Test
  public void testNoProgress() throws InterruptedException {
    final Reporter reporter = new Reporter();

    Runnable r = new Runnable() {

      @Override
      public void run() {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          logger.debug("Someone interrupted us (expected!)");
        }
      }
    };

    boolean finished = ConcurrencyUtils.executeReportAwareWithSLA(
        executorService, r, reporter, 200, 3000);

    Assert.assertFalse(finished);
  }

  /**
   * Run a task that reports status but exceeds the deadline.
   */
  @Test
  public void testDeadline() throws InterruptedException {
    final Reporter reporter = new Reporter();

    Runnable r = new Runnable() {

      @Override
      public void run() {
        try {
          for (int i = 0; i < 35; i++) {
            Thread.sleep(100);
            reporter.progress();
          }
        } catch (InterruptedException e) {
          logger.debug("Someone interrupted us (expected!)");
        }
      }
    };

    boolean finished = ConcurrencyUtils.executeReportAwareWithSLA(
        executorService, r, reporter, 200, 3000);

    Assert.assertFalse(finished);
  }

}
