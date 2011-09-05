package org.apache.flume.source;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.PollableSource;
import org.apache.flume.SourceRunner;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PollableSourceRunner implements SourceRunner {

  private static final Logger logger = LoggerFactory
      .getLogger(PollableSourceRunner.class);

  private AtomicBoolean shouldStop;
  private PollableSource source;

  private CounterGroup counterGroup;
  private PollingRunner runner;
  private Thread runnerThread;
  private LifecycleState lifecycleState;

  public PollableSourceRunner() {
    shouldStop = new AtomicBoolean();
    counterGroup = new CounterGroup();
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start(Context context) {

    source.start(context);

    runner = new PollingRunner();

    runner.source = source;
    runner.counterGroup = counterGroup;
    runner.shouldStop = shouldStop;

    runnerThread = new Thread(runner);
    runnerThread.start();

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop(Context context) {

    runner.shouldStop.set(true);

    try {
      runnerThread.interrupt();
      runnerThread.join();
    } catch (InterruptedException e) {
      logger
          .warn(
              "Interrupted while waiting for polling runner to stop. Please report this.",
              e);
      Thread.currentThread().interrupt();
    }

    source.stop(context);

    lifecycleState = LifecycleState.STOP;
  }

  public PollableSource getSource() {
    return source;
  }

  public void setSource(PollableSource source) {
    this.source = source;
  }

  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public static class PollingRunner implements Runnable {

    private PollableSource source;
    private AtomicBoolean shouldStop;
    private CounterGroup counterGroup;

    @Override
    public void run() {
      logger.debug("Polling runner starting. Source:{}", source);

      while (!shouldStop.get()) {
        try {
          source.process();
          counterGroup.incrementAndGet("events.successful");
        } catch (Exception e) {
          logger.error("Unable to process event. Exception follows.", e);
          counterGroup.incrementAndGet("events.failed");
        }
      }

      logger.debug("Polling runner exiting. Metrics:{}", counterGroup);
    }

  }

}
