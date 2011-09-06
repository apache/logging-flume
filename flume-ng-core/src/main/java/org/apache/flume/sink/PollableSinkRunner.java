package org.apache.flume.sink;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSink;
import org.apache.flume.SinkRunner;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PollableSinkRunner extends SinkRunner {

  private static final Logger logger = LoggerFactory
      .getLogger(PollableSinkRunner.class);

  private PollableSink sink;

  private CounterGroup counterGroup;
  private PollingRunner runner;
  private Thread runnerThread;
  private LifecycleState lifecycleState;

  public PollableSinkRunner() {
    counterGroup = new CounterGroup();
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start() {
    runner = new PollingRunner();

    runner.sink = sink;
    runner.counterGroup = counterGroup;
    runner.shouldStop = new AtomicBoolean();

    runnerThread = new Thread(runner);
    runnerThread.start();

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop() {

    if (runnerThread != null) {
      runner.shouldStop.set(true);
      runnerThread.interrupt();

      while (runnerThread.isAlive()) {
        try {
          logger.debug("Waiting for runner thread to exit");
          runnerThread.join(500);
        } catch (InterruptedException e) {
          logger
              .debug(
                  "Interrupted while waiting for runner thread to exit. Exception follows.",
                  e);
        }
      }
    }

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public PollableSink getSink() {
    return sink;
  }

  public void setSink(PollableSink sink) {
    this.sink = sink;
  }

  public static class PollingRunner implements Runnable {

    private PollableSink sink;
    private AtomicBoolean shouldStop;
    private CounterGroup counterGroup;

    @Override
    public void run() {
      logger.debug("Polling sink runner starting");

      while (!shouldStop.get()) {
        try {
          sink.process();
        } catch (InterruptedException e) {
          logger.debug("Interrupted while processing an event. Exiting.");
          counterGroup.incrementAndGet("runner.interruptions");
        } catch (EventDeliveryException e) {
          logger.error("Unable to deliver event. Exception follows.", e);
          counterGroup.incrementAndGet("runner.errors");
        }
      }

      logger.debug("Polling sink runner exiting");
    }

  }

}
