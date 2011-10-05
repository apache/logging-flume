package org.apache.flume.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ExecSource extends AbstractSource implements EventDrivenSource,
    Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(ExecSource.class);

  private String command;

  private CounterGroup counterGroup;
  private ExecutorService executor;
  private Future<?> runnerFuture;

  @Override
  public void start() {
    logger.info("Exec source starting with command:{}", command);

    executor = Executors.newSingleThreadExecutor();
    counterGroup = new CounterGroup();

    ExecRunnable runner = new ExecRunnable();

    runner.command = command;
    runner.channel = getChannel();
    runner.counterGroup = counterGroup;

    // FIXME: Use a callback-like executor / future to signal us upon failure.
    runnerFuture = executor.submit(runner);

    /*
     * NB: This comes at the end rather than the beginning of the method because
     * it sets our state to running. We want to make sure the executor is alive
     * and well first.
     */
    super.start();

    logger.debug("Exec source started");
  }

  @Override
  public void stop() {
    logger.info("Stopping exec source with command:{}", command);

    if (runnerFuture != null) {
      logger.debug("Stopping exec runner");
      runnerFuture.cancel(true);
      logger.debug("Exec runner stopped");
    }

    executor.shutdown();

    while (!executor.isTerminated()) {
      logger.debug("Waiting for exec executor service to stop");
      try {
        executor.awaitTermination(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger
            .debug("Interrupted while waiting for exec executor service to stop. Just exiting.");
        Thread.currentThread().interrupt();
      }
    }

    super.stop();

    logger.debug("Exec source with command:{} stopped. Metrics:{}", command,
        counterGroup);
  }

  @Override
  public void configure(Context context) {
    command = context.get("command", String.class);

    Preconditions.checkState(command != null,
        "The parameter command must be specified");
  }

  private static class ExecRunnable implements Runnable {

    private String command;
    private Channel channel;
    private CounterGroup counterGroup;

    @Override
    public void run() {
      try {
        String[] commandArgs = command.split("\\s+");
        Process process = new ProcessBuilder(commandArgs).start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(
            process.getInputStream()));

        String line = null;

        while ((line = reader.readLine()) != null) {
          counterGroup.incrementAndGet("exec.lines.read");

          Transaction transaction = channel.getTransaction();

          transaction.begin();
          Event event = EventBuilder.withBody(line.getBytes());
          channel.put(event);
          transaction.commit();
        }

        reader.close();
      } catch (IOException e) {
        logger.error("Failed while running command:{} - Exception follows.",
            command, e);
      }
    }

  }

}
