/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Source;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A {@link Source} implementation that executes a Unix process and turns each
 * line of text into an event.
 * </p>
 * <p>
 * This source runs a given Unix command on start up and expects that process to
 * continuously produce data on standard out (stderr ignored by default). Unless
 * told to restart, if the process exits for any reason, the source also exits and
 * will produce no further data. This means configurations such as <tt>cat [named pipe]</tt>
 * or <tt>tail -F [file]</tt> are going to produce the desired results where as
 * <tt>date</tt> will probably not - the former two commands produce streams of
 * data where as the latter produces a single event and exits.
 * </p>
 * <p>
 * The <tt>ExecSource</tt> is meant for situations where one must integrate with
 * existing systems without modifying code. It is a compatibility gateway built
 * to allow simple, stop-gap integration and doesn't necessarily offer all of
 * the benefits or guarantees of native integration with Flume. If one has the
 * option of using the <tt>AvroSource</tt>, for instance, that would be greatly
 * preferred to this source as it (and similarly implemented sources) can
 * maintain the transactional guarantees that exec can not.
 * </p>
 * <p>
 * <i>Why doesn't <tt>ExecSource</tt> offer transactional guarantees?</i>
 * </p>
 * <p>
 * The problem with <tt>ExecSource</tt> and other asynchronous sources is that
 * the source can not guarantee that if there is a failure to put the event into
 * the {@link Channel} the client knows about it. As a for instance, one of the
 * most commonly requested features is the <tt>tail -F [file]</tt>-like use case
 * where an application writes to a log file on disk and Flume tails the file,
 * sending each line as an event. While this is possible, there's an obvious
 * problem; what happens if the channel fills up and Flume can't send an event?
 * Flume has no way of indicating to the application writing the log file that
 * it needs to retain the log or that the event hasn't been sent, for some
 * reason. If this doesn't make sense, you need only know this: <b>Your
 * application can never guarantee data has been received when using a
 * unidirectional asynchronous interface such as ExecSource!</b> As an extension
 * of this warning - and to be completely clear - there is absolutely zero
 * guarantee of event delivery when using this source. You have been warned.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>command</tt></td>
 * <td>The command to execute</td>
 * <td>String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>restart</tt></td>
 * <td>Whether to restart the command when it exits</td>
 * <td>Boolean</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td><tt>restartThrottle</tt></td>
 * <td>How long in milliseconds to wait before restarting the command</td>
 * <td>Long</td>
 * <td>10000</td>
 * </tr>
 * <tr>
 * <td><tt>logStderr</tt></td>
 * <td>Whether to log or discard the standard error stream of the command</td>
 * <td>Boolean</td>
 * <td>false</td>
 * </tr>
 * <tr>
 * <td><tt>batchSize</tt></td>
 * <td>The number of events to commit to channel at a time.</td>
 * <td>integer</td>
 * <td>20</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class ExecSource extends AbstractSource implements EventDrivenSource,
Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(ExecSource.class);


  private String command;
  private CounterGroup counterGroup;
  private ExecutorService executor;
  private Future<?> runnerFuture;
  private long restartThrottle;
  private boolean restart;
  private boolean logStderr;
  private Integer bufferCount;
  private ExecRunnable runner;

  @Override
  public void start() {
    logger.info("Exec source starting with command:{}", command);

    executor = Executors.newSingleThreadExecutor();
    counterGroup = new CounterGroup();

    runner = new ExecRunnable(command, getChannelProcessor(), counterGroup,
        restart, restartThrottle, logStderr, bufferCount);

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

    if(runner != null) {
      runner.setRestart(false);
    }
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
        logger.debug("Interrupted while waiting for exec executor service "
            + "to stop. Just exiting.");
        Thread.currentThread().interrupt();
      }
    }

    super.stop();

    logger.debug("Exec source with command:{} stopped. Metrics:{}", command,
        counterGroup);
  }

  @Override
  public void configure(Context context) {
    command = context.getString("command");

    Preconditions.checkState(command != null,
        "The parameter command must be specified");

    restartThrottle = context.getLong(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE,
        ExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);

    restart = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_RESTART,
        ExecSourceConfigurationConstants.DEFAULT_RESTART);

    logStderr = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_LOG_STDERR,
        ExecSourceConfigurationConstants.DEFAULT_LOG_STDERR);

    bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE,
        ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);
  }

  private static class ExecRunnable implements Runnable {

    public ExecRunnable(String command, ChannelProcessor channelProcessor,
        CounterGroup counterGroup, boolean restart, long restartThrottle,
        boolean logStderr, int bufferCount) {
      this.command = command;
      this.channelProcessor = channelProcessor;
      this.counterGroup = counterGroup;
      this.restartThrottle = restartThrottle;
      this.bufferCount = bufferCount;
      this.restart = restart;
      this.logStderr = logStderr;
    }

    private String command;
    private ChannelProcessor channelProcessor;
    private CounterGroup counterGroup;
    private volatile boolean restart;
    private long restartThrottle;
    private int bufferCount;
    private boolean logStderr;

    @Override
    public void run() {
      do {
        String exitCode = "unknown";
        BufferedReader reader = null;
        Process process = null;
        try {
          String[] commandArgs = command.split("\\s+");
          process = new ProcessBuilder(commandArgs).start();
          reader = new BufferedReader(
              new InputStreamReader(process.getInputStream()));

          // StderrLogger dies as soon as the input stream is invalid
          StderrReader stderrReader = new StderrReader(new BufferedReader(
              new InputStreamReader(process.getErrorStream())), logStderr);
          stderrReader.setName("StderrReader-[" + command + "]");
          stderrReader.setDaemon(true);
          stderrReader.start();

          String line = null;
          List<Event> eventList = new ArrayList<Event>();
          while ((line = reader.readLine()) != null) {
            counterGroup.incrementAndGet("exec.lines.read");
            eventList.add(EventBuilder.withBody(line.getBytes()));
            if(eventList.size() >= bufferCount) {
              channelProcessor.processEventBatch(eventList);
              eventList.clear();
            }
          }
          if(!eventList.isEmpty()) {
            channelProcessor.processEventBatch(eventList);
          }
        } catch (Exception e) {
          logger.error("Failed while running command: " + command, e);
          if(e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
        } finally {
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException ex) {
              logger.error("Failed to close reader for exec source", ex);
            }
          }
          if(process != null) {
            process.destroy();
            try {
              exitCode = String.valueOf(process.waitFor());
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            }
          }
        }
        if(restart) {
          logger.info("Restarting in {}ms, exit code {}", restartThrottle, exitCode);
          try {
            Thread.sleep(restartThrottle);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      } while(restart);
    }
    public void setRestart(boolean restart) {
      this.restart = restart;
    }
  }
  private static class StderrReader extends Thread {
    private BufferedReader input;
    private boolean logStderr;
    protected StderrReader(BufferedReader input, boolean logStderr) {
      this.input = input;
      this.logStderr = logStderr;
    }
    @Override
    public void run() {
      try {
        int i = 0;
        String line = null;
        while((line = input.readLine()) != null) {
          if(logStderr) {
            logger.info("StderrLogger[{}] = '{}'", ++i, line);
          }
        }
      } catch (IOException e) {
        logger.info("StderrLogger exiting", e);
      } finally {
        try {
          if(input != null) {
            input.close();
          }
        } catch (IOException ex) {
          logger.error("Failed to close stderr reader for exec source", ex);
        }
      }
    }

  }
}
