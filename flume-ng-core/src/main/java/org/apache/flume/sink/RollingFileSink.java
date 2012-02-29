/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.EventFormatter;
import org.apache.flume.formatter.output.PathManager;
import org.apache.flume.formatter.output.TextDelimitedOutputFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class RollingFileSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(RollingFileSink.class);
  private static final long defaultRollInterval = 30;

  private File directory;
  private long rollInterval;
  private OutputStream outputStream;
  private ScheduledExecutorService rollService;

  private CounterGroup counterGroup;

  private PathManager pathController;
  private EventFormatter formatter;
  private volatile boolean shouldRotate;

  public RollingFileSink() {
    formatter = new TextDelimitedOutputFormatter();
    counterGroup = new CounterGroup();
    pathController = new PathManager();
    shouldRotate = false;
  }

  @Override
  public void configure(Context context) {
    String directory = context.getString("sink.directory");
    String rollInterval = context.getString("sink.rollInterval");

    Preconditions.checkArgument(directory != null, "Directory may not be null");

    if (rollInterval == null) {
      this.rollInterval = defaultRollInterval;
    } else {
      this.rollInterval = Long.parseLong(rollInterval);
    }

    this.directory = new File(directory);
  }

  @Override
  public void start() {

    super.start();

    pathController.setBaseDirectory(directory);

    rollService = Executors.newScheduledThreadPool(
        1,
        new ThreadFactoryBuilder().setNameFormat(
            "rollingFileSink-roller-" + Thread.currentThread().getId() + "-%d")
            .build());

    /*
     * Every N seconds, mark that it's time to rotate. We purposefully do NOT
     * touch anything other than the indicator flag to avoid error handling
     * issues (e.g. IO exceptions occuring in two different threads. Resist the
     * urge to actually perform rotation in a separate thread!
     */
    rollService.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        logger.debug("Marking time to rotate file {}",
            pathController.getCurrentFile());
        shouldRotate = true;
      }

    }, rollInterval, rollInterval, TimeUnit.SECONDS);
  }

  @Override
  public Status process() throws EventDeliveryException {
    if (shouldRotate) {
      logger.debug("Time to rotate {}", pathController.getCurrentFile());

      if (outputStream != null) {
        logger.debug("Closing file {}", pathController.getCurrentFile());

        try {
          outputStream.flush();
          outputStream.close();
          shouldRotate = false;
        } catch (IOException e) {
          throw new EventDeliveryException("Unable to rotate file "
              + pathController.getCurrentFile() + " while delivering event", e);
        }

        outputStream = null;
        pathController.rotate();
      }
    }

    if (outputStream == null) {
      try {
        logger.debug("Opening output stream for file {}",
            pathController.getCurrentFile());

        outputStream = new BufferedOutputStream(new FileOutputStream(
            pathController.getCurrentFile()));
      } catch (IOException e) {
        throw new EventDeliveryException("Failed to open file "
            + pathController.getCurrentFile() + " while delivering event", e);
      }
    }

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;
    Status result = Status.READY;

    try {
      transaction.begin();
      event = channel.take();

      if (event != null) {
        byte[] bytes = formatter.format(event);

        outputStream.write(bytes);

        /*
         * FIXME: Feature: Rotate on size and time by checking bytes written and
         * setting shouldRotate = true if we're past a threshold.
         */
        counterGroup.addAndGet("sink.bytesWritten", (long) bytes.length);

        /*
         * FIXME: Feature: Control flush interval based on time or number of
         * events. For now, we're super-conservative and flush on each write.
         */
        outputStream.flush();
      } else {
        // No events found, request back-off semantics from runner
        result = Status.BACKOFF;
      }
      transaction.commit();
    } catch (Exception ex) {
      transaction.rollback();
      throw new EventDeliveryException("Failed to process event: " + event, ex);
    } finally {
      transaction.close();
    }

    return result;
  }

  @Override
  public void stop() {

    super.stop();

    if (outputStream != null) {
      logger.debug("Closing file {}", pathController.getCurrentFile());

      try {
        outputStream.flush();
        outputStream.close();
      } catch (IOException e) {
        logger.error("Unable to close output stream. Exception follows.", e);
      }
    }

    rollService.shutdown();

    while (!rollService.isTerminated()) {
      try {
        rollService.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger
            .debug(
                "Interrupted while waiting for roll service to stop. Please report this.",
                e);
      }
    }
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }

  public long getRollInterval() {
    return rollInterval;
  }

  public void setRollInterval(long rollInterval) {
    this.rollInterval = rollInterval;
  }

  public EventFormatter getFormatter() {
    return formatter;
  }

  public void setFormatter(EventFormatter formatter) {
    this.formatter = formatter;
  }

}
