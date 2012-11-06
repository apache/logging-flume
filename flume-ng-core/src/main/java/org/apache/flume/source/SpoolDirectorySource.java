/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source;

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.client.avro.SpoolingFileLineReader;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SpoolDirectorySource extends AbstractSource implements
Configurable, EventDrivenSource {

  private static final Logger logger = LoggerFactory
      .getLogger(SpoolDirectorySource.class);

  // Delay used when polling for new files
  private static int POLL_DELAY_MS = 500;

  /* Config options */
  private String completedSuffix;
  private String spoolDirectory;
  private boolean fileHeader;
  private String fileHeaderKey;
  private int batchSize;
  private int bufferMaxLines;
  private int bufferMaxLineLength;

  private ScheduledExecutorService executor;
  private CounterGroup counterGroup;
  private Runnable runner;
  SpoolingFileLineReader reader;

  @Override
  public void start() {
    logger.info("SpoolDirectorySource source starting with directory:{}",
        spoolDirectory);

    executor = Executors.newSingleThreadScheduledExecutor();
    counterGroup = new CounterGroup();

    File directory = new File(spoolDirectory);
    reader = new SpoolingFileLineReader(directory, completedSuffix,
        bufferMaxLines, bufferMaxLineLength);
    runner = new SpoolDirectoryRunnable(reader, counterGroup);

    executor.scheduleWithFixedDelay(
        runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("SpoolDirectorySource source started");
  }

  @Override
  public void stop() {
    super.stop();
  }

  @Override
  public void configure(Context context) {
    spoolDirectory = context.getString(
        SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY);
    Preconditions.checkState(spoolDirectory != null,
        "Configuration must specify a spooling directory");

    completedSuffix = context.getString(
        SpoolDirectorySourceConfigurationConstants.SPOOLED_FILE_SUFFIX,
        SpoolDirectorySourceConfigurationConstants.DEFAULT_SPOOLED_FILE_SUFFIX);
    fileHeader = context.getBoolean(
        SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER,
        SpoolDirectorySourceConfigurationConstants.DEFAULT_FILE_HEADER);
    fileHeaderKey = context.getString(
        SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER_KEY,
        SpoolDirectorySourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY);
    batchSize = context.getInteger(
        SpoolDirectorySourceConfigurationConstants.BATCH_SIZE,
        SpoolDirectorySourceConfigurationConstants.DEFAULT_BATCH_SIZE);
    bufferMaxLines = context.getInteger(
        SpoolDirectorySourceConfigurationConstants.BUFFER_MAX_LINES,
        SpoolDirectorySourceConfigurationConstants.DEFAULT_BUFFER_MAX_LINES);
    bufferMaxLineLength = context.getInteger(
        SpoolDirectorySourceConfigurationConstants.BUFFER_MAX_LINE_LENGTH,
        SpoolDirectorySourceConfigurationConstants.DEFAULT_BUFFER_MAX_LINE_LENGTH);
  }

  private Event createEvent(String lineEntry, String filename) {
    Event out = EventBuilder.withBody(lineEntry.getBytes());
    if (fileHeader) {
      out.getHeaders().put(fileHeaderKey, filename);
    }
    return out;
  }

  private class SpoolDirectoryRunnable implements Runnable {
    private SpoolingFileLineReader reader;
    private CounterGroup counterGroup;

    public SpoolDirectoryRunnable(SpoolingFileLineReader reader,
        CounterGroup counterGroup) {
      this.reader = reader;
      this.counterGroup = counterGroup;
    }
    @Override
    public void run() {
      try {
        while (true) {
          List<String> strings = reader.readLines(batchSize);
          if (strings.size() == 0) { break; }
          String file = reader.getLastFileRead();
          List<Event> events = Lists.newArrayList();
          for (String s: strings) {
            counterGroup.incrementAndGet("spooler.lines.read");
            events.add(createEvent(s, file));
          }
          getChannelProcessor().processEventBatch(events);
          reader.commit();
        }
      }
      catch (Throwable t) {
        logger.error("Uncaught exception in Runnable", t);
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
    }
  }
}
