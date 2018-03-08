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
package org.apache.flume.node;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.flume.CounterGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class PollingPropertiesFileConfigurationProvider
    extends PropertiesFileConfigurationProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PollingPropertiesFileConfigurationProvider.class);

  private final File file;
  private final int interval;
  private final CounterGroup counterGroup;

  private ScheduledExecutorService executorService;

  public PollingPropertiesFileConfigurationProvider(String agentName,
      File file, int interval) {
    super(agentName, file);
    this.file = file;
    this.interval = interval;
    counterGroup = new CounterGroup();
  }

  @Override
  public void registerConfigurationConsumer(
          Consumer<MaterializedConfiguration> configurationConsumer) {
    setConfigurationConsumer(configurationConsumer);
  }

  @Override
  public void start() {
    LOGGER.info("Configuration provider starting");

    Preconditions.checkState(file != null,
        "The parameter file must not be null");

    executorService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("conf-file-poller-%d")
                .build());

    FileWatcherRunnable fileWatcherRunnable =
        new FileWatcherRunnable(file, counterGroup);

    executorService.scheduleWithFixedDelay(fileWatcherRunnable, 0, interval,
        TimeUnit.SECONDS);

    super.start();

    LOGGER.debug("Configuration provider started");
  }

  @Override
  public void stop() {
    LOGGER.info("Configuration provider stopping");

    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
        LOGGER.debug("File watcher has not terminated. Forcing shutdown of executor.");
        executorService.shutdownNow();
        while (!executorService.awaitTermination(500, TimeUnit.MILLISECONDS)) {
          LOGGER.debug("Waiting for file watcher to terminate");
        }
      }
    } catch (InterruptedException e) {
      LOGGER.debug("Interrupted while waiting for file watcher to terminate");
      Thread.currentThread().interrupt();
    }
    super.stop();
    LOGGER.debug("Configuration provider stopped");
  }

  @Override
  public String toString() {
    return "{ file:" + file + " counterGroup:" + counterGroup + "  provider:"
        + getClass().getCanonicalName() + " agentName:" + getAgentName() + " }";
  }

  public class FileWatcherRunnable implements Runnable {

    private final File file;
    private final CounterGroup counterGroup;

    private long lastChange;

    public FileWatcherRunnable(File file, CounterGroup counterGroup) {
      super();
      this.file = file;
      this.counterGroup = counterGroup;
      this.lastChange = 0L;
    }

    @Override
    public void run() {
      LOGGER.debug("Checking file:{} for changes", file);

      counterGroup.incrementAndGet("file.checks");

      long lastModified = file.lastModified();

      if (lastModified > lastChange) {
        LOGGER.info("Reloading configuration file:{}", file);

        counterGroup.incrementAndGet("file.loads");

        lastChange = lastModified;

        try {
          notifyConfigurationConsumer(getConfiguration());
        } catch (Exception e) {
          LOGGER.error("Failed to load configuration data. Exception follows.",
              e);
        } catch (NoClassDefFoundError e) {
          LOGGER.error("Failed to start agent because dependencies were not " +
              "found in classpath. Error follows.", e);
        } catch (Throwable t) {
          // caught because the caller does not handle or log Throwables
          LOGGER.error("Unhandled error", t);
        }
      }
    }
  }

}
