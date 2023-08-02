/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.node;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.CounterGroup;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <p>
 * A configuration provider that uses properties for specifying
 * configuration. The configurations follow the Java properties file syntax
 * rules specified at {@link Properties#load(java.io.Reader)}. Every
 * configuration value specified in the properties is prefixed by an
 * <em>Agent Name</em> which helps isolate an individual agent&apos;s namespace.
 * </p>
 * <p>
 * Valid configurations must observe the following rules for every agent
 * namespace.
 * <ul>
 * <li>For every &lt;agent name&gt; there must be three lists specified that
 * include <tt>&lt;agent name&gt;.sources</tt>,
 * <tt>&lt;agent name&gt;.sinks</tt>, and <tt>&lt;agent name&gt;.channels</tt>.
 * Each of these lists must contain a space separated list of names
 * corresponding to that particular entity.</li>
 * <li>For each source named in <tt>&lt;agent name&gt;.sources</tt>, there must
 * be a non-empty <tt>type</tt> attribute specified from the valid set of source
 * types. For example:
 * <tt>&lt;agent name&gt;.sources.&lt;source name&gt;.type = event</tt></li>
 * <li>For each source named in <tt>&lt;agent name&gt;.sources</tt>, there must
 * be a space-separated list of channel names that the source will associate
 * with during runtime. Each of these names must be contained in the channels
 * list specified by <tt>&lt;agent name&gt;.channels</tt>. For example:
 * <tt>&lt;agent name&gt;.sources.&lt;source name&gt;.channels =
 * &lt;channel-1 name&gt; &lt;channel-2 name&gt;</tt></li>
 * <li>For each source named in the <tt>&lt;agent name&gt;.sources</tt>, there
 * must be a <tt>runner</tt> namespace of configuration that configures the
 * associated source runner. For example:
 * <tt>&lt;agent name&gt;.sources.&lt;source name&gt;.runner.type = avro</tt>.
 * This namespace can also be used to configure other configuration of the
 * source runner as needed. For example:
 * <tt>&lt;agent name&gt;.sources.&lt;source name&gt;.runner.port = 10101</tt>
 * </li>
 * <li>For each source named in <tt>&lt;sources&gt;.sources</tt> there can
 * be an optional <tt>selector.type</tt> specified that identifies the type
 * of channel selector associated with the source. If not specified, the
 * default replicating channel selector is used.
 * </li><li>For each channel named in the <tt>&lt;agent name&gt;.channels</tt>,
 * there must be a non-empty <tt>type</tt> attribute specified from the valid
 * set of channel types. For example:
 * <tt>&lt;agent name&gt;.channels.&lt;channel name&gt;.type = mem</tt></li>
 * <li>For each sink named in the <tt>&lt;agent name&gt;.sinks</tt>, there must
 * be a non-empty <tt>type</tt> attribute specified from the valid set of sink
 * types. For example:
 * <tt>&lt;agent name&gt;.sinks.&lt;sink name&gt;.type = hdfs</tt></li>
 * <li>For each sink named in the <tt>&lt;agent name&gt;.sinks</tt>, there must
 * be a non-empty single-valued channel name specified as the value of the
 * <tt>channel</tt> attribute. This value must be contained in the channels list
 * specified by <tt>&lt;agent name&gt;.channels</tt>. For example:
 * <tt>&lt;agent name&gt;.sinks.&lt;sink name&gt;.channel =
 * &lt;channel name&gt;</tt></li>
 * <li>For each sink named in the <tt>&lt;agent name&gt;.sinks</tt>, there must
 * be a <tt>runner</tt> namespace of configuration that configures the
 * associated sink runner. For example:
 * <tt>&lt;agent name&gt;.sinks.&lt;sink name&gt;.runner.type = polling</tt>.
 * This namespace can also be used to configure other configuration of the sink
 * runner as needed. For example:
 * <tt>&lt;agent name&gt;.sinks.&lt;sink name&gt;.runner.polling.interval =
 * 60</tt></li>
 * <li>A fourth optional list <tt>&lt;agent name&gt;.sinkgroups</tt>
 * may be added to each agent, consisting of unique space separated names
 * for groups</li>
 * <li>Each sinkgroup must specify sinks, containing a list of all sinks
 * belonging to it. These cannot be shared by multiple groups.
 * Further, one can set a processor and behavioral parameters to determine
 * how sink selection is made via <tt>&lt;agent name&gt;.sinkgroups.&lt;
 * group name&lt.processor</tt>. For further detail refer to individual processor
 * documentation</li>
 * <li>Sinks not assigned to a group will be assigned to default single sink
 * groups.</li>
 * </ul>
 * <p>
 * Apart from the above required configuration values, each source, sink or
 * channel can have its own set of arbitrary configuration as required by the
 * implementation. Each of these configuration values are expressed by fully
 * namespace qualified configuration keys. For example, the configuration
 * property called <tt>capacity</tt> for a channel called <tt>ch1</tt> for the
 * agent named <tt>host1</tt> with value <tt>1000</tt> will be expressed as:
 * <tt>host1.channels.ch1.capacity = 1000</tt>.
 * </p>
 * <p>
 * Any information contained in the configuration file other than what pertains
 * to the configured agents, sources, sinks and channels via the explicitly
 * enumerated list of sources, sinks and channels per agent name are ignored by
 * this provider. Moreover, if any of the required configuration values are not
 * present in the configuration file for the configured entities, that entity
 * and anything that depends upon it is considered invalid and consequently not
 * configured. For example, if a channel is missing its <tt>type</tt> attribute,
 * it is considered misconfigured. Also, any sources or sinks that depend upon
 * this channel are also considered misconfigured and not initialized.
 * </p>
 * <p>
 * Example configuration file:
 *
 * <pre>
 * #
 * # Flume Configuration
 * # This file contains configuration for one Agent identified as host1.
 * #
 *
 * host1.sources = avroSource thriftSource
 * host1.channels = jdbcChannel
 * host1.sinks = hdfsSink
 *
 * # avroSource configuration
 * host1.sources.avroSource.type = org.apache.flume.source.AvroSource
 * host1.sources.avroSource.runner.type = avro
 * host1.sources.avroSource.runner.port = 11001
 * host1.sources.avroSource.channels = jdbcChannel
 * host1.sources.avroSource.selector.type = replicating
 *
 * # thriftSource configuration
 * host1.sources.thriftSource.type = org.apache.flume.source.ThriftSource
 * host1.sources.thriftSource.runner.type = thrift
 * host1.sources.thriftSource.runner.port = 12001
 * host1.sources.thriftSource.channels = jdbcChannel
 *
 * # jdbcChannel configuration
 * host1.channels.jdbcChannel.type = jdbc
 * host1.channels.jdbcChannel.jdbc.driver = com.mysql.jdbc.Driver
 * host1.channels.jdbcChannel.jdbc.connect.url = http://localhost/flumedb
 * host1.channels.jdbcChannel.jdbc.username = flume
 * host1.channels.jdbcChannel.jdbc.password = flume
 *
 * # hdfsSink configuration
 * host1.sinks.hdfsSink.type = hdfs
 * host1.sinks.hdfsSink.hdfs.path = hdfs://localhost/
 * host1.sinks.hdfsSink.batchsize = 1000
 * host1.sinks.hdfsSink.runner.type = polling
 * host1.sinks.hdfsSink.runner.polling.interval = 60
 * </pre>
 *
 * </p>
 *
 * @see Properties#load(java.io.Reader)
 */
public class UriConfigurationProvider extends AbstractConfigurationProvider
    implements LifecycleAware {

  private static final Logger LOGGER = LoggerFactory.getLogger(UriConfigurationProvider.class);

  private final List<ConfigurationSource> configurationSources;
  private final File backupDirectory;
  private final EventBus eventBus;
  private final int interval;
  private final CounterGroup counterGroup;
  private LifecycleState lifecycleState = LifecycleState.IDLE;
  private ScheduledExecutorService executorService;

  public UriConfigurationProvider(String agentName, List<ConfigurationSource> sourceList,
      String backupDirectory, EventBus eventBus, int pollInterval) {
    super(agentName);
    this.configurationSources = sourceList;
    this.backupDirectory = backupDirectory != null ? new File(backupDirectory) : null;
    this.eventBus = eventBus;
    this.interval = pollInterval;
    counterGroup = new CounterGroup();
  }

  @Override
  public void start() {
    if (eventBus != null && interval > 0) {
      executorService = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setNameFormat("conf-file-poller-%d")
              .build());

      WatcherRunnable watcherRunnable = new WatcherRunnable(configurationSources, counterGroup,
          eventBus);

      executorService.scheduleWithFixedDelay(watcherRunnable, 0, interval,
          TimeUnit.SECONDS);
    }
    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop() {
    if (executorService != null) {
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
    }
    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  protected List<ConfigurationSource> getConfigurationSources() {
    return configurationSources;
  }

  @Override
  public FlumeConfiguration getFlumeConfiguration() {
    Map<String, String> configMap = null;
    Properties properties = new Properties();
    for (ConfigurationSource configurationSource : configurationSources) {
      try (InputStream is = configurationSource.getInputStream()) {
        if (is != null) {
          switch (configurationSource.getExtension()) {
            case ConfigurationSource.JSON: case ConfigurationSource.YAML:
            case ConfigurationSource.XML: {
              LOGGER.warn("File extension type {} is unsupported",
                  configurationSource.getExtension());
              break;
            }
            default: {
              properties.load(is);
              break;
            }
          }
        }
      } catch (IOException ioe) {
        LOGGER.warn("Unable to load properties from {}: {}", configurationSource.getUri(),
            ioe.getMessage());
      }
      if (properties.size() > 0) {
        configMap = MapResolver.resolveProperties(properties);
      }
    }
    if (configMap != null) {
      Properties props = new Properties();
      props.putAll(configMap);
      if (backupDirectory != null) {
        if (backupDirectory.mkdirs()) {
          // This is only being logged to keep Spotbugs happy. We can't ignore the result of mkdirs.
          LOGGER.debug("Created directories for {}", backupDirectory.toString());
        }
        File backupFile = getBackupFile(backupDirectory, getAgentName());
        try (OutputStream os = new FileOutputStream(backupFile)) {
          props.store(os, "Backup created at " + LocalDateTime.now().toString());
        } catch (IOException ioe) {
          LOGGER.warn("Unable to create backup properties file: {}" + ioe.getMessage());
        }
      }
    } else {
      if (backupDirectory != null) {
        File backup = getBackupFile(backupDirectory, getAgentName());
        if (backup.exists()) {
          Properties props = new Properties();
          try (InputStream is = new FileInputStream(backup)) {
            LOGGER.warn("Unable to access primary configuration. Trying backup");
            props.load(is);
            configMap = MapResolver.resolveProperties(props);
          } catch (IOException ex) {
            LOGGER.warn("Error reading backup file: {}", ex.getMessage());
          }
        }
      }
    }
    if (configMap != null) {
      return new FlumeConfiguration(configMap);
    } else {
      LOGGER.error("No configuration could be found");
      return null;
    }
  }

  private File getBackupFile(File backupDirectory, String agentName) {
    if (backupDirectory != null) {
      return new File(backupDirectory, "." + agentName + ".properties");
    }
    return null;
  }

  private class WatcherRunnable implements Runnable {

    private List<ConfigurationSource> configurationSources;
    private final CounterGroup counterGroup;
    private final EventBus eventBus;

    public WatcherRunnable(List<ConfigurationSource> sources, CounterGroup counterGroup,
        EventBus eventBus) {
      this.configurationSources = sources;
      this.counterGroup = counterGroup;
      this.eventBus = eventBus;
    }

    @Override
    public void run() {
      LOGGER.debug("Checking for changes to sources");

      counterGroup.incrementAndGet("uri.checks");
      try {
        boolean isModified = false;
        for (ConfigurationSource source : configurationSources) {
          if (source.isModified()) {
            isModified = true;
          }
        }
        if (isModified) {
          eventBus.post(getConfiguration());
        }
      } catch (ConfigurationException ex) {
        LOGGER.warn("Unable to update configuration: {}", ex.getMessage());
      }
    }
  }
}
