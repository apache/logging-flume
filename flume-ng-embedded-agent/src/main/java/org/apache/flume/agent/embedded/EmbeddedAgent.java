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
package org.apache.flume.agent.embedded;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceRunner;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.apache.flume.node.MaterializedConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * EmbeddedAgent gives Flume users the ability to embed simple agents in
 * applications. This Agent is mean to be much simpler than a traditional
 * agent and as such it's more restrictive than what can be configured
 * for a traditional agent. For specifics see the Flume User Guide.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EmbeddedAgent {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(EmbeddedAgent.class);

  private final MaterializedConfigurationProvider configurationProvider;
  private final String name;

  private final LifecycleSupervisor supervisor;
  private State state;
  private SourceRunner sourceRunner;
  private Channel channel;
  private SinkRunner sinkRunner;
  private EmbeddedSource embeddedSource;

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @VisibleForTesting
  EmbeddedAgent(MaterializedConfigurationProvider configurationProvider,
      String name) {
    this.configurationProvider = configurationProvider;
    this.name = name;
    state = State.NEW;
    supervisor = new LifecycleSupervisor();

  }

  public EmbeddedAgent(String name) {
    this(new MaterializedConfigurationProvider(), name);
  }

  /**
   * Configures the embedded agent. Can only be called after the object
   * is created or after the stop() method is called.
   *
   * @param properties source, channel, and sink group configuration
   * @throws FlumeException if a component is unable to be found or configured
   * @throws IllegalStateException if called while the agent is started
   */
  public void configure(Map<String, String> properties)
      throws FlumeException {
    if (state == State.STARTED) {
      throw new IllegalStateException("Cannot be configured while started");
    }
    doConfigure(properties);
    state = State.STOPPED;
  }

  /**
   * Started the agent. Can only be called after a successful call to
   * configure().
   *
   * @throws FlumeException if a component cannot be started
   * @throws IllegalStateException if the agent has not been configured or is
   * already started
   */
  public void start()
      throws FlumeException {
    if (state == State.STARTED) {
      throw new IllegalStateException("Cannot be started while started");
    } else if (state == State.NEW) {
      throw new IllegalStateException("Cannot be started before being " +
          "configured");
    }
    // This check needs to be done before doStart(),
    // as doStart() accesses sourceRunner.getSource()
    Source source = Preconditions.checkNotNull(sourceRunner.getSource(),
        "Source runner returned null source");
    if (source instanceof EmbeddedSource) {
      embeddedSource = (EmbeddedSource)source;
    } else {
      throw new IllegalStateException("Unknown source type: " + source.getClass().getName());
    }
    doStart();
    state = State.STARTED;
  }

  /**
   * Stops the agent. Can only be called after a successful call to start().
   * After a call to stop(), the agent can be re-configured with the
   * configure() method or re-started with the start() method.
   *
   * @throws FlumeException if a component cannot be stopped
   * @throws IllegalStateException if the agent is not started
   */
  public void stop()
      throws FlumeException {
    if (state != State.STARTED) {
      throw new IllegalStateException("Cannot be stopped unless started");
    }
    supervisor.stop();
    embeddedSource = null;
    state = State.STOPPED;
  }

  private void doConfigure(Map<String, String> properties) {

    properties = EmbeddedAgentConfiguration.configure(name, properties);

    if (LOGGER.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
      LOGGER.debug("Agent configuration values");
      for (String key : new TreeSet<String>(properties.keySet())) {
        LOGGER.debug(key + " = " + properties.get(key));
      }
    }

    MaterializedConfiguration conf = configurationProvider.get(name,
        properties);
    Map<String, SourceRunner> sources = conf.getSourceRunners();
    if (sources.size() != 1) {
      throw new FlumeException("Expected one source and got "  +
          sources.size());
    }
    Map<String, Channel> channels = conf.getChannels();
    if (channels.size() != 1) {
      throw new FlumeException("Expected one channel and got "  +
          channels.size());
    }
    Map<String, SinkRunner> sinks = conf.getSinkRunners();
    if (sinks.size() != 1) {
      throw new FlumeException("Expected one sink group and got "  +
          sinks.size());
    }
    this.sourceRunner = sources.values().iterator().next();
    this.channel = channels.values().iterator().next();
    this.sinkRunner = sinks.values().iterator().next();
  }

  /**
   * Adds event to the channel owned by the agent. Note however, that the
   * event is not copied and as such, the byte array and headers cannot
   * be re-used by the caller.
   * @param event
   * @throws EventDeliveryException if unable to add event to channel
   */
  public void put(Event event) throws EventDeliveryException {
    if (state != State.STARTED) {
      throw new IllegalStateException("Cannot put events unless started");
    }
    try {
      embeddedSource.put(event);
    } catch (ChannelException ex) {
      throw new EventDeliveryException("Embedded agent " + name +
          ": Unable to process event: " + ex.getMessage(), ex);
    }
  }

  /**
   * Adds events to the channel owned by the agent. Note however, that the
   * event is not copied and as such, the byte array and headers cannot
   * be re-used by the caller.
   * @param events
   * @throws EventDeliveryException if unable to add event to channel
   */
  public void putAll(List<Event> events) throws EventDeliveryException {
    if (state != State.STARTED) {
      throw new IllegalStateException("Cannot put events unless started");
    }
    try {
      embeddedSource.putAll(events);
    } catch (ChannelException ex) {
      throw new EventDeliveryException("Embedded agent " + name +
          ": Unable to process event: " + ex.getMessage(), ex);
    }
  }

  private void doStart() {
    boolean error = true;
    try {
      channel.start();
      sinkRunner.start();
      sourceRunner.start();

      supervisor.supervise(channel,
          new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      supervisor.supervise(sinkRunner,
          new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      supervisor.supervise(sourceRunner,
          new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      error = false;
    } finally {
      if (error) {
        stopLogError(sourceRunner);
        stopLogError(channel);
        stopLogError(sinkRunner);
        supervisor.stop();
      }
    }
  }

  private void stopLogError(LifecycleAware lifeCycleAware) {
    try {
      if (LifecycleState.START.equals(lifeCycleAware.getLifecycleState())) {
        lifeCycleAware.stop();
      }
    } catch (Exception e) {
      LOGGER.warn("Exception while stopping " + lifeCycleAware, e);
    }
  }

  private static enum State {
    NEW(),
    STOPPED(),
    STARTED();
  }
}
