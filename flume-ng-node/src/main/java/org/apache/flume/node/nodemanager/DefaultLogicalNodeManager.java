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

package org.apache.flume.node.nodemanager;

import java.util.Map.Entry;

import org.apache.flume.Channel;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.apache.flume.node.NodeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.util.Properties;
import java.util.Set;
import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.MonitoringType;


public class DefaultLogicalNodeManager extends AbstractLogicalNodeManager
    implements NodeConfigurationAware {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultLogicalNodeManager.class);

  private LifecycleSupervisor nodeSupervisor;
  private LifecycleState lifecycleState;
  private NodeConfiguration nodeConfiguration;

  private MonitorService monitorServer;

  public static final String CONF_MONITOR_CLASS = "flume.monitoring.type";
  public static final String CONF_MONITOR_PREFIX = "flume.monitoring.";

  public DefaultLogicalNodeManager() {
    nodeSupervisor = new LifecycleSupervisor();
    lifecycleState = LifecycleState.IDLE;
    nodeConfiguration = null;
  }

  @Override
  public void stopAllComponents() {
    if (this.nodeConfiguration != null) {
      logger.info("Shutting down configuration: {}", this.nodeConfiguration);
      for (Entry<String, SourceRunner> entry : this.nodeConfiguration
          .getSourceRunners().entrySet()) {
        try{
          logger.info("Stopping Source " + entry.getKey());
          nodeSupervisor.unsupervise(entry.getValue());
        } catch (Exception e){
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }

      for (Entry<String, SinkRunner> entry :
        this.nodeConfiguration.getSinkRunners().entrySet()) {
        try{
          logger.info("Stopping Sink " + entry.getKey());
          nodeSupervisor.unsupervise(entry.getValue());
        } catch (Exception e){
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }

      for (Entry<String, Channel> entry :
        this.nodeConfiguration.getChannels().entrySet()) {
        try{
          logger.info("Stopping Channel " + entry.getKey());
          nodeSupervisor.unsupervise(entry.getValue());
        } catch (Exception e){
          logger.error("Error while stopping {}", entry.getValue(), e);
        }
      }
    }
    if(monitorServer != null) {
      monitorServer.stop();
    }
  }

  @Override
  public void startAllComponents(NodeConfiguration nodeConfiguration) {
    logger.info("Starting new configuration:{}", nodeConfiguration);

    this.nodeConfiguration = nodeConfiguration;

    for (Entry<String, Channel> entry :
      nodeConfiguration.getChannels().entrySet()) {
      try{
        logger.info("Starting Channel " + entry.getKey());
        nodeSupervisor.supervise(entry.getValue(),
            new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e){
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }

    /*
     * Wait for all channels to start.
     */
    for(Channel ch: nodeConfiguration.getChannels().values()){
      while(ch.getLifecycleState() != LifecycleState.START
          && !nodeSupervisor.isComponentInErrorState(ch)){
        try {
          logger.info("Waiting for channel: " + ch.getName() +
              " to start. Sleeping for 500 ms");
          Thread.sleep(500);
        } catch (InterruptedException e) {
          logger.error("Interrupted while waiting for channel to start.", e);
          Throwables.propagate(e);
        }
      }
    }

    for (Entry<String, SinkRunner> entry : nodeConfiguration.getSinkRunners()
        .entrySet()) {
      try{
        logger.info("Starting Sink " + entry.getKey());
        nodeSupervisor.supervise(entry.getValue(),
          new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }

    for (Entry<String, SourceRunner> entry : nodeConfiguration
        .getSourceRunners().entrySet()) {
      try{
        logger.info("Starting Source " + entry.getKey());
        nodeSupervisor.supervise(entry.getValue(),
          new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
      } catch (Exception e) {
        logger.error("Error while starting {}", entry.getValue(), e);
      }
    }

    this.loadMonitoring();
  }

  @Override
  public boolean add(LifecycleAware node) {
    /*
     * FIXME: This type of overriding worries me. There should be a better
     * separation of addition of nodes and management. (i.e. state vs. function)
     */
    Preconditions.checkState(getLifecycleState().equals(LifecycleState.START),
        "You can not add nodes to a manager that hasn't been started");

    if (super.add(node)) {
      nodeSupervisor.supervise(node,
          new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);

      return true;
    }

    return false;
  }

  @Override
  public boolean remove(LifecycleAware node) {
    /*
     * FIXME: This type of overriding worries me. There should be a better
     * separation of addition of nodes and management. (i.e. state vs. function)
     */
    Preconditions.checkState(getLifecycleState().equals(LifecycleState.START),
        "You can not remove nodes from a manager that hasn't been started");

    if (super.remove(node)) {
      nodeSupervisor.unsupervise(node);

      return true;
    }

    return false;
  }

  @Override
  public void start() {

    logger.info("Node manager starting");

    nodeSupervisor.start();

    logger.debug("Node manager started");

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop() {

    logger.info("Node manager stopping");

    stopAllComponents();

    nodeSupervisor.stop();

    logger.debug("Node manager stopped");

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  private void loadMonitoring() {
    Properties systemProps = System.getProperties();
    Set<String> keys = systemProps.stringPropertyNames();
    try {
      if (keys.contains(CONF_MONITOR_CLASS)) {
        String monitorType = systemProps.getProperty(CONF_MONITOR_CLASS);
        Class<? extends MonitorService> klass;
        try {
          //Is it a known type?
          klass = MonitoringType.valueOf(
                  monitorType.toUpperCase()).getMonitorClass();
        } catch (Exception e) {
          //Not a known type, use FQCN
          klass = (Class<? extends MonitorService>) Class.forName(monitorType);
        }
        this.monitorServer = klass.newInstance();
        Context context = new Context();
        for (String key : keys) {
          if (key.startsWith(CONF_MONITOR_PREFIX)) {
            context.put(key.substring(CONF_MONITOR_PREFIX.length()),
                    systemProps.getProperty(key));
          }
        }
        monitorServer.configure(context);
        monitorServer.start();
      }
    } catch (Exception e) {
      logger.warn("Error starting monitoring. "
              + "Monitoring might not be available.", e);
    }

  }
}
