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

public class DefaultLogicalNodeManager extends AbstractLogicalNodeManager
    implements NodeConfigurationAware {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultLogicalNodeManager.class);

  private LifecycleSupervisor nodeSupervisor;
  private LifecycleState lifecycleState;

  public DefaultLogicalNodeManager() {
    nodeSupervisor = new LifecycleSupervisor();
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void onNodeConfigurationChanged(NodeConfiguration nodeConfiguration) {
    logger.info("Node configuration change:{}", nodeConfiguration);

    for (Entry<String, SinkRunner> entry : nodeConfiguration.getSinkRunners()
        .entrySet()) {

      nodeSupervisor.supervise(entry.getValue(),
          new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
    }

    for (Entry<String, SourceRunner> entry : nodeConfiguration
        .getSourceRunners().entrySet()) {

      nodeSupervisor.supervise(entry.getValue(),
          new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
    }
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
      nodeSupervisor.setDesiredState(node, LifecycleState.STOP);
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

    nodeSupervisor.stop();

    logger.debug("Node manager stopped");

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

}
