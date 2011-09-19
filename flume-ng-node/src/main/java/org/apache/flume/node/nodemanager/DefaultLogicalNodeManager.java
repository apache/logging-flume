package org.apache.flume.node.nodemanager;

import java.util.Map.Entry;

import org.apache.flume.LogicalNode;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
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
  public boolean add(LogicalNode node) {
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
  public boolean remove(LogicalNode node) {
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
