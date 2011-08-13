package org.apache.flume.node.nodemanager;

import org.apache.flume.Context;
import org.apache.flume.EventSink;
import org.apache.flume.EventSource;
import org.apache.flume.LogicalNode;
import org.apache.flume.SinkFactory;
import org.apache.flume.SourceFactory;
import org.apache.flume.lifecycle.LifecycleException;
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

  private SourceFactory sourceFactory;
  private SinkFactory sinkFactory;

  private LifecycleSupervisor nodeSupervisor;
  private LifecycleState lifecycleState;

  public DefaultLogicalNodeManager() {
    nodeSupervisor = new LifecycleSupervisor();
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void onNodeConfigurationChanged(NodeConfiguration nodeConfiguration) {
    logger.info("Node configuration change:{}", nodeConfiguration);

    /*
     * FIXME: Decide if nodeConfiguration is worth applying. We can't trust the
     * caller to know our config.
     */

    EventSource source = null;
    EventSink sink = null;

    try {
      source = sourceFactory.create(nodeConfiguration.getSourceDefinition());
    } catch (InstantiationException e) {
      logger
          .error(
              "Failed to apply configuration:{} because of source failure:{} - retaining old configuration",
              nodeConfiguration, e.getMessage());
      return;
    }

    try {
      sink = sinkFactory.create(nodeConfiguration.getSinkDefinition());
    } catch (InstantiationException e) {
      logger
          .error(
              "Failed to apply configuration:{} because of sink failure:{} - retaining old configuration",
              nodeConfiguration, e.getMessage());
      return;
    }

    LogicalNode newLogicalNode = new LogicalNode();

    newLogicalNode.setName(nodeConfiguration.getName());
    newLogicalNode.setSource(source);
    newLogicalNode.setSink(sink);

    add(newLogicalNode);
  }

  @Override
  public boolean add(LogicalNode node) {
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
  public void start(Context context) throws LifecycleException,
      InterruptedException {

    logger.info("Node manager starting");

    nodeSupervisor.start(context);

    logger.debug("Node manager started");

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop(Context context) throws LifecycleException,
      InterruptedException {

    logger.info("Node manager stopping");

    nodeSupervisor.stop(context);

    logger.debug("Node manager stopped");

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

}
