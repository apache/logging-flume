package org.apache.flume.node;

import org.apache.flume.core.Context;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FlumeNode implements LifecycleAware {

  private static final Logger logger = LoggerFactory.getLogger(FlumeNode.class);

  private String name;
  private LifecycleState lifecycleState;
  private NodeManager nodeManager;
  private NodeConfigurationClient configurationClient;

  @Override
  public void start(Context context) throws LifecycleException,
      InterruptedException {

    Preconditions.checkState(name != null, "Node name can not be null");
    Preconditions.checkState(nodeManager != null,
        "Node manager can not be null");

    logger.info("Flume node starting - {}", name);

    nodeManager.start(context);

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop(Context context) throws LifecycleException,
      InterruptedException {

    logger.info("Flume node stopping - {}", name);

    nodeManager.stop(context);

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public String toString() {
    return "{ name:" + name + " nodeManager:" + nodeManager + " }";
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public NodeManager getNodeManager() {
    return nodeManager;
  }

  public void setNodeManager(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

}
