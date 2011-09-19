package org.apache.flume.node;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FlumeNode implements LifecycleAware {

  private static final Logger logger = LoggerFactory.getLogger(FlumeNode.class);

  private String name;
  private LifecycleState lifecycleState;
  private NodeManager nodeManager;
  private ConfigurationProvider configurationProvider;
  private LifecycleSupervisor supervisor;

  public FlumeNode() {
    supervisor = new LifecycleSupervisor();
  }

  @Override
  public void start() {

    Preconditions.checkState(name != null, "Node name can not be null");
    Preconditions.checkState(nodeManager != null,
        "Node manager can not be null");

    supervisor.start();

    logger.info("Flume node starting - {}", name);

    supervisor.supervise(nodeManager,
        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
    supervisor.supervise(configurationProvider,
        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop() {

    logger.info("Flume node stopping - {}", name);

    supervisor.stop();

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public String toString() {
    return "{ name:" + name + " nodeManager:" + nodeManager
        + " configurationProvider:" + configurationProvider + " }";
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

  public ConfigurationProvider getConfigurationProvider() {
    return configurationProvider;
  }

  public void setConfigurationProvider(
      ConfigurationProvider configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

}
