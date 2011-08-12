package org.apache.flume.node.nodemanager;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.flume.LogicalNode;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeStatusMonitor implements Runnable {

  private static final Logger logger = LoggerFactory
      .getLogger(NodeStatusMonitor.class);

  private Map<String, Status> statuses;

  private NodeManager nodeManager;

  public NodeStatusMonitor() {
    statuses = new HashMap<String, Status>();
  }

  @Override
  public void run() {
    updateStatus();
  }

  public void updateStatus() {
    Set<LogicalNode> nodes = nodeManager.getNodes();

    logger.debug("Checking statuses of all nodes");

    for (LogicalNode node : nodes) {
      updateStatus(node);
    }
  }

  public void updateStatus(LogicalNode node) {
    logger.debug("Checking statuses of node:{}", node);

    Long now = System.currentTimeMillis();
    Status status = statuses.get(node.getName());

    if (status == null) {
      status = new Status();

      status.firstSeen = now;
    }

    if (!node.getLifecycleState().equals(status.state)) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Detected state change: node:{} - {} (lastSeen:{}) -> {}",
            new Object[] { node.getName(), status.state, status.lastSeen,
                node.getLifecycleState() });
      }

      status.lastStateChange = now;
    }

    status.lastSeen = now;
    status.state = node.getLifecycleState();

    statuses.put(node.getName(), status);
  }

  @Override
  public String toString() {
    return "{ statuses:" + statuses + " nodeManager:" + nodeManager + " }";
  }

  public NodeManager getNodeManager() {
    return nodeManager;
  }

  public void setNodeManager(NodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  public static class Status {
    public Long firstSeen;
    public Long lastSeen;
    public LifecycleState state;
    public Long lastStateChange;
  }

}
