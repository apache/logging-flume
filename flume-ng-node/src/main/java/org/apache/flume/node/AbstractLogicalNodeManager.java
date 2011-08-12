package org.apache.flume.node;

import java.util.HashSet;
import java.util.Set;

import org.apache.flume.core.LogicalNode;

import com.google.common.base.Preconditions;

abstract public class AbstractLogicalNodeManager implements NodeManager {

  private Set<LogicalNode> nodes;

  public AbstractLogicalNodeManager() {
    nodes = new HashSet<LogicalNode>();
  }

  @Override
  public boolean add(LogicalNode node) {
    Preconditions.checkNotNull(node);

    return nodes.add(node);
  }

  @Override
  public boolean remove(LogicalNode node) {
    Preconditions.checkNotNull(node);

    return nodes.remove(node);
  }

  @Override
  public Set<LogicalNode> getNodes() {
    return new HashSet<LogicalNode>(nodes);
  }

  @Override
  public void setNodes(Set<LogicalNode> nodes) {
    Preconditions.checkNotNull(nodes);

    this.nodes = new HashSet<LogicalNode>(nodes);
  }

  @Override
  public String toString() {
    return "{ nodes:" + nodes + " }";
  }

}
