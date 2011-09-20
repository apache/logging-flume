package org.apache.flume.node.nodemanager;

import java.util.HashSet;
import java.util.Set;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.node.NodeManager;

import com.google.common.base.Preconditions;

abstract public class AbstractLogicalNodeManager implements NodeManager {

  private Set<LifecycleAware> nodes;

  public AbstractLogicalNodeManager() {
    nodes = new HashSet<LifecycleAware>();
  }

  @Override
  public boolean add(LifecycleAware node) {
    Preconditions.checkNotNull(node);

    return nodes.add(node);
  }

  @Override
  public boolean remove(LifecycleAware node) {
    Preconditions.checkNotNull(node);

    return nodes.remove(node);
  }

  @Override
  public Set<LifecycleAware> getNodes() {
    return nodes;
  }

  @Override
  public void setNodes(Set<LifecycleAware> nodes) {
    Preconditions.checkNotNull(nodes);

    this.nodes = new HashSet<LifecycleAware>(nodes);
  }

  @Override
  public String toString() {
    return "{ nodes:" + nodes + " }";
  }

}
