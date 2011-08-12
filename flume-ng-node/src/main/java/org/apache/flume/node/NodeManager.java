package org.apache.flume.node;

import java.util.Set;

import org.apache.flume.core.LogicalNode;
import org.apache.flume.lifecycle.LifecycleAware;

public interface NodeManager extends LifecycleAware {

  public boolean add(LogicalNode node);

  public boolean remove(LogicalNode node);

  public Set<LogicalNode> getNodes();

  public void setNodes(Set<LogicalNode> nodes);

}
