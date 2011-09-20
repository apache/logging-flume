package org.apache.flume.node;

import java.util.Set;

import org.apache.flume.lifecycle.LifecycleAware;

public interface NodeManager extends LifecycleAware {

  public boolean add(LifecycleAware node);

  public boolean remove(LifecycleAware node);

  public Set<LifecycleAware> getNodes();

  public void setNodes(Set<LifecycleAware> nodes);

}
