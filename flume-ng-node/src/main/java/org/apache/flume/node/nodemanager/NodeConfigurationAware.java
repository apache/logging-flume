package org.apache.flume.node.nodemanager;

import org.apache.flume.node.NodeConfiguration;

public interface NodeConfigurationAware {

  public void onNodeConfigurationChanged(NodeConfiguration nodeConfiguration);

}
