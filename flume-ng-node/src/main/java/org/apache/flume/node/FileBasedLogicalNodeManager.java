package org.apache.flume.node;

import org.apache.flume.core.Context;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;

public class FileBasedLogicalNodeManager extends AbstractLogicalNodeManager {

  private LifecycleState lifecycleState;

  public FileBasedLogicalNodeManager() {
    super();

    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start(Context context) throws LifecycleException {
    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop(Context context) throws LifecycleException {
    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  @Override
  public void transitionTo(LifecycleState state) {
    // Not sure if this make sense.
  }

}
