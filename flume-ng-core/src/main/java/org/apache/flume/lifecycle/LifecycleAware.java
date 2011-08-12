package org.apache.flume.lifecycle;

import org.apache.flume.core.Context;

public interface LifecycleAware {

  public void start(Context context) throws LifecycleException;

  public void stop(Context context) throws LifecycleException;

  public LifecycleState getLifecycleState();

  public void transitionTo(LifecycleState state);

}
