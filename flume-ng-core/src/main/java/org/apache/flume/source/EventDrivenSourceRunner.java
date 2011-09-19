package org.apache.flume.source;

import org.apache.flume.SourceRunner;
import org.apache.flume.lifecycle.LifecycleState;

public class EventDrivenSourceRunner extends SourceRunner {

  private LifecycleState lifecycleState;

  public EventDrivenSourceRunner() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start() {
    getSource().start();
    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop() {
    getSource().stop();
    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

}
