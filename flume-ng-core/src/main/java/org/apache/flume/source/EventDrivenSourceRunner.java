package org.apache.flume.source;

import org.apache.flume.EventDrivenSource;
import org.apache.flume.SourceRunner;
import org.apache.flume.lifecycle.LifecycleState;

public class EventDrivenSourceRunner implements SourceRunner {

  private EventDrivenSource source;

  private LifecycleState lifecycleState;

  public EventDrivenSourceRunner() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start() {
    source.start();
    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop() {
    source.stop();
    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public EventDrivenSource getSource() {
    return source;
  }

  public void setSource(EventDrivenSource source) {
    this.source = source;
  }

}
