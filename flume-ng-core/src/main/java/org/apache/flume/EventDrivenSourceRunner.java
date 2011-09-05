package org.apache.flume;

import org.apache.flume.lifecycle.LifecycleState;

public class EventDrivenSourceRunner implements SourceRunner {

  private EventDrivenSource source;

  private LifecycleState lifecycleState;

  public EventDrivenSourceRunner() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start(Context context) {
    source.start(context);
    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop(Context context) {
    source.stop(context);
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
