package org.apache.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventSink;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;

abstract public class AbstractEventSink implements EventSink, LifecycleAware {

  private Channel channel;

  private LifecycleState lifecycleState;

  public AbstractEventSink() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public synchronized void start(Context context) {
    lifecycleState = LifecycleState.START;
  }

  @Override
  public synchronized void stop(Context context) {
    lifecycleState = LifecycleState.STOP;
  }

  public synchronized Channel getChannel() {
    return channel;
  }

  @Override
  public synchronized void setChannel(Channel channel) {
    this.channel = channel;
  }

  @Override
  public synchronized LifecycleState getLifecycleState() {
    return lifecycleState;
  }

}
