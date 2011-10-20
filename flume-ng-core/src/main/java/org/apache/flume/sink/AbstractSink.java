package org.apache.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.Sink;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;

import com.google.common.base.Preconditions;

abstract public class AbstractSink implements Sink, LifecycleAware {

  private Channel channel;

  private LifecycleState lifecycleState;

  public AbstractSink() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public synchronized void start() {
    Preconditions.checkState(channel != null, "No channel configured");

    lifecycleState = LifecycleState.START;
  }

  @Override
  public synchronized void stop() {
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
