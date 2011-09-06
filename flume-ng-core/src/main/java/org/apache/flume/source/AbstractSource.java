package org.apache.flume.source;

import org.apache.flume.Channel;
import org.apache.flume.Source;
import org.apache.flume.lifecycle.LifecycleState;

abstract public class AbstractSource implements Source {

  private Channel channel;

  private LifecycleState lifecycleState;

  public AbstractSource() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start() {
    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop() {
    lifecycleState = LifecycleState.STOP;
  }

  public Channel getChannel() {
    return channel;
  }

  public void setChannel(Channel channel) {
    this.channel = channel;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

}
