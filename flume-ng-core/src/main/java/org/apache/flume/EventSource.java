package org.apache.flume;

import org.apache.flume.lifecycle.LifecycleAware;

public interface EventSource extends LifecycleAware {

  public void setChannel(Channel channel);

  public Channel getChannel();

}
