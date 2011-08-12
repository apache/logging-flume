package org.apache.flume.core;

import org.apache.flume.lifecycle.LifecycleException;

public interface EventSource {

  public void open(Context context) throws InterruptedException,
      LifecycleException;

  public Event<?> next(Context context) throws InterruptedException,
      EventDeliveryException;

  public void close(Context context) throws InterruptedException,
      LifecycleException;

}
