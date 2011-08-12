package org.apache.flume.core;

import org.apache.flume.lifecycle.LifecycleException;

public interface EventSink {

  public void open(Context context) throws InterruptedException,
      LifecycleException;

  public void append(Context context, Event<?> event)
      throws InterruptedException, MessageDeliveryException;

  public void close(Context context) throws InterruptedException,
      LifecycleException;

}
