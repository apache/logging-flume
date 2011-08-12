package org.apache.flume;

import org.apache.flume.lifecycle.LifecycleException;

public interface EventSink {

  public void open(Context context) throws InterruptedException,
      LifecycleException;

  public void append(Context context, Event<?> event)
      throws InterruptedException, EventDeliveryException;

  public void close(Context context) throws InterruptedException,
      LifecycleException;

}
