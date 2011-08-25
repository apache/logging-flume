package org.apache.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventSource;
import org.apache.flume.lifecycle.LifecycleException;

abstract public class AbstractEventSource implements EventSource {

  @Override
  public void open(Context context) throws LifecycleException,
      InterruptedException {
    // Empty implementation by default.
  }

  @Override
  abstract public Event next(Context context) throws InterruptedException,
      EventDeliveryException;

  @Override
  public void close(Context context) throws LifecycleException,
      InterruptedException {
    // Empty implementation by default.
  }

}
