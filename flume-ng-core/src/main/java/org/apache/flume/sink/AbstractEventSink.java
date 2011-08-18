package org.apache.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventSink;
import org.apache.flume.lifecycle.LifecycleException;

abstract public class AbstractEventSink implements EventSink {

  @Override
  public void open(Context context) throws InterruptedException,
      LifecycleException {
    // Empty implementation by default.
  }

  @Override
  abstract public void append(Context context, Event event)
      throws InterruptedException, EventDeliveryException;

  @Override
  public void close(Context context) throws InterruptedException,
      LifecycleException {
    // Empty implementation by default.
  }

}
