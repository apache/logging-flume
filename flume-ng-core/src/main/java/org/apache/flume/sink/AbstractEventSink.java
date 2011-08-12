package org.apache.flume.sink;

import org.apache.flume.core.Context;
import org.apache.flume.core.Event;
import org.apache.flume.core.EventSink;
import org.apache.flume.core.MessageDeliveryException;
import org.apache.flume.lifecycle.LifecycleException;

abstract public class AbstractEventSink implements EventSink {

  @Override
  public void open(Context context) throws InterruptedException,
      LifecycleException {
    // Empty implementation by default.
  }

  @Override
  abstract public void append(Context context, Event<?> event)
      throws InterruptedException, MessageDeliveryException;

  @Override
  public void close(Context context) throws InterruptedException,
      LifecycleException {
    // Empty implementation by default.
  }

}
