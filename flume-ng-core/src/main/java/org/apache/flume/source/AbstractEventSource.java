package org.apache.flume.source;

import org.apache.flume.core.Context;
import org.apache.flume.core.Event;
import org.apache.flume.core.EventSource;
import org.apache.flume.core.EventDeliveryException;

abstract public class AbstractEventSource implements EventSource {

  @Override
  public void open(Context context) throws InterruptedException {
    // Empty implementation by default.
  }

  @Override
  abstract public Event<?> next(Context context) throws InterruptedException,
      EventDeliveryException;

  @Override
  public void close(Context context) throws InterruptedException {
    // Empty implementation by default.
  }

}
