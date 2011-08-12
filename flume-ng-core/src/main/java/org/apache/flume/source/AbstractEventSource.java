package org.apache.flume.source;

import org.apache.flume.core.Context;
import org.apache.flume.core.Event;
import org.apache.flume.core.EventSource;

abstract public class AbstractEventSource implements EventSource {

  @Override
  public void open(Context context) {
    // Empty implementation by default.
  }

  @Override
  abstract public Event<?> next(Context context);

  @Override
  public void close(Context context) {
    // Empty implementation by default.
  }

}
