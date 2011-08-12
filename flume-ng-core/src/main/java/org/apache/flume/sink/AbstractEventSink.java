package org.apache.flume.sink;

import org.apache.flume.core.Context;
import org.apache.flume.core.Event;
import org.apache.flume.core.EventSink;

abstract public class AbstractEventSink implements EventSink {

  @Override
  public void open(Context context) {
    // Empty implementation by default.
  }

  @Override
  abstract public void append(Context context, Event<?> event);

  @Override
  public void close(Context context) {
    // Empty implementation by default.
  }

}
