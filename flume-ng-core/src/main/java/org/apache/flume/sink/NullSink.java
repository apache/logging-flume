package org.apache.flume.sink;

import org.apache.flume.core.Context;
import org.apache.flume.core.Event;
import org.apache.flume.core.EventDeliveryException;

public class NullSink extends AbstractEventSink {

  @Override
  public void append(Context context, Event<?> event)
      throws InterruptedException, EventDeliveryException {

    /* We purposefully do absolutely nothing. */

  }

}
