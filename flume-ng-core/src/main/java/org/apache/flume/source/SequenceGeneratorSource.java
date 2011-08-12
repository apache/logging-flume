package org.apache.flume.source;

import org.apache.flume.core.Context;
import org.apache.flume.core.Event;
import org.apache.flume.core.EventDeliveryException;
import org.apache.flume.core.SimpleEvent;

public class SequenceGeneratorSource extends AbstractEventSource {

  private long sequence;

  @Override
  public Event<?> next(Context context) throws InterruptedException,
      EventDeliveryException {

    Event<Long> event = new SimpleEvent<Long>();

    event.setBody(sequence++);

    return event;
  }

}
