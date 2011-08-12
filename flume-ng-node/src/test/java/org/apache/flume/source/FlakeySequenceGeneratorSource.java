package org.apache.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

public class FlakeySequenceGeneratorSource extends SequenceGeneratorSource {

  @Override
  public Event<?> next(Context context) throws InterruptedException,
      EventDeliveryException {

    if (Math.round(Math.random()) == 1) {
      Thread.sleep(1000);
      throw new InterruptedException("I'm broken!");
    }

    return super.next(context);
  }

}
