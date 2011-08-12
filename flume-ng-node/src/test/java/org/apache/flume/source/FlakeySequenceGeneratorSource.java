package org.apache.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlakeySequenceGeneratorSource extends SequenceGeneratorSource {

  private static final Logger logger = LoggerFactory
      .getLogger(FlakeySequenceGeneratorSource.class);

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
