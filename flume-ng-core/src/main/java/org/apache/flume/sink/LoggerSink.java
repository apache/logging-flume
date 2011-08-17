package org.apache.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerSink extends AbstractEventSink {

  private static final Logger logger = LoggerFactory
      .getLogger(LoggerSink.class);

  @Override
  public void append(Context context, Event<?> event)
      throws InterruptedException, EventDeliveryException {

    logger.info("event:{}", event);
  }

}
