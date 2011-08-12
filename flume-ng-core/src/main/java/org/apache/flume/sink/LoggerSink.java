package org.apache.flume.sink;

import org.apache.flume.core.Context;
import org.apache.flume.core.Event;
import org.apache.flume.core.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerSink extends AbstractEventSink {

  private static final Logger logger = LoggerFactory
      .getLogger(TestLoggerSink.class);

  @Override
  public void append(Context context, Event<?> event)
      throws InterruptedException, EventDeliveryException {

    logger.info("event:{}", event);
  }

}
