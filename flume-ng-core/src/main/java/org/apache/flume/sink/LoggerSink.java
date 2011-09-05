package org.apache.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.PollableSink;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerSink extends AbstractEventSink implements PollableSink {

  private static final Logger logger = LoggerFactory
      .getLogger(LoggerSink.class);

  @Override
  public void process() {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();
      Event event = channel.take();
      logger.info(event.toString());
      channel.release(event);
      transaction.commit();
    } catch (Exception e) {
      transaction.rollback();
    }
  }
}
