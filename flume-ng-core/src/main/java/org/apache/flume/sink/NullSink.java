package org.apache.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.PollableSink;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NullSink extends AbstractSink implements PollableSink {

  private static final Logger logger = LoggerFactory.getLogger(NullSink.class);

  @Override
  public void process() {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      Event event = channel.take();
      /* We purposefully do nothing useful. */

      channel.release(event);
      transaction.commit();
    } catch (Exception e) {
      logger.error("Failed to deliver event. Exception follows.", e);
      transaction.rollback();
    }

  }

}
