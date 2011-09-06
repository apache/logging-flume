package org.apache.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.PollableSink;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NullSink extends AbstractSink implements PollableSink {

  private static final Logger logger = LoggerFactory.getLogger(NullSink.class);

  private CounterGroup counterGroup;

  public NullSink() {
    counterGroup = new CounterGroup();
  }

  @Override
  public void process() {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      Event event = channel.take();

      channel.release(event);
      transaction.commit();

      counterGroup.incrementAndGet("events.successful");
    } catch (Exception e) {
      counterGroup.incrementAndGet("events.failed");
      logger.error("Failed to deliver event. Exception follows.", e);
      transaction.rollback();
    }

  }

  @Override
  public void start() {
    logger.info("Null sink starting");

    super.start();

    logger.debug("Null sink started");
  }

  @Override
  public void stop() {
    logger.info("Null sink stopping");

    super.stop();

    logger.debug("Null sink stopped. Event metrics:{}", counterGroup);
  }

}
