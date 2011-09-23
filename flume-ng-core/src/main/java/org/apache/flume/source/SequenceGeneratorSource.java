package org.apache.flume.source;

import org.apache.flume.Channel;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.Transaction;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceGeneratorSource extends AbstractSource implements
    PollableSource {

  private static final Logger logger = LoggerFactory
      .getLogger(SequenceGeneratorSource.class);

  private long sequence;
  private CounterGroup counterGroup;

  public SequenceGeneratorSource() {
    sequence = 0;
    counterGroup = new CounterGroup();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();
      channel.put(EventBuilder.withBody(String.valueOf(sequence++).getBytes()));
      transaction.commit();

      counterGroup.incrementAndGet("events.successful");
    } catch (Exception e) {
      transaction.rollback();
      counterGroup.incrementAndGet("events.failed");
    } finally {
      transaction.close();
    }

    return Status.READY;
  }

  @Override
  public void start() {
    logger.info("Sequence generator source starting");

    super.start();

    logger.debug("Sequence generator source started");
  }

  @Override
  public void stop() {
    logger.info("Sequence generator source stopping");

    super.stop();

    logger.info("Sequence generator source stopped. Metrics:{}", counterGroup);
  }

}
