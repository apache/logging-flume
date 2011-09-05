package org.apache.flume.source;

import org.apache.flume.Channel;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.Transaction;
import org.apache.flume.event.EventBuilder;

public class SequenceGeneratorSource extends AbstractSource implements
    PollableSource {

  private long sequence;

  public SequenceGeneratorSource() {
    sequence = 0;
  }

  @Override
  public void process() throws InterruptedException, EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();
      channel.put(EventBuilder.withBody(String.valueOf(sequence++).getBytes()));
      transaction.commit();
    } catch (Exception e) {
      transaction.rollback();
    }
    /* FIXME: Add finally { transaction.close() } */
  }

}
