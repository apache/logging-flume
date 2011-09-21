package org.apache.flume.source;

import org.apache.flume.EventDeliveryException;

public class FlakeySequenceGeneratorSource extends SequenceGeneratorSource {

  @Override
  public Status process() throws EventDeliveryException {

    if (Math.round(Math.random()) == 1) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // Do nothing.
      }

      throw new EventDeliveryException("I'm broken!");
    } else {
      return super.process();
    }
  }

}
