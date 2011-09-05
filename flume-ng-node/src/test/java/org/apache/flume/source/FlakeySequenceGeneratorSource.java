package org.apache.flume.source;

import org.apache.flume.EventDeliveryException;

public class FlakeySequenceGeneratorSource extends SequenceGeneratorSource {

  @Override
  public void process() throws EventDeliveryException, InterruptedException {

    if (Math.round(Math.random()) == 1) {
      Thread.sleep(1000);
      throw new EventDeliveryException("I'm broken!");
    } else {
      super.process();
    }
  }

}
