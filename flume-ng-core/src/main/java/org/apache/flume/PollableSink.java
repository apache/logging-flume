package org.apache.flume;

public interface PollableSink extends Sink {

  public void process() throws InterruptedException, EventDeliveryException;

}
