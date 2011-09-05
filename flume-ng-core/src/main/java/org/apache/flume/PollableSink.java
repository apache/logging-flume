package org.apache.flume;

public interface PollableSink extends EventSink {

  public void process() throws InterruptedException, EventDeliveryException;

}
