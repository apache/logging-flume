package org.apache.flume;

public interface PollableSink extends Sink {

  public Status process() throws EventDeliveryException;

  public static enum Status {
    READY, BACKOFF
  }

}
