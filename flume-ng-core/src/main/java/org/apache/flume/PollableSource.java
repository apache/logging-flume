package org.apache.flume;

public interface PollableSource extends Source {

  public Status process() throws EventDeliveryException;

  public static enum Status {
    READY, BACKOFF
  }

}
