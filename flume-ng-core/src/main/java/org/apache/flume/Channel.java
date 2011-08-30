package org.apache.flume;

public interface Channel {

  public void put(Event event) throws InterruptedException,
      EventDeliveryException;

  public Event take() throws InterruptedException;

  public void release(Event event);

  public Transaction getTransaction();

}
