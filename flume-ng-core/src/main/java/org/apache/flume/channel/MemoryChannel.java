package org.apache.flume.channel;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;

public class MemoryChannel implements Channel {

  private BlockingQueue<Event> queue;

  public MemoryChannel() {
    queue = new ArrayBlockingQueue<Event>(50);
  }

  @Override
  public void put(Event event) throws InterruptedException,
      EventDeliveryException {

    queue.put(event);
  }

  @Override
  public Event take() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void release(Event event) {
    /* Release is a no-op on an in memory channel. */
  }

  @Override
  public Transaction getTransaction() {
    return NoOpTransaction.sharedInstance();
  }

  /**
   * <p>
   * A no-op transaction implementation that does nothing at all.
   * </p>
   */
  public static class NoOpTransaction implements Transaction {

    private static NoOpTransaction sharedInstance;

    public static Transaction sharedInstance() {
      if (sharedInstance == null) {
        sharedInstance = new NoOpTransaction();
      }

      return sharedInstance;
    }

    @Override
    public void begin() {
    }

    @Override
    public void commit() {
    }

    @Override
    public void rollback() {
    }

  }

}
