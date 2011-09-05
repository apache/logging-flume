package org.apache.flume;

public interface PollableSource extends EventSource {

  /*
   * FIXME: Arvind removed InterruptedException from the interface in his
   * branch.
   */
  public void process() throws InterruptedException, EventDeliveryException;

}
