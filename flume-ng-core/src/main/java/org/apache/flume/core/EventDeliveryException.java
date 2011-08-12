package org.apache.flume.core;

public class EventDeliveryException extends Exception {

  private static final long serialVersionUID = 1102327497549834945L;

  public EventDeliveryException() {
    super();
  }

  public EventDeliveryException(String message) {
    super(message);
  }

  public EventDeliveryException(String message, Throwable t) {
    super(message, t);
  }

  public EventDeliveryException(Throwable t) {
    super(t);
  }

}
