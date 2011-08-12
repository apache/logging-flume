package org.apache.flume.core;

public class MessageDeliveryException extends Exception {

  private static final long serialVersionUID = 1102327497549834945L;

  public MessageDeliveryException() {
    super();
  }

  public MessageDeliveryException(String message) {
    super(message);
  }

  public MessageDeliveryException(String message, Throwable t) {
    super(message, t);
  }

  public MessageDeliveryException(Throwable t) {
    super(t);
  }

}
