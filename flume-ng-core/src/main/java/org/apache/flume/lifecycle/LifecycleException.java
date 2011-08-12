package org.apache.flume.lifecycle;

public class LifecycleException extends Exception {

  private static final long serialVersionUID = 4689000562519155240L;

  public LifecycleException() {
    super();
  }

  public LifecycleException(String message) {
    super(message);
  }

  public LifecycleException(String message, Throwable t) {
    super(message, t);
  }

  public LifecycleException(Throwable t) {
    super(t);
  }

}
