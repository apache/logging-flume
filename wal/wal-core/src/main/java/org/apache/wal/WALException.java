package org.apache.wal;

public class WALException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public WALException(String message, Throwable cause) {
    super(message, cause);
  }

  public WALException(String message) {
    super(message);
  }

  public WALException(Throwable cause) {
    super(cause);
  }

}
