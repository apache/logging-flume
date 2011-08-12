package org.apache.flume.core;

public interface EventSource {

  public void open(Context context);

  public Event<?> next(Context context);

  public void close(Context context);

}
