package org.apache.flume.core;

public interface EventSink {

  public void open(Context context);

  public void append(Context context, Event<?> event);

  public void close(Context context);

}
