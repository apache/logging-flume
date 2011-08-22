package org.apache.flume.durability;

import java.io.IOException;

import org.apache.flume.Event;

public interface WALWriter {

  public void open() throws IOException;

  public void close() throws IOException;

  public void flush() throws IOException;

  public void write(Event event) throws IOException;

}
