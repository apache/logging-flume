package org.apache.wal;

public interface WALReader {

  public void open();

  public WALEntry next();

  public void close();

  public void mark();

  public void reset();

}
