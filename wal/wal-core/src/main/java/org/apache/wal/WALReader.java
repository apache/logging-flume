package org.apache.wal;

public interface WALReader {

  public WALEntry next();

  public void mark();

  public void reset();

}
