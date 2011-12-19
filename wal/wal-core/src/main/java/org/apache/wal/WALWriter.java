package org.apache.wal;

public interface WALWriter {

  public void open();

  public void write(WALEntry entry);

  public void close();

  public void mark();

  public void reset();

}
