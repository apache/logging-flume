package org.apache.wal;

import java.util.Map;

public interface WAL {

  public void configure(Map<String, String> properties);

  public void open();

  public void close();

  public WALReader getReader();

  public WALWriter getWriter();

}
