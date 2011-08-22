package org.apache.flume.durability;

import java.io.IOException;

public interface WAL {

  public WALWriter getWriter() throws IOException;

}
