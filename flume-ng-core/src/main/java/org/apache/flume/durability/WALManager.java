package org.apache.flume.durability;

public interface WALManager {

  public WAL getWAL(String name);

}
