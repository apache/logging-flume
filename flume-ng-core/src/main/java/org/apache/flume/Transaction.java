package org.apache.flume;

public interface Transaction {

  public void begin();

  public void commit();

  public void rollback();

}
