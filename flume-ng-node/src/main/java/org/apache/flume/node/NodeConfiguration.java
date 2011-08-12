package org.apache.flume.node;

public interface NodeConfiguration {

  public long getVersion();

  public String getName();

  public String getSourceDefinition();

  public String getSinkDefinition();

}
