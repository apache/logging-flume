package org.apache.flume.node;

public class DefaultNodeConfiguration implements NodeConfiguration {

  private long version;
  private String name;
  private String sourceDefinition;
  private String sinkDefinition;

  @Override
  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String getSourceDefinition() {
    return sourceDefinition;
  }

  public void setSourceDefinition(String sourceDefinition) {
    this.sourceDefinition = sourceDefinition;
  }

  @Override
  public String getSinkDefinition() {
    return sinkDefinition;
  }

  public void setSinkDefinition(String sinkDefinition) {
    this.sinkDefinition = sinkDefinition;
  }

}
