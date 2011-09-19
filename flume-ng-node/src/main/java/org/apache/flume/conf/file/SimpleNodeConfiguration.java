package org.apache.flume.conf.file;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.node.NodeConfiguration;

public class SimpleNodeConfiguration implements NodeConfiguration {

  private Map<String, Channel> channels;
  private Map<String, SourceRunner> sourceRunners;
  private Map<String, SinkRunner> sinkRunners;

  public SimpleNodeConfiguration() {
    channels = new HashMap<String, Channel>();
    sourceRunners = new HashMap<String, SourceRunner>();
    sinkRunners = new HashMap<String, SinkRunner>();
  }

  @Override
  public String toString() {
    return "{ sourceRunners:" + sourceRunners + " sinkRunners:" + sinkRunners
        + " channels:" + channels + " }";
  }

  @Override
  public Map<String, Channel> getChannels() {
    return channels;
  }

  public void setChannels(Map<String, Channel> channels) {
    this.channels = channels;
  }

  @Override
  public Map<String, SourceRunner> getSourceRunners() {
    return sourceRunners;
  }

  public void setSourceRunners(Map<String, SourceRunner> sourceRunners) {
    this.sourceRunners = sourceRunners;
  }

  @Override
  public Map<String, SinkRunner> getSinkRunners() {
    return sinkRunners;
  }

  public void setSinkRunners(Map<String, SinkRunner> sinkRunners) {
    this.sinkRunners = sinkRunners;
  }

}
