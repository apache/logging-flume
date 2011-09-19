package org.apache.flume.conf.file;

import java.util.HashSet;
import java.util.Set;

import org.apache.flume.Channel;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;

public class JsonFlumeConfiguration {

  private Set<Channel> channels;
  private Set<SourceRunner> sourceRunners;
  private Set<SinkRunner> sinkRunners;

  public JsonFlumeConfiguration() {
    channels = new HashSet<Channel>();
    sourceRunners = new HashSet<SourceRunner>();
    sinkRunners = new HashSet<SinkRunner>();
  }

  @Override
  public String toString() {
    return "{ sourceRunners:" + sourceRunners + " sinkRunners:" + sinkRunners
        + " channels:" + channels + " }";
  }

  public Set<Channel> getChannels() {
    return channels;
  }

  public void setChannels(Set<Channel> channels) {
    this.channels = channels;
  }

  public Set<SourceRunner> getSourceRunners() {
    return sourceRunners;
  }

  public void setSourceRunners(Set<SourceRunner> sourceRunners) {
    this.sourceRunners = sourceRunners;
  }

  public Set<SinkRunner> getSinkRunners() {
    return sinkRunners;
  }

  public void setSinkRunners(Set<SinkRunner> sinkRunners) {
    this.sinkRunners = sinkRunners;
  }

}
