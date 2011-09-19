package org.apache.flume.node;

import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;

public interface NodeConfiguration {

  public Map<String, SourceRunner> getSourceRunners();

  public Map<String, SinkRunner> getSinkRunners();

  public Map<String, Channel> getChannels();

}
