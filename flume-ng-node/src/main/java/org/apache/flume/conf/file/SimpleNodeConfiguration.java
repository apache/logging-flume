/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
