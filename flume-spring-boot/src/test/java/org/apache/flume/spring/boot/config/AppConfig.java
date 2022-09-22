/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.flume.spring.boot.config;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.sink.DefaultSinkProcessor;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.SequenceGeneratorSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
@Configuration
public class AppConfig extends AbstractFlumeConfiguration {

  @Bean
  @ConfigurationProperties(prefix = "flume.sources.source1")
  public Map<String, String> source1Properties() {
    return new HashMap<>();
  }

  @Bean
  @ConfigurationProperties(prefix = "flume.channels.channel1")
  public Map<String, String> channel1Properties() {
    return new HashMap<>();
  }

  @Bean
  public Channel memoryChannel(Map<String, String> channel1Properties) {
    return configureChannel("channel1", MemoryChannel.class, channel1Properties);
  }

  @Bean
  public SourceRunner seqSource(Channel memoryChannel, Map<String, String> source1Properties) {
    ChannelSelector selector = new ReplicatingChannelSelector();
    selector.setChannels(listOf(memoryChannel));
    return configureSource("source1", SequenceGeneratorSource.class, selector,
        source1Properties);
  }

  @Bean
  public SinkRunner nullSink(Channel memoryChannel) {
    SinkProcessor sinkProcessor = new DefaultSinkProcessor();
    Sink sink = configureSink("null", NullSink.class, memoryChannel,null);
    sinkProcessor.setSinks(listOf(sink));
    return new SinkRunner(sinkProcessor);
  }
}
