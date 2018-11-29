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
package org.apache.flume.agent.embedded;

import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.MaterializedConfiguration;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestEmbeddedAgentEmbeddedSource {

  private EmbeddedAgent agent;
  private Map<String, String> properties;

  private MaterializedConfiguration config;
  private EmbeddedSource source;
  private SourceRunner sourceRunner;
  private Channel channel;
  private SinkRunner sinkRunner;

  @Before
  public void setUp() throws Exception {

    properties = Maps.newHashMap();
    properties.put("source.type", EmbeddedAgentConfiguration.SOURCE_TYPE_EMBEDDED);
    properties.put("channel.type", "memory");
    properties.put("sinks", "sink1 sink2");
    properties.put("sink1.type", "avro");
    properties.put("sink2.type", "avro");
    properties.put("processor.type", "load_balance");

    sourceRunner = mock(SourceRunner.class);
    channel = mock(Channel.class);
    sinkRunner = mock(SinkRunner.class);

    source = mock(EmbeddedSource.class);
    when(sourceRunner.getSource()).thenReturn(source);

    when(sourceRunner.getLifecycleState()).thenReturn(LifecycleState.START);
    when(channel.getLifecycleState()).thenReturn(LifecycleState.START);
    when(sinkRunner.getLifecycleState()).thenReturn(LifecycleState.START);

    config = new MaterializedConfiguration() {
      @Override
      public ImmutableMap<String, SourceRunner> getSourceRunners() {
        Map<String, SourceRunner> result = Maps.newHashMap();
        result.put("source", sourceRunner);
        return ImmutableMap.copyOf(result);
      }

      @Override
      public ImmutableMap<String, SinkRunner> getSinkRunners() {
        Map<String, SinkRunner> result = Maps.newHashMap();
        result.put("sink", sinkRunner);
        return ImmutableMap.copyOf(result);
      }

      @Override
      public ImmutableMap<String, Channel> getChannels() {
        Map<String, Channel> result = Maps.newHashMap();
        result.put("channel", channel);
        return ImmutableMap.copyOf(result);
      }

      @Override
      public void addSourceRunner(String name, SourceRunner sourceRunner) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void addSinkRunner(String name, SinkRunner sinkRunner) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void addChannel(String name, Channel channel) {
        throw new UnsupportedOperationException();
      }
    };
    agent = new EmbeddedAgent(new MaterializedConfigurationProvider() {
      public MaterializedConfiguration get(String name, Map<String, String> properties) {
        return config;
      }
    }, "dummy");
  }

  @Test
  public void testStart() {
    agent.configure(properties);
    agent.start();
    verify(sourceRunner, times(1)).start();
    verify(channel, times(1)).start();
    verify(sinkRunner, times(1)).start();
  }

  @Test
  public void testStop() {
    agent.configure(properties);
    agent.start();
    agent.stop();
    verify(sourceRunner, times(1)).stop();
    verify(channel, times(1)).stop();
    verify(sinkRunner, times(1)).stop();
  }

  @Test
  public void testStartSourceThrowsException() {
    doThrow(new LocalRuntimeException()).when(sourceRunner).start();
    startExpectingLocalRuntimeException();
  }

  @Test
  public void testStartChannelThrowsException() {
    doThrow(new LocalRuntimeException()).when(channel).start();
    startExpectingLocalRuntimeException();
  }

  @Test
  public void testStartSinkThrowsException() {
    doThrow(new LocalRuntimeException()).when(sinkRunner).start();
    startExpectingLocalRuntimeException();
  }

  private void startExpectingLocalRuntimeException() {
    agent.configure(properties);
    try {
      agent.start();
      Assert.fail();
    } catch (LocalRuntimeException e) {
      // expected
    }
    verify(sourceRunner, times(1)).stop();
    verify(channel, times(1)).stop();
    verify(sinkRunner, times(1)).stop();
  }

  private static class LocalRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 116546244849853151L;
  }

  @Test
  public void testPut() throws EventDeliveryException {
    Event event = new SimpleEvent();
    agent.configure(properties);
    agent.start();
    agent.put(event);
    verify(source, times(1)).put(event);
  }

  @Test
  public void testPutAll() throws EventDeliveryException {
    Event event = new SimpleEvent();
    List<Event> events = Lists.newArrayList();
    events.add(event);
    agent.configure(properties);
    agent.start();
    agent.putAll(events);
    verify(source, times(1)).putAll(events);
  }

  @Test(expected = IllegalStateException.class)
  public void testPutNotStarted() throws EventDeliveryException {
    Event event = new SimpleEvent();
    agent.configure(properties);
    agent.put(event);
  }

  @Test(expected = IllegalStateException.class)
  public void testPutAllNotStarted() throws EventDeliveryException {
    Event event = new SimpleEvent();
    List<Event> events = Lists.newArrayList();
    events.add(event);
    agent.configure(properties);
    agent.putAll(events);
  }
}
