/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.node;

import java.util.Map;

import junit.framework.Assert;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.Disposable;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.channel.AbstractChannel;
import org.apache.flume.conf.FlumeConfiguration;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestAbstractConfigurationProvider {

  @Test
  public void testDispoableChannel() throws Exception {
    String agentName = "agent1";
    Map<String, String> properties = getPropertiesForChannel(agentName,
        DisposableChannel.class.getName());
    MemoryConfigurationProvider provider =
        new MemoryConfigurationProvider(agentName, properties);
    MaterializedConfiguration config1 = provider.getConfiguration();
    Channel channel1 = config1.getChannels().values().iterator().next();
    Assert.assertTrue(channel1 instanceof DisposableChannel);
    MaterializedConfiguration config2 = provider.getConfiguration();
    Channel channel2 = config2.getChannels().values().iterator().next();
    Assert.assertTrue(channel2 instanceof DisposableChannel);
    Assert.assertNotSame(channel1, channel2);
  }

  @Test
  public void testReusableChannel() throws Exception {
    String agentName = "agent1";
    Map<String, String> properties = getPropertiesForChannel(agentName,
        RecyclableChannel.class.getName());
    MemoryConfigurationProvider provider =
        new MemoryConfigurationProvider(agentName, properties);

    MaterializedConfiguration config1 = provider.getConfiguration();
    Channel channel1 = config1.getChannels().values().iterator().next();
    Assert.assertTrue(channel1 instanceof RecyclableChannel);

    MaterializedConfiguration config2 = provider.getConfiguration();
    Channel channel2 = config2.getChannels().values().iterator().next();
    Assert.assertTrue(channel2 instanceof RecyclableChannel);

    Assert.assertSame(channel1, channel2);
  }

  @Test
  public void testUnspecifiedChannel() throws Exception {
    String agentName = "agent1";
    Map<String, String> properties = getPropertiesForChannel(agentName,
        UnspecifiedChannel.class.getName());
    MemoryConfigurationProvider provider =
        new MemoryConfigurationProvider(agentName, properties);

    MaterializedConfiguration config1 = provider.getConfiguration();
    Channel channel1 = config1.getChannels().values().iterator().next();
    Assert.assertTrue(channel1 instanceof UnspecifiedChannel);

    MaterializedConfiguration config2 = provider.getConfiguration();
    Channel channel2 = config2.getChannels().values().iterator().next();
    Assert.assertTrue(channel2 instanceof UnspecifiedChannel);

    Assert.assertSame(channel1, channel2);
  }

  @Test
  public void testReusableChannelNotReusedLater() throws Exception {
    String agentName = "agent1";
    Map<String, String> propertiesReusable = getPropertiesForChannel(agentName,
        RecyclableChannel.class.getName());
    Map<String, String> propertiesDispoable = getPropertiesForChannel(agentName,
        DisposableChannel.class.getName());
    MemoryConfigurationProvider provider =
        new MemoryConfigurationProvider(agentName, propertiesReusable);
    MaterializedConfiguration config1 = provider.getConfiguration();
    Channel channel1 = config1.getChannels().values().iterator().next();
    Assert.assertTrue(channel1 instanceof RecyclableChannel);

    provider.setProperties(propertiesDispoable);
    MaterializedConfiguration config2 = provider.getConfiguration();
    Channel channel2 = config2.getChannels().values().iterator().next();
    Assert.assertTrue(channel2 instanceof DisposableChannel);

    provider.setProperties(propertiesReusable);
    MaterializedConfiguration config3 = provider.getConfiguration();
    Channel channel3 = config3.getChannels().values().iterator().next();
    Assert.assertTrue(channel3 instanceof RecyclableChannel);

    Assert.assertNotSame(channel1, channel3);
  }


  private Map<String, String> getPropertiesForChannel(String agentName, String channelType) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(agentName + ".sources", "source1");
    properties.put(agentName + ".channels", "channel1");
    properties.put(agentName + ".sinks", "sink1");
    properties.put(agentName + ".sources.source1.type", "seq");
    properties.put(agentName + ".sources.source1.channels", "channel1");
    properties.put(agentName + ".channels.channel1.type", channelType);
    properties.put(agentName + ".channels.channel1.capacity", "100");
    properties.put(agentName + ".sinks.sink1.type", "null");
    properties.put(agentName + ".sinks.sink1.channel", "channel1");
    return properties;
  }

  public static class MemoryConfigurationProvider extends AbstractConfigurationProvider {
    private Map<String, String> properties;
    public MemoryConfigurationProvider(String agentName, Map<String, String> properties) {
      super(agentName);
      this.properties = properties;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }

    @Override
    protected FlumeConfiguration getFlumeConfiguration() {
      return new FlumeConfiguration(properties);
    }
  }
  @Disposable
  public static class DisposableChannel extends AbstractChannel {
    @Override
    public void put(Event event) throws ChannelException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Event take() throws ChannelException {
      throw new UnsupportedOperationException();
     }
    @Override
    public Transaction getTransaction() {
      throw new UnsupportedOperationException();
    }
  }
  @Recyclable
  public static class RecyclableChannel extends AbstractChannel {
    @Override
    public void put(Event event) throws ChannelException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Event take() throws ChannelException {
      throw new UnsupportedOperationException();
     }
    @Override
    public Transaction getTransaction() {
      throw new UnsupportedOperationException();
    }
  }
  public static class UnspecifiedChannel extends AbstractChannel {
    @Override
    public void put(Event event) throws ChannelException {
      throw new UnsupportedOperationException();
    }
    @Override
    public Event take() throws ChannelException {
      throw new UnsupportedOperationException();
     }
    @Override
    public Transaction getTransaction() {
      throw new UnsupportedOperationException();
    }
  }
}
