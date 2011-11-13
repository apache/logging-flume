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

import java.io.File;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.Channel;
import org.apache.flume.ChannelFactory;
import org.apache.flume.SinkFactory;
import org.apache.flume.SourceFactory;
import org.apache.flume.SourceRunner;
import org.apache.flume.channel.DefaultChannelFactory;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.node.NodeConfiguration;
import org.apache.flume.node.nodemanager.NodeConfigurationAware;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.flume.sink.LoggerSink;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.DefaultSourceFactory;
import org.apache.flume.source.NetcatSource;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJsonFileConfigurationProvider {

  private static final File testFile = new File(
      TestJsonFileConfigurationProvider.class.getClassLoader()
          .getResource("flume-conf.json").getFile());
  private static final Logger logger = LoggerFactory
      .getLogger(TestJsonFileConfigurationProvider.class);

  private JsonFileConfigurationProvider provider;

  @Before
  public void setUp() {
    ChannelFactory channelFactory = new DefaultChannelFactory();
    SourceFactory sourceFactory = new DefaultSourceFactory();
    SinkFactory sinkFactory = new DefaultSinkFactory();

    channelFactory.register("memory", MemoryChannel.class);

    sourceFactory.register("seq", SequenceGeneratorSource.class);
    sourceFactory.register("netcat", NetcatSource.class);

    sinkFactory.register("null", NullSink.class);
    sinkFactory.register("logger", LoggerSink.class);

    provider = new JsonFileConfigurationProvider();

    provider.setNodeName("localhost");
    provider.setChannelFactory(channelFactory);
    provider.setSourceFactory(sourceFactory);
    provider.setSinkFactory(sinkFactory);
  }

  @Test
  public void testLifecycle() throws InterruptedException {
    final AtomicBoolean sawEvent = new AtomicBoolean();
    final CountDownLatch latch = new CountDownLatch(1);

    provider.setFile(testFile);

    NodeConfigurationAware delegate = new NodeConfigurationAware() {

      @Override
      public void onNodeConfigurationChanged(NodeConfiguration nodeConfiguration) {
        sawEvent.set(true);

        Map<String, Channel> channels = nodeConfiguration.getChannels();

        Assert.assertNotNull("Channel ch1 exists", channels.get("ch1"));
        Assert.assertNotNull("Channel ch2 exists", channels.get("ch2"));

        Map<String, SourceRunner> sourceRunners = nodeConfiguration
            .getSourceRunners();

        Assert.assertNotNull("Source runner for source1 exists",
            sourceRunners.get("source1"));
        Assert.assertNotNull("Source runner for source2 exists",
            sourceRunners.get("source2"));

        latch.countDown();
      }
    };

    provider.setConfigurationAware(delegate);

    provider.start();

    Thread.sleep(100L);

    provider.stop();

    latch.await(5, TimeUnit.SECONDS);

    logger.debug("provider:{}", provider);

    Assert.assertTrue("Saw a configuration event", sawEvent.get());
  }

}
