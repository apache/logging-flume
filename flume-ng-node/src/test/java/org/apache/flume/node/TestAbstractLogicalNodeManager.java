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

package org.apache.flume.node;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceRunner;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.nodemanager.AbstractLogicalNodeManager;
import org.apache.flume.sink.DefaultSinkProcessor;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAbstractLogicalNodeManager {

  private static final Logger logger = LoggerFactory
      .getLogger(TestAbstractLogicalNodeManager.class);

  private AbstractLogicalNodeManager nodeManager;

  @Before
  public void setUp() {
    nodeManager = new AbstractLogicalNodeManager() {

      private LifecycleState lifecycleState = LifecycleState.IDLE;

      @Override
      public void stop() {

        for (LifecycleAware node : getNodes()) {
          node.stop();

          boolean reached = false;

          try {
            reached = LifecycleController.waitForOneOf(node,
                LifecycleState.STOP_OR_ERROR);
          } catch (InterruptedException e) {
            // Do nothing.
          }

          if (!reached) {
            logger.error(
                "Unable to stop logical node:{} This *will* cause failures.",
                node);
          }

          if (node.getLifecycleState().equals(LifecycleState.ERROR)) {
            lifecycleState = LifecycleState.ERROR;
          }
        }

        lifecycleState = LifecycleState.STOP;
      }

      @Override
      public void start() {

        for (LifecycleAware node : getNodes()) {
          node.start();

          boolean reached = false;

          try {
            reached = LifecycleController.waitForOneOf(node,
                LifecycleState.START_OR_ERROR);
          } catch (InterruptedException e) {
            // Do nothing.
          }

          if (!reached) {
            logger.error(
                "Unable to stop logical node:{} This *will* cause failures.",
                node);
          }

          if (node.getLifecycleState().equals(LifecycleState.ERROR)) {
            lifecycleState = LifecycleState.ERROR;
          }
        }

        lifecycleState = LifecycleState.START;
      }

      @Override
      public LifecycleState getLifecycleState() {
        return lifecycleState;
      }
    };
  }

  @Test
  public void testEmptyLifecycle() throws LifecycleException,
  InterruptedException {

    nodeManager.start();
    boolean reached = LifecycleController.waitForOneOf(nodeManager,
        LifecycleState.START_OR_ERROR);

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.START, nodeManager.getLifecycleState());

    nodeManager.stop();
    reached = LifecycleController.waitForOneOf(nodeManager,
        LifecycleState.STOP_OR_ERROR);

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.STOP, nodeManager.getLifecycleState());
  }

  @Test
  public void testLifecycle() throws LifecycleException, InterruptedException {

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());

    Source generatorSource = new SequenceGeneratorSource();
    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    generatorSource.setChannelProcessor(new ChannelProcessor(rcs));

    Sink nullSink = new NullSink();
    nullSink.setChannel(channel);

    nodeManager.add(SourceRunner.forSource(generatorSource));
    SinkProcessor processor = new DefaultSinkProcessor();
    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(nullSink);
    processor.setSinks(sinks);
    nodeManager.add(new SinkRunner(processor));

    nodeManager.start();
    boolean reached = LifecycleController.waitForOneOf(nodeManager,
        LifecycleState.START_OR_ERROR);

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.START, nodeManager.getLifecycleState());

    nodeManager.stop();
    reached = LifecycleController.waitForOneOf(nodeManager,
        LifecycleState.STOP_OR_ERROR);

    Assert.assertTrue(reached);
    Assert.assertEquals(LifecycleState.STOP, nodeManager.getLifecycleState());
  }

  @Test
  public void testRapidLifecycleFlapping() throws LifecycleException,
  InterruptedException {

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());

    Source source = new SequenceGeneratorSource();
    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));

    Sink sink = new NullSink();
    sink.setChannel(channel);

    nodeManager.add(SourceRunner.forSource(source));
    SinkProcessor processor = new DefaultSinkProcessor();
    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(sink);
    processor.setSinks(sinks);
    nodeManager.add(new SinkRunner(processor));

    for (int i = 0; i < 10; i++) {
      nodeManager.start();
      boolean reached = LifecycleController.waitForOneOf(nodeManager,
          LifecycleState.START_OR_ERROR);

      Assert.assertTrue(reached);
      Assert
      .assertEquals(LifecycleState.START, nodeManager.getLifecycleState());

      nodeManager.stop();
      reached = LifecycleController.waitForOneOf(nodeManager,
          LifecycleState.STOP_OR_ERROR);

      Assert.assertTrue(reached);
      Assert.assertEquals(LifecycleState.STOP, nodeManager.getLifecycleState());
    }
  }

}
