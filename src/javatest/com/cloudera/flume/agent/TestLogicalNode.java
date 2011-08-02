/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.flume.agent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactoryImpl;
import com.cloudera.flume.conf.SourceFactoryImpl;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.master.StatusManager.NodeState;
import com.cloudera.util.Clock;

public class TestLogicalNode {

  final public static Logger LOG = Logger.getLogger(TestLogicalNode.class);

  /**
   * Test that checkConfig has the correct versioning behaviour
   */
  @Test
  public void testCheckConfig() {
    LogicalNode node = new LogicalNode(new Context(), "test-logical-node");

    assertFalse(node.checkConfig(null));

    // Are new configs accepted?
    FlumeConfigData cfgData = new FlumeConfigData(0, "null", "null", 0, 0,
        "my-test-flow");
    assertTrue(node.checkConfig(cfgData));
    assertFalse(node.checkConfig(cfgData));

    // Are updated configs accepted?
    FlumeConfigData cfgData2 = new FlumeConfigData(0, "null", "null", 1, 0,
        "my-test-flow");
    assertTrue(node.checkConfig(cfgData2));
    assertFalse(node.checkConfig(cfgData2));
    assertFalse(node.checkConfig(cfgData));

    // Are configs with the same version rejected?
    FlumeConfigData cfgData3 = new FlumeConfigData(0, "null", "null", 1, 1,
        "my-test-flow");
    assertFalse(node.checkConfig(cfgData));
    assertFalse(node.checkConfig(cfgData2));
    assertFalse(node.checkConfig(cfgData3));

  }

  /**
   * Core driver for this series of tests.
   */
  LogicalNode drive(String src, String snk) throws IOException,
      RuntimeException, FlumeSpecException, InterruptedException {
    LogicalNode node = new LogicalNode(new Context(), "test-logical-node");
    FlumeConfigData cfg = new FlumeConfigData(0, src, snk, 1, 1, "my-test-flow");
    node.loadConfig(cfg);

    long sleep = 1000;
    Clock.sleep(sleep); // sleep is not the right approach

    long reconfs = node.getReport().getLongMetric(LogicalNode.A_RECONFIGURES);
    LOG.info("reconfigured " + reconfs + " times in " + sleep + " ms");
    // failed, and then to last good which is null|null;
    assertEquals(1, reconfs);
    return node;
  }

  /**
   * This test makes sure that an "bad" config update stops and goes into an
   * error state.
   */
  @Test
  public void testFailedConfig() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    LogicalNode node = drive("fail(\"null\")", "null");

    // Check that state is failed.
    assertEquals(NodeState.ERROR, node.getStatus().state);
  }

  /**
   * Test to make sure we stop after a failed open on source
   */
  @Test
  public void testFailOpenSource() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    SourceFactoryImpl srcfact = new SourceFactoryImpl();
    srcfact.setSource("failOpen", new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        return new EventSource.Base() {
          @Override
          public void open() throws IOException {
            LOG.info("in FailOpenSource,open");
            throw new IOException("open always fails");
          }
        };
      }
    });

    FlumeBuilder.setSourceFactory(srcfact);
    LogicalNode node = drive("failOpen", "null"); // Check that state is failed.
    assertEquals(NodeState.ERROR, node.getStatus().state);

  }

  @Test
  public void testFailNextSource() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    SourceFactoryImpl srcfact = new SourceFactoryImpl();
    srcfact.setSource("failNext", new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        return new EventSource.Base() {
          @Override
          public Event next() throws IOException {
            LOG.info("in FailOpenSource.next");
            throw new IOException("next always fails");
          }
        };
      }
    });

    FlumeBuilder.setSourceFactory(srcfact);

    LogicalNode node = drive("failNext", "null");
    // Check that state is failed.
    assertEquals(NodeState.ERROR, node.getStatus().state);

  }

  @Test
  public void testFailCloseSource() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    SourceFactoryImpl srcfact = new SourceFactoryImpl();
    srcfact.setSource("failClose", new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        return new EventSource.Base() {
          @Override
          public void close() throws IOException {
            LOG.info("in FailOpenSource.close");
            throw new IOException("close always fails");
          }
        };
      }
    });

    FlumeBuilder.setSourceFactory(srcfact);

    LogicalNode node = drive("failClose", "null");
    // Check don't care if close throws exn
    assertEquals(NodeState.ERROR, node.getStatus().state);

  }

  @Test
  public void testFailOpenSink() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    SinkFactoryImpl snkfact = new SinkFactoryImpl();
    snkfact.setSink("failOpen", new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        return new EventSink.Base() {
          @Override
          public void open() throws IOException {
            throw new IOException("open always fails");
          }
        };
      }
    });

    FlumeBuilder.setSinkFactory(snkfact);

    LogicalNode node = drive("asciisynth(10)", "failOpen");
    // Check that state is failed.
    assertEquals(NodeState.ERROR, node.getStatus().state);
  }

  @Test
  public void testFailAppendSink() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    SinkFactoryImpl snkfact = new SinkFactoryImpl();
    snkfact.setSink("failAppend", new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        return new EventSink.Base() {
          @Override
          public void append(Event e) throws IOException {
            throw new IOException("sink.append always fails");
          }
        };
      }
    });

    FlumeBuilder.setSinkFactory(snkfact);

    LogicalNode node = drive("asciisynth(10)", "failAppend");

    // Check that state is failed.
    assertEquals(NodeState.ERROR, node.getStatus().state);
  }

  @Test
  public void testFailCloseSink() throws IOException, RuntimeException,
      FlumeSpecException, InterruptedException {
    SinkFactoryImpl snkfact = new SinkFactoryImpl();
    snkfact.setSink("failClose", new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        return new EventSink.Base() {
          @Override
          public void open() throws IOException {
            throw new IOException("open always fails");
          }
        };
      }
    });

    FlumeBuilder.setSinkFactory(snkfact);

    LogicalNode node = drive("asciisynth(10)", "failClose");
    // Check that state is failed.
    assertEquals(NodeState.ERROR, node.getStatus().state);

  }
}
