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
package com.cloudera.flume.core;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.conf.SinkFactoryImpl;
import com.google.common.base.Preconditions;

/**
 * Any sink or decorator that creates new sinks needs to pass the context into
 * its children. This test verifies that a context is passed in.
 */
public class TestContextThreading {

  /**
   * This is a context with a value "bar" associated with attribute "foo"
   */
  static class FooContext extends Context {
    FooContext(Context ctx) {
      super(ctx);
      putValue("foo", "bar");
    }

    FooContext() {
      super();
      putValue("foo", "bar");
    }
  }

  /**
   * This sink will thrown an exception on open if a empty or invalid context is
   * threaded through to it.
   */
  static class CheckSink extends EventSink.Base {
    Logger LOG = LoggerFactory.getLogger(CheckSink.class);
    Context ctx;

    CheckSink(Context ctx) {
      this.ctx = ctx;
    }

    public void open() {
      Preconditions.checkArgument(ctx.getValue("foo").equals("bar"));
      LOG.info("Context Threaded");
    }

    public static SinkBuilder builder() {
      return new SinkBuilder() {
        @Override
        public EventSink build(Context context, String... argv) {
          return new CheckSink(context);
        }
      };
    }
  }

  @Before
  public void addCheckSink() {
    SinkFactoryImpl sf = new SinkFactoryImpl();
    sf.setSink("check", CheckSink.builder());
    FlumeBuilder.setSinkFactory(sf);
  }

  /**
   * The absence of a IllegalArgumentException means that the FooContext is
   * properly passed through the roller.
   */
  @Test
  public void testRollThreading() throws FlumeSpecException, IOException,
      InterruptedException {
    String spec = "roll(10) { check }";
    EventSink snk = FlumeBuilder.buildSink(new FooContext(), spec);
    snk.open();

    snk.close();
  }

  /**
   * The absence of a IllegalArgumentException means that the FooContext is
   * properly passed the diskFailover deco.
   */
  @Test
  public void testDFOThreading() throws FlumeSpecException, IOException,
      InterruptedException {
    String spec = "{ diskFailover=> check }";
    EventSink snk = FlumeBuilder.buildSink(new FooContext(LogicalNodeContext
        .testingContext()), spec);
    snk.open();
    snk.close();
  }

  /**
   * The absence of a IllegalArgumentException means that the FooContext is
   * properly passed through the ackedWriteAhead deco.
   */
  @Test
  public void testE2EThreading() throws IOException, FlumeSpecException,
      InterruptedException {
    String spec = "{ ackedWriteAhead => check }";
    EventSink snk = FlumeBuilder.buildSink(new FooContext(LogicalNodeContext
        .testingContext()), spec);
    snk.open();
    snk.close();
  }

}
