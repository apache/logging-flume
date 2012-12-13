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

package org.apache.flume.source;
import static org.mockito.Mockito.*;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestBasicSourceSemantics {

  private BasicSourceSemantics source;
  private ChannelProcessor channelProcessor;
  private Context context;

  @Before
  public void setUp() {
    context = new Context();
    channelProcessor = mock(ChannelProcessor.class);
  }
  public DoNothingSource spyAndConfigure(DoNothingSource source) {
    source = spy(source);
    source.setChannelProcessor(channelProcessor);
    source.configure(context);
    return source;
  }
  @Test
  public void testDoConfigureThrowsException() throws Exception {
    source = spy(new DoNothingSource() {
      @Override
      protected void doConfigure(Context context) throws FlumeException {
        throw new FlumeException("dummy");
      }
    });
    source.setChannelProcessor(channelProcessor);
    try {
      source.configure(context);
      Assert.fail();
    } catch (FlumeException expected) {

    }
    Assert.assertFalse(source.isStarted());
    Assert.assertEquals(LifecycleState.ERROR, source.getLifecycleState());
    Assert.assertNotNull(source.getStartException());
  }
  @Test
  public void testDoStartThrowsException() throws Exception {
    source = spyAndConfigure(new DoNothingSource() {
      @Override
      protected void doStart() throws FlumeException {
        throw new FlumeException("dummy");
      }
    });
    source.start();
    Assert.assertFalse(source.isStarted());
    Assert.assertEquals(LifecycleState.ERROR, source.getLifecycleState());
    Assert.assertNotNull(source.getStartException());
  }
  @Test
  public void testDoStopThrowsException() throws Exception {
    source = spyAndConfigure(new DoNothingSource() {
      @Override
      protected void doStop() throws FlumeException {
        throw new FlumeException("dummy");
      }
    });
    source.start();
    source.stop();
    Assert.assertFalse(source.isStarted());
    Assert.assertEquals(LifecycleState.ERROR, source.getLifecycleState());
    Assert.assertNull(source.getStartException());
  }
  @Test
  public void testConfigureCalledWhenStarted() throws Exception {
    source = spyAndConfigure(new DoNothingSource());
    source.start();
    try {
      source.configure(context);
      Assert.fail();
    } catch (IllegalStateException expected) {

    }
    Assert.assertTrue(source.isStarted());
    Assert.assertNull(source.getStartException());
  }
  private static class DoNothingSource extends BasicSourceSemantics {
    @Override
    protected void doConfigure(Context context) throws FlumeException {

    }
    @Override
    protected void doStart() throws FlumeException {

    }
    @Override
    protected void doStop() throws FlumeException {

    }
  }
}