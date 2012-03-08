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

package org.apache.flume.lifecycle;

import junit.framework.Assert;

import org.junit.Test;

public class TestLifecycleController {

  @Test
  public void testWaitForState() throws LifecycleException,
      InterruptedException {

    LifecycleAware delegate = new SleeperLifecycleDelegate();

    Assert.assertTrue(delegate.getLifecycleState().equals(LifecycleState.IDLE));

    delegate.start();

    boolean reached = LifecycleController.waitForState(delegate,
        LifecycleState.START, 2000);

    Assert.assertEquals(true, reached);
    Assert.assertEquals(LifecycleState.START, delegate.getLifecycleState());

    delegate.stop();

    reached = LifecycleController.waitForState(delegate, LifecycleState.STOP,
        2000);

    Assert.assertEquals(true, reached);
    Assert.assertEquals(LifecycleState.STOP, delegate.getLifecycleState());

    delegate.start();

    reached = LifecycleController.waitForState(delegate, LifecycleState.IDLE,
        500);

    Assert.assertEquals(false, reached);
    Assert.assertEquals(LifecycleState.START, delegate.getLifecycleState());

  }

  @Test
  public void testWaitForOneOf() throws LifecycleException,
      InterruptedException {

    LifecycleAware delegate = new SleeperLifecycleDelegate();

    Assert.assertEquals(LifecycleState.IDLE, delegate.getLifecycleState());

    delegate.start();

    boolean reached = LifecycleController.waitForOneOf(delegate,
        new LifecycleState[] { LifecycleState.STOP, LifecycleState.START },
        2000);

    Assert.assertTrue("Matched a state change", reached);
    Assert.assertEquals(LifecycleState.START, delegate.getLifecycleState());
  }

  public static class SleeperLifecycleDelegate implements LifecycleAware {

    private long sleepTime;
    private LifecycleState state;

    public SleeperLifecycleDelegate() {
      sleepTime = 0;
      state = LifecycleState.IDLE;
    }

    @Override
    public void start() {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        // Do nothing.
      }

      state = LifecycleState.START;
    }

    @Override
    public void stop() {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        // Do nothing
      }

      state = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getLifecycleState() {
      return state;
    }

    public long getSleepTime() {
      return sleepTime;
    }

    public void setSleepTime(long sleepTime) {
      this.sleepTime = sleepTime;
    }

  }

}
