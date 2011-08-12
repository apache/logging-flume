package org.apache.flume.lifecycle;

import junit.framework.Assert;

import org.apache.flume.core.Context;
import org.junit.Test;

public class TestLifecycleController {

  @Test
  public void testWaitForState() throws LifecycleException,
      InterruptedException {

    Context context = new Context();
    LifecycleAware delegate = new SleeperLifecycleDelegate();

    Assert.assertTrue(delegate.getLifecycleState().equals(LifecycleState.IDLE));

    delegate.start(context);

    boolean reached = LifecycleController.waitForState(delegate,
        LifecycleState.START, 2000);

    Assert.assertEquals(true, reached);
    Assert.assertEquals(LifecycleState.START, delegate.getLifecycleState());

    delegate.stop(context);

    reached = LifecycleController.waitForState(delegate, LifecycleState.STOP,
        2000);

    Assert.assertEquals(true, reached);
    Assert.assertEquals(LifecycleState.STOP, delegate.getLifecycleState());

    delegate.start(context);

    reached = LifecycleController.waitForState(delegate, LifecycleState.IDLE,
        500);

    Assert.assertEquals(false, reached);
    Assert.assertEquals(LifecycleState.START, delegate.getLifecycleState());

  }

  @Test
  public void testWaitForOneOf() throws LifecycleException,
      InterruptedException {

    Context context = new Context();
    LifecycleAware delegate = new SleeperLifecycleDelegate();

    Assert.assertEquals(LifecycleState.IDLE, delegate.getLifecycleState());

    delegate.start(context);

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
    public void start(Context context) throws LifecycleException {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        throw new LifecycleException(e);
      }

      state = LifecycleState.START;
    }

    @Override
    public void stop(Context context) throws LifecycleException {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        throw new LifecycleException(e);
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
