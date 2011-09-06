package org.apache.flume.lifecycle;

import org.apache.flume.CounterGroup;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLifecycleSupervisor {

  private LifecycleSupervisor supervisor;

  @Before
  public void setUp() {
    supervisor = new LifecycleSupervisor();
  }

  @Test
  public void testLifecycle() throws LifecycleException, InterruptedException {
    supervisor.start();
    supervisor.stop();
  }

  @Test
  public void testSupervise() throws LifecycleException, InterruptedException {
    supervisor.start();

    /* Attempt to supervise a known-to-fail config. */
    /*
     * LogicalNode node = new LogicalNode(); SupervisorPolicy policy = new
     * SupervisorPolicy.OnceOnlyPolicy(); supervisor.supervise(node, policy,
     * LifecycleState.START);
     */

    CountingLifecycleAware node = new CountingLifecycleAware();

    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor.supervise(node, policy, LifecycleState.START);

    Thread.sleep(10000);

    node = new CountingLifecycleAware();

    policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor.supervise(node, policy, LifecycleState.START);

    Thread.sleep(5000);

    supervisor.stop();
  }

  @Test
  public void testSuperviseBroken() throws LifecycleException,
      InterruptedException {

    supervisor.start();

    /* Attempt to supervise a known-to-fail config. */
    LifecycleAware node = new LifecycleAware() {

      @Override
      public void stop() {
      }

      @Override
      public void start() {
        throw new NullPointerException("Boom!");
      }

      @Override
      public LifecycleState getLifecycleState() {
        return LifecycleState.IDLE;
      }
    };

    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor.supervise(node, policy, LifecycleState.START);

    Thread.sleep(5000);

    supervisor.stop();
  }

  @Test
  public void testSuperviseSupervisor() throws LifecycleException,
      InterruptedException {

    supervisor.start();

    LifecycleSupervisor supervisor2 = new LifecycleSupervisor();

    CountingLifecycleAware node = new CountingLifecycleAware();

    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor2.supervise(node, policy, LifecycleState.START);

    supervisor.supervise(supervisor2,
        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);

    Thread.sleep(10000);

    supervisor.stop();
  }

  @Test
  public void testUnsuperviseServce() throws LifecycleException,
      InterruptedException {

    supervisor.start();

    LifecycleAware service = new CountingLifecycleAware();
    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();

    supervisor.supervise(service, policy, LifecycleState.START);
    supervisor.unsupervise(service);

    service.stop();

    supervisor.stop();
  }

  @Test
  public void testStopServce() throws LifecycleException, InterruptedException {
    supervisor.start();

    CountingLifecycleAware service = new CountingLifecycleAware();
    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();

    Assert.assertEquals(Long.valueOf(0), service.counterGroup.get("start"));
    Assert.assertEquals(Long.valueOf(0), service.counterGroup.get("stop"));

    supervisor.supervise(service, policy, LifecycleState.START);

    Thread.sleep(3200);

    Assert.assertEquals(Long.valueOf(1), service.counterGroup.get("start"));
    Assert.assertEquals(Long.valueOf(0), service.counterGroup.get("stop"));

    supervisor.setDesiredState(service, LifecycleState.STOP);

    Thread.sleep(3200);

    Assert.assertEquals(Long.valueOf(1), service.counterGroup.get("start"));
    Assert.assertEquals(Long.valueOf(1), service.counterGroup.get("stop"));

    supervisor.stop();
  }

  public static class CountingLifecycleAware implements LifecycleAware {

    public CounterGroup counterGroup;
    private LifecycleState lifecycleState;

    public CountingLifecycleAware() {
      lifecycleState = LifecycleState.IDLE;
      counterGroup = new CounterGroup();
    }

    @Override
    public void start() {

      counterGroup.incrementAndGet("start");

      lifecycleState = LifecycleState.START;
    }

    @Override
    public void stop() {

      counterGroup.incrementAndGet("stop");

      lifecycleState = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getLifecycleState() {
      return lifecycleState;
    }

  }

}
