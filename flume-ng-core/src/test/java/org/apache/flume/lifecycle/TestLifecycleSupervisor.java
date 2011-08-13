package org.apache.flume.lifecycle;

import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.LogicalNode;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.SequenceGeneratorSource;
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
    Context context = new Context();

    supervisor.start(context);
    supervisor.stop(context);
  }

  @Test
  public void testSupervise() throws LifecycleException, InterruptedException {
    Context context = new Context();

    supervisor.start(context);

    /* Attempt to supervise a known-to-fail config. */
    /*
     * LogicalNode node = new LogicalNode(); SupervisorPolicy policy = new
     * SupervisorPolicy.OnceOnlyPolicy(); supervisor.supervise(node, policy,
     * LifecycleState.START);
     */

    LogicalNode node = new LogicalNode();
    node.setName("node1");
    node.setSource(new SequenceGeneratorSource());
    node.setSink(new NullSink());

    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor.supervise(node, policy, LifecycleState.START);

    Thread.sleep(10000);

    node = new LogicalNode();
    node.setName("node2");
    node.setSource(new SequenceGeneratorSource());
    node.setSink(new NullSink());

    policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor.supervise(node, policy, LifecycleState.START);

    Thread.sleep(5000);

    supervisor.stop(context);
  }

  @Test
  public void testSuperviseBroken() throws LifecycleException,
      InterruptedException {
    Context context = new Context();

    supervisor.start(context);

    /* Attempt to supervise a known-to-fail config. */
    LogicalNode node = new LogicalNode();
    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor.supervise(node, policy, LifecycleState.START);

    Thread.sleep(5000);

    supervisor.stop(context);
  }

  @Test
  public void testSuperviseSupervisor() throws LifecycleException,
      InterruptedException {

    Context context = new Context();

    supervisor.start(context);

    LifecycleSupervisor supervisor2 = new LifecycleSupervisor();

    /* Attempt to supervise a known-to-fail config. */
    LogicalNode node = new LogicalNode();
    node.setName("node1");
    node.setSource(new SequenceGeneratorSource());
    node.setSink(new NullSink());

    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();
    supervisor2.supervise(node, policy, LifecycleState.START);

    supervisor.supervise(supervisor2,
        new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);

    Thread.sleep(10000);

    supervisor.stop(context);
  }

  @Test
  public void testUnsuperviseServce() throws LifecycleException,
      InterruptedException {
    Context context = new Context();

    supervisor.start(context);

    LifecycleAware service = new CountingLifecycleAware();
    SupervisorPolicy policy = new SupervisorPolicy.OnceOnlyPolicy();

    supervisor.supervise(service, policy, LifecycleState.START);
    supervisor.unsupervise(service);

    service.stop(context);

    supervisor.stop(context);
  }

  @Test
  public void testStopServce() throws LifecycleException, InterruptedException {
    Context context = new Context();

    supervisor.start(context);

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

    supervisor.stop(context);
  }

  public static class CountingLifecycleAware implements LifecycleAware {

    public CounterGroup counterGroup;
    private LifecycleState lifecycleState;

    public CountingLifecycleAware() {
      lifecycleState = LifecycleState.IDLE;
      counterGroup = new CounterGroup();
    }

    @Override
    public void start(Context context) throws LifecycleException,
        InterruptedException {

      counterGroup.incrementAndGet("start");

      lifecycleState = LifecycleState.START;
    }

    @Override
    public void stop(Context context) throws LifecycleException,
        InterruptedException {

      counterGroup.incrementAndGet("stop");

      lifecycleState = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getLifecycleState() {
      return lifecycleState;
    }

  }

}
