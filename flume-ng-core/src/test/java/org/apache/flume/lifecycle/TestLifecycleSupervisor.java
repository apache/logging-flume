package org.apache.flume.lifecycle;

import org.apache.flume.Context;
import org.apache.flume.LogicalNode;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.SequenceGeneratorSource;
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

}
