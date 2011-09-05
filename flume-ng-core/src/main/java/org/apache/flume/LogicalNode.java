package org.apache.flume;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;

public class LogicalNode implements LifecycleAware {

  private String name;
  private SourceRunner sourceRunner;
  private SinkRunner sinkRunner;

  private LifecycleState lifecycleState;

  public LogicalNode() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start(Context context) {

    sourceRunner.start(context);
    sinkRunner.start(context);

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop(Context context) {

    sourceRunner.stop(context);
    sinkRunner.stop(context);

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public SourceRunner getSourceRunner() {
    return sourceRunner;
  }

  public void setSourceRunner(SourceRunner sourceRunner) {
    this.sourceRunner = sourceRunner;
  }

  public SinkRunner getSinkRunner() {
    return sinkRunner;
  }

  public void setSinkRunner(SinkRunner sinkRunner) {
    this.sinkRunner = sinkRunner;
  }

}
