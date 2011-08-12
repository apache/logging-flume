package org.apache.flume.core;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class LogicalNode implements LifecycleAware {

  private static final Logger logger = LoggerFactory
      .getLogger(LogicalNode.class);

  private String name;
  private EventSource source;
  private EventSink sink;

  private ChannelDriver driver;

  private LifecycleState lifecycleState;

  public LogicalNode() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start(Context context) throws LifecycleException,
      InterruptedException {

    logger.info("Starting logical node:{}", this);

    Preconditions.checkState(name != null, "Logical node name can not be null");
    Preconditions.checkState(source != null,
        "Logical node source can not be null");
    Preconditions.checkState(sink != null, "Logical node sink can not be null");

    driver = new ChannelDriver(name + "-channelDriver");

    driver.setSource(source);
    driver.setSink(sink);

    driver.start(context);

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop(Context context) throws LifecycleException {
    logger.info("Stopping logical node:{}", this);

    boolean complete = false;

    driver.stop(context);

    try {
      complete = LifecycleController.waitForOneOf(driver, new LifecycleState[] {
          LifecycleState.STOP, LifecycleState.ERROR });
    } catch (InterruptedException e) {
      logger.debug("Interrupted while waiting for the driver to stop.");
      complete = false;
    }

    if (!complete) {
      logger
          .error(
              "There's a good chance the source or sink aren't shutting down. This will lead to problems. Contact the developers! Trace:{}",
              Thread.currentThread().getStackTrace());
    }

    /* Our state is the channel driver's state. */
    lifecycleState = driver.getLifecycleState();
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  @Override
  public String toString() {
    return "{ name:" + name + " source:" + source + " sink:" + sink
        + " lifecycleState:" + lifecycleState + " }";
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public EventSource getSource() {
    return source;
  }

  public void setSource(EventSource source) {
    this.source = source;
  }

  public EventSink getSink() {
    return sink;
  }

  public void setSink(EventSink sink) {
    this.sink = sink;
  }

}
