package org.apache.flume.core;

import org.apache.flume.core.ChannelDriver.ChannelDriverThread;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A complete Flume node - a source, sink pair that can be run.
 * </p>
 * <p>
 * A logical node in Flume is the user-configurable unit of execution. Users
 * provide a name, source, and sink and the logical node handles the lifecycle
 * as well as moving data from the source to the sink. The latter is referred to
 * as
 * <q>the shuffle loop.</q> Internally, the logical node contains a
 * {@link ChannelDriver} which does the heavy lifting of lifecycle events (i.e.
 * {@link #start(Context)} and {@link #stop(Context)}).
 * </p>
 * <p>
 * LogicalNode implements the {@link LifecycleAware} interface. Both the
 * {@link #start(Context)} and {@link #stop(Context)} methods are asynchronous
 * but do block while starting / stopping the underlying infrastructure (i.e.
 * ChannelDriver). Both methods may also be interrupted. In the case of start,
 * an interruption will force the logical node to attempt to clean up resources
 * which may involve stopping the source and sink (which can in turn block). An
 * interrupt to stop will, in turn, interrupt the underlying thread(s). In both
 * cases though, the logical node will continue to block on cleanup to prevent
 * nasty issues with subsequent restarts.
 * </p>
 * <p>
 * Example usage:
 * </p>
 * <code>
 *  LogicalNode node = new LogicalNode();
 *  Context context = new Context();
 * 
 *  node.setName("sequence-generating-logger");
 *  node.setSource(new SequenceGeneratorSource());
 *  node.setSink(new LoggerSink());
 * 
 *  node.start(context);
 * 
 *  ...do other stuff.
 * 
 *  node.stop(context);
 * </code>
 */
public class LogicalNode implements LifecycleAware {

  private static final Logger logger = LoggerFactory
      .getLogger(LogicalNode.class);

  private String name;
  private EventSource source;
  private EventSink sink;

  private ChannelDriverThread driver;

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

    driver = new ChannelDriverThread(name + "-channelDriver");

    driver.setSource(source);
    driver.setSink(sink);

    driver.start();

    while (!driver.getLifecycleState().equals(LifecycleState.START)
        && !driver.getLifecycleState().equals(LifecycleState.ERROR)) {

      logger.debug("Waiting for driver to start");

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger
            .error("Interrupted while waiting for driver to start. Shutting down.");
        lifecycleState = LifecycleState.ERROR;

        driver.setShouldStop(true);

        /*
         * We refuse to return with outstanding resources so we go into a cycle
         * where we interrupt the driver thread and block on it. If we're
         * interrupted again while waiting for it, we warn, but we still refuse
         * to give up on it. Sorry, caller; we don't know what the source or
         * sink is doing so we have to give them time!
         */
        while (driver.isAlive()) {
          logger.debug("Interrupting driver");

          driver.interrupt();

          logger.debug("Waiting for driver to stop");
          try {
            driver.join();
          } catch (InterruptedException e1) {
            logger
                .warn(
                    "Interrupted while waiting for driver to stop. This almost certainly means something awful is happening in the source or sink. Report this error. Interrupting it again!",
                    e1);
          }
        }

        return;
      }
    }

    lifecycleState = driver.getLifecycleState();
  }

  @Override
  public void stop(Context context) throws LifecycleException,
      InterruptedException {

    logger.info("Stopping logical node:{}", this);

    driver.setShouldStop(true);

    while (driver.isAlive()) {
      logger.debug("Waiting for driver to stop");

      try {
        driver.join();
      } catch (InterruptedException e) {
        logger
            .error("Interrupted while waiting for driver to stop. Interrupting it.");
        lifecycleState = LifecycleState.ERROR;

        /*
         * We refuse to return with outstanding resources so we go into a cycle
         * where we interrupt the driver thread and block on it. If we're
         * interrupted again while waiting for it, we warn, but we still refuse
         * to give up on it. Sorry, caller; we don't know what the source or
         * sink is doing so we have to give them time!
         */
        logger.debug("Interrupting driver");
        driver.interrupt();
      }
    }

    /*
     * If we're already in an error state, preserve that, otherwise stop
     * successfully.
     */
    if (!lifecycleState.equals(LifecycleState.ERROR)) {
      lifecycleState = LifecycleState.STOP;
    }
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
