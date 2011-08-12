package org.apache.flume.core;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ChannelDriver implements LifecycleAware {

  private static final Logger logger = LoggerFactory
      .getLogger(ChannelDriver.class);

  private String name;
  private EventSource source;
  private EventSink sink;
  private ChannelDriverThread driverThread;

  private LifecycleState lifecycleState;

  public ChannelDriver(String name) {
    this.name = name;

    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void start(Context context) throws LifecycleException {
    logger.debug("Channel driver starting:{}", this);

    driverThread = new ChannelDriverThread(name);

    driverThread.setSource(source);
    driverThread.setSink(sink);

    lifecycleState = LifecycleState.START;

    driverThread.start();
  }

  @Override
  public void stop(Context context) throws LifecycleException {
    logger.debug("Channel driver stopping:{}", this);

    driverThread.setShouldStop(true);

    while (driverThread.isAlive()) {
      try {
        logger.debug("Waiting for driver to stop");

        driverThread.join(1000);
      } catch (InterruptedException e) {
        logger
            .debug("Interrupted while waiting for driver thread to shutdown. Interrupting it.");
        driverThread.interrupt();
      }
    }

    /*
     * FIXME: We repurpose LifecycleState for the driver thread, but we don't
     * actually use all phases of the lifecycle because we don't have a hook to
     * know when the thread has successfully stopped. This means we treat a
     * START state to mean successful exit and ERROR to mean something bad.
     * You've been warned.
     */
    LifecycleState driverThreadResult = driverThread.getLifecycleState();

    if (!driverThreadResult.equals(LifecycleState.START)
        || driverThread.getLastException() != null) {
      lifecycleState = LifecycleState.ERROR;
    } else {
      lifecycleState = LifecycleState.STOP;
    }
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  @Override
  public void transitionTo(LifecycleState state) {
  }

  @Override
  public String toString() {
    return "{ source:" + source + " sink:" + sink + " }";
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

  private static class ChannelDriverThread extends Thread {

    private EventSource source;
    private EventSink sink;
    private Context context;

    private LifecycleState lifecycleState;
    private Exception lastException;

    private long totalEvents;
    private long discardedEvents;
    private long nullEvents;
    private long successfulEvents;

    volatile private boolean shouldStop;

    public ChannelDriverThread(String name) {
      super(name);

      totalEvents = 0;
      discardedEvents = 0;
      nullEvents = 0;
      successfulEvents = 0;

      lifecycleState = LifecycleState.IDLE;
      shouldStop = false;
    }

    @Override
    public void run() {
      logger.debug("Channel driver thread running");

      Preconditions.checkState(source != null, "Source can not be null");
      Preconditions.checkState(sink != null, "Sink can not be null");

      lifecycleState = LifecycleState.START;

      try {
        sink.open(context);
      } catch (InterruptedException e) {
        logger.debug("Interrupted while opening sink. Exception follows.", e);
        lastException = e;
        lifecycleState = LifecycleState.ERROR;
        shouldStop = true;
        return;
      } catch (LifecycleException e) {
        logger.error("Failed to open sink. Exception follows.", e);
        lastException = e;
        lifecycleState = LifecycleState.ERROR;
        shouldStop = true;
        return;
      }

      try {
        source.open(context);
      } catch (InterruptedException e) {
        logger.debug("Interrupted while opening source. Exception follows.", e);
        lastException = e;
        lifecycleState = LifecycleState.ERROR;
        shouldStop = true;

        /* FIXME: This is gross. Factor this out. */
        try {
          sink.close(context);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        } catch (LifecycleException e1) {
          logger
              .error(
                  "While cleaning up after \"{}\" failed to close the sink down - {}",
                  e.getMessage(), e.toString());
        }

        return;
      } catch (LifecycleException e) {
        logger.error("Failed to open source. Exception follows.", e);
        lastException = e;
        lifecycleState = LifecycleState.ERROR;
        shouldStop = true;

        try {
          sink.close(context);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        } catch (LifecycleException e1) {
          logger
              .error(
                  "While cleaning up after \"{}\" failed to close the sink down - {}",
                  e.getMessage(), e.toString());
        }

        return;
      }

      while (!shouldStop) {
        Event<?> event = null;

        try {
          event = source.next(context);

          if (event != null) {
            sink.append(context, event);
            successfulEvents++;
          } else {
            nullEvents++;
          }
        } catch (InterruptedException e) {
          logger.debug("Received an interrupt while moving events - stopping");
          lastException = e;
          lifecycleState = LifecycleState.ERROR;
          shouldStop = true;
        } catch (MessageDeliveryException e) {
          logger.debug("Unable to deliver event:{} (may be null) - Reason:{}",
              event, e.getMessage());
          discardedEvents++;
          /* FIXME: Handle dead messages. */
        }

        totalEvents++;
      }

      try {
        source.close(context);
      } catch (InterruptedException e) {
        logger.debug("Interrupted while closing source. Exception follows.", e);
        lastException = e;
        lifecycleState = LifecycleState.ERROR;
      } catch (LifecycleException e) {
        logger.error("Failed to close source. Exception follows.", e);
        lastException = e;
        lifecycleState = LifecycleState.ERROR;
      }

      try {
        sink.close(context);
      } catch (InterruptedException e) {
        logger.debug("Interrupted while closing sink. Exception follows.", e);
        lastException = e;
        lifecycleState = LifecycleState.ERROR;
      } catch (LifecycleException e) {
        logger.error("Failed to close sink. Exception follows.", e);
        lastException = e;
        lifecycleState = LifecycleState.ERROR;
      }

      logger.debug("Channel driver thread exiting with state:{}",
          lifecycleState);
      logger
          .info(
              "Logical node ended. Event metrics - totalEvents:{} successfulEvents:{} nullEvents:{} discardedEvents:{}",
              new Object[] { totalEvents, successfulEvents, nullEvents,
                  discardedEvents });
    }

    public void setSource(EventSource source) {
      this.source = source;
    }

    public void setSink(EventSink sink) {
      this.sink = sink;
    }

    public void setShouldStop(boolean shouldStop) {
      this.shouldStop = shouldStop;
    }

    public LifecycleState getLifecycleState() {
      return lifecycleState;
    }

    public Exception getLastException() {
      return lastException;
    }

  }

}
