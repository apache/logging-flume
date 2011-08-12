package org.apache.flume.core;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A channel driver is responsible for managing the core lifecycle of sources
 * and sinks as well as the shuffle loop.
 * </p>
 * <p>
 * The intended usage is to supply a channel driver with a name - normally
 * containing the name of the LogicalNode to which it belongs - a source, and a
 * sink, and ask it to run. It implements {@link LifecycleAware} which is the
 * only interface for execution.
 * </p>
 * <p>
 * There's no good reason for anything other than a LogicalNode using this
 * class. This is an internal system.
 * </p>
 */
public class ChannelDriver implements LifecycleAware {

  private static final Logger logger = LoggerFactory
      .getLogger(ChannelDriver.class);

  private String name;
  private EventSource source;
  private EventSink sink;
  private ChannelDriverThread driverThread;

  private LifecycleState lifecycleState;
  private Exception lastException;

  public ChannelDriver(String name) {
    Preconditions.checkNotNull(name);

    this.name = name;

    lifecycleState = LifecycleState.IDLE;
  }

  /**
   * <p>
   * Start the channel driver.
   * </p>
   * <p>
   * This method blocks on starting the source and sink. Should it receive an
   * interruption, it attempts to safely close down the source and sink (or the
   * shuffle loop if it's already running) and may continue to block for a short
   * time after interruption because of this.
   * </p>
   */
  @Override
  public void start(Context context) throws LifecycleException,
      InterruptedException {

    logger.debug("Channel driver starting:{}", this);

    driverThread = new ChannelDriverThread(name);

    driverThread.setSource(source);
    driverThread.setSink(sink);

    driverThread.start();

    /*
     * FIXME: We can't use LifecycleController because the driver thread isn't
     * technically LifecycleAware.
     */
    while (!driverThread.getLifecycleState().equals(LifecycleState.START)
        && !driverThread.getLifecycleState().equals(LifecycleState.ERROR)) {

      logger.debug("Waiting for driver thread to start");

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger
            .error(
                "Interrupted while waiting for the driver thread to start. Likely a code error. Please report this to the developers. Exception follow.",
                e);

        lifecycleState = LifecycleState.ERROR;

        driverThread.setShouldStop(true);
        driverThread.join();

        return;
      }
    }

    lifecycleState = LifecycleState.START;
  }

  /**
   * <p>
   * Stop the channel driver and the underlying source and sink. Halts the
   * shuffle loop if it's running.
   * </p>
   * <p>
   * If there were any errors during open, close, or the shuffle loop, the
   * lifecycle state will be set to {code ERROR} and last exception <b>MAY</b>
   * be set.
   * </p>
   * <p>
   * This method blocks on shutting down the source and sink. Upon interruption,
   * it interrupts its underlying resources but still waits for everything to
   * exit cleanly.
   * </p>
   */
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
      lastException = driverThread.getLastException();
    } else {
      lifecycleState = LifecycleState.STOP;
    }
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
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

  public Exception getLastException() {
    return lastException;
  }

  /**
   * <p>
   * The thread that does the actual work of opening, closing, and running the
   * shuffle loop (i.e. the loop that takes {@link Event}s from the
   * {@link EventSource} and gives them to the {@link EventSink}.
   * </p>
   * <p>
   * Because this class extends {@link Thread} it doesn't fully implement the
   * {@link LifecycleAware} interface. That said, it does use
   * {@link LifecycleState} (or at least some of the states from it) to indicate
   * its current status. This is kind of gross and should be revisited. Ideally,
   * this would become a Runnable so we could decouple thread lifetime from
   * instances.
   * </p>
   * <p>
   * This class is not meant for any users other than {@link ChannelDriver}
   * itself.
   * </p>
   */
  private static class ChannelDriverThread extends Thread {

    private EventSource source;
    private EventSink sink;
    private Context context;

    volatile private LifecycleState lifecycleState;
    volatile private Exception lastException;

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

      /*
       * Developer note: We purposefully separate source and sink open and close
       * try / catch blocks so we can provide slightly better error messaging
       * and recovery. Please resist the urge to combine them. The ordering of
       * sink open, source open, source close, sink close is deliberate as well.
       */

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
          logger
              .error("Interrupted while trying to close the sink (because we were interrupted while trying to open the source)");
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
          logger
              .error("Interrupted while trying to close the sink (because we were cleaning up from a lifecycle exception from opening the source.)");
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
