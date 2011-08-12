package org.apache.flume;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
public class ChannelDriverThread extends Thread {

  private static final Logger logger = LoggerFactory
      .getLogger(ChannelDriverThread.class);

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
     * try / catch blocks so we can provide slightly better error messaging and
     * recovery. Please resist the urge to combine them. The ordering of sink
     * open, source open, source close, sink close is deliberate as well.
     */

    try {
      sink.open(context);
    } catch (InterruptedException e) {
      logger.error("Interrupted while opening sink. Exception follows.", e);
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
      } catch (EventDeliveryException e) {
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

    logger.debug("Channel driver thread exiting with state:{}", lifecycleState);
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