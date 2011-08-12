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

    driverThread.start();

    lifecycleState = LifecycleState.START;
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
            .debug("Interrupted while waiting for driver thread to shutdown. Interrupting it and stopping.");
        driverThread.interrupt();
      }
    }

    lifecycleState = LifecycleState.STOP;
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

      shouldStop = false;
    }

    @Override
    public void run() {
      logger.debug("Channel driver thread running");

      Preconditions.checkState(source != null, "Source can not be null");
      Preconditions.checkState(sink != null, "Sink can not be null");

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
          shouldStop = true;
        } catch (MessageDeliveryException e) {
          logger.debug("Unable to deliver event:{} (may be null)", event);
          discardedEvents++;
          /* FIXME: Handle dead messages. */
        }

        totalEvents++;
      }

      logger.debug("Channel driver thread exiting cleanly");
      logger
          .info(
              "Event metrics - totalEvents:{} successfulEvents:{} nullEvents:{} discardedEvents:{}",
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

  }

}
