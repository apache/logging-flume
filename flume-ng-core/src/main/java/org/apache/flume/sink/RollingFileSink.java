package org.apache.flume.sink;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.EventFormatter;
import org.apache.flume.formatter.output.PathManager;
import org.apache.flume.formatter.output.TextDelimitedOutputFormatter;
import org.apache.flume.lifecycle.LifecycleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class RollingFileSink extends AbstractEventSink implements Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(RollingFileSink.class);
  private static final long defaultRollInterval = 30;

  private File directory;
  private long rollInterval;
  private OutputStream outputStream;
  private ScheduledExecutorService rollService;

  private CounterGroup counterGroup;

  private PathManager pathController;
  private EventFormatter formatter;
  private volatile boolean shouldRotate;

  public RollingFileSink() {
    formatter = new TextDelimitedOutputFormatter();
    counterGroup = new CounterGroup();
    pathController = new PathManager();
    shouldRotate = false;
  }

  @Override
  public void configure(Context context) {
    File directory = context.get("sink.directory", File.class);
    Long rollInterval = context.get("sink.rollInterval", Long.class);

    Preconditions.checkArgument(directory != null, "Directory may not be null");

    if (rollInterval == null) {
      rollInterval = defaultRollInterval;
    }

    this.rollInterval = rollInterval;
    this.directory = directory;
  }

  @Override
  public void open(Context context) throws InterruptedException,
      LifecycleException {

    super.open(context);

    pathController.setBaseDirectory(directory);

    rollService = Executors.newScheduledThreadPool(
        1,
        new ThreadFactoryBuilder().setNameFormat(
            "rollingFileSink-roller-" + Thread.currentThread().getId() + "-%d")
            .build());

    /*
     * Every N seconds, mark that it's time to rotate. We purposefully do NOT
     * touch anything other than the indicator flag to avoid error handling
     * issues (e.g. IO exceptions occuring in two different threads. Resist the
     * urge to actually perform rotation in a separate thread!
     */
    rollService.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        logger.debug("Marking time to rotate file {}",
            pathController.getCurrentFile());
        shouldRotate = true;
      }

    }, rollInterval, rollInterval, TimeUnit.SECONDS);
  }

  @Override
  public void append(Context context, Event event) throws InterruptedException,
      EventDeliveryException {

    if (shouldRotate) {
      logger.debug("Time to rotate {}", pathController.getCurrentFile());

      if (outputStream != null) {
        logger.debug("Closing file {}", pathController.getCurrentFile());

        try {
          outputStream.flush();
          outputStream.close();
          shouldRotate = false;
        } catch (IOException e) {
          throw new EventDeliveryException("Unable to rotate file "
              + pathController.getCurrentFile() + " while delivering event", e);
        }

        outputStream = null;
        pathController.rotate();
      }
    }

    if (outputStream == null) {
      try {
        logger.debug("Opening output stream for file {}",
            pathController.getCurrentFile());

        outputStream = new BufferedOutputStream(new FileOutputStream(
            pathController.getCurrentFile()));
      } catch (IOException e) {
        throw new EventDeliveryException("Failed to open file "
            + pathController.getCurrentFile() + " while delivering event", e);
      }
    }

    try {
      byte[] bytes = formatter.format(event);

      /*
       * FIXME: Feature: Rotate on size and time by checking bytes written and
       * setting shouldRotate = true if we're past a threshold.
       */
      counterGroup.addAndGet("sink.bytesWritten", (long) bytes.length);

      outputStream.write(bytes);

      /*
       * FIXME: Feature: Control flush interval based on time or number of
       * events. For now, we're super-conservative and flush on each write.
       */
      outputStream.flush();
    } catch (IOException e) {
      throw new EventDeliveryException("Failed to write event:" + event, e);
    }
  }

  @Override
  public void close(Context context) throws InterruptedException,
      LifecycleException {

    super.close(context);

    if (outputStream != null) {
      logger.debug("Closing file {}", pathController.getCurrentFile());

      try {
        outputStream.flush();
        outputStream.close();
      } catch (IOException e) {
        throw new LifecycleException("Unable to close output stream", e);
      }
    }

    rollService.shutdown();

    while (!rollService.isTerminated()) {
      rollService.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }

  public long getRollInterval() {
    return rollInterval;
  }

  public void setRollInterval(long rollInterval) {
    this.rollInterval = rollInterval;
  }

  public EventFormatter getFormatter() {
    return formatter;
  }

  public void setFormatter(EventFormatter formatter) {
    this.formatter = formatter;
  }

}
