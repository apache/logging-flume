package org.apache.flume.durability.file;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.durability.WALWriter;
import org.apache.flume.formatter.output.EventFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FileBasedWALWriter implements WALWriter {

  private static final Logger logger = LoggerFactory
      .getLogger(FileBasedWALWriter.class);
  private static final long defaultRollInterval = 30000;

  private File directory;
  private EventFormatter formatter;
  private volatile long rollInterval;

  private File currentFile;
  private BufferedOutputStream outputStream;
  private volatile long openTime;
  private volatile boolean shouldRoll;

  public FileBasedWALWriter() {
    shouldRoll = false;
    rollInterval = defaultRollInterval;
  }

  @Override
  public void open() throws IOException {
    Preconditions.checkState(directory != null,
        "Directory must be configured prior to opening.");

    logger.debug("Opening WAL {}", directory);

    long now = System.currentTimeMillis();

    currentFile = new File(directory, String.valueOf(now) + "-"
        + Thread.currentThread().getId());
    outputStream = new BufferedOutputStream(new FileOutputStream(currentFile));
    openTime = now;
  }

  @Override
  public void write(Event event) throws IOException {
    if (shouldRoll) {
      close();
      shouldRoll = false;
    }

    if (outputStream == null) {
      open();
    }

    outputStream.write(formatter.format(event));
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
    outputStream = null;
  }

  @Override
  public void flush() throws IOException {
    Preconditions.checkState(outputStream != null,
        "Attempt to flush an unopen WAL: %s", currentFile);

    outputStream.flush();
  }

  public File getFile() {
    return directory;
  }

  public void setFile(File file) {
    this.directory = file;
  }

  public EventFormatter getFormatter() {
    return formatter;
  }

  public void setFormatter(EventFormatter formatter) {
    this.formatter = formatter;
  }

  public class RollCheckRunnable implements Runnable {

    @Override
    public void run() {
      long now = System.currentTimeMillis();

      if (openTime + rollInterval >= now) {
        logger.debug("Marking time to roll");

        shouldRoll = true;
      }
    }
  }

}
