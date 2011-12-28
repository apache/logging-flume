package org.apache.wal.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.wal.WALEntry;
import org.apache.wal.WALException;
import org.apache.wal.WALWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;

public class AvroWALWriter implements WALWriter {

  private static final Logger logger = LoggerFactory
      .getLogger(AvroWALWriter.class);
  private static final int defaultEventLimit = 100;

  private File directory;
  private int eventLimit;

  private FileOutputStream walOutputStream;
  private Encoder encoder;
  private SpecificDatumWriter<AvroWALEntry> writer;

  private WALIndex index;

  private int eventCount;
  private int eventBatchCount;
  private File currentFile;
  private long currentPosition;
  private FileChannel outputChannel;

  public AvroWALWriter() {
    eventLimit = defaultEventLimit;
    eventCount = 0;
    eventBatchCount = 0;
  }

  @Override
  public void open() {
    logger.info("Opening write ahead log in:{}", directory);

    openIndex();
    openWALFile();

    writer = new SpecificDatumWriter<AvroWALEntry>(AvroWALEntry.class);
  }

  private void openIndex() {
    logger.info("Opening write ahead log index in directory:{}", directory);

    index = new WALIndex();

    index.setDirectory(directory);

    try {
      index.open();
    } catch (FileNotFoundException e) {
      throw new WALException("Failed to open WAL index. Exception follows.", e);
    } catch (IOException e) {
      throw new WALException("Failed to open WAL index. Exception follows.", e);
    }

    logger.debug("Opened write ahead log index:{}", index);
  }

  private void openWALFile() {
    logger.info("Opening WAL file");

    currentFile = new File(directory, System.currentTimeMillis() + ".wal");

    try {
      walOutputStream = new FileOutputStream(currentFile, true);
      outputChannel = walOutputStream.getChannel();
      currentPosition = outputChannel.position();
      encoder = EncoderFactory.get().directBinaryEncoder(walOutputStream, null);

      index.updateIndex(currentFile.getPath(), 0);
    } catch (FileNotFoundException e) {
      throw new WALException(
          "Failed to open WAL (missing parent directory?). Exception follows.",
          e);
    } catch (IOException e) {
      throw new WALException("Failed to open WAL. Exception follows.", e);
    }

    logger.debug("Opened write ahead log:{} currentPosition:{}", currentFile,
        currentPosition);
  }

  private void closeWALFile() {
    Closeables.closeQuietly(outputChannel);
    Closeables.closeQuietly(walOutputStream);
  }

  @Override
  public void write(WALEntry entry) {
    Preconditions.checkArgument(entry instanceof AvroWALEntryAdapter,
        "WAL entry must be an instance of AvroWALEntryAdapter");

    try {
      writer.write(((AvroWALEntryAdapter) entry).getEntry(), encoder);
      encoder.flush();
      outputChannel.force(true);
      currentPosition = outputChannel.position();
      eventBatchCount++;

      if (logger.isDebugEnabled()) {
        logger.debug("Wrote entry:{} markPosition:{} currentPosition:{}",
            new Object[] { entry, index.getPosition(), currentPosition });
      }
    } catch (IOException e) {
      throw new WALException("Failed to write WAL entry. Exception follows.", e);
    }
  }

  @Override
  public void close() {
    logger.info("Closing write ahead log at:{}", currentFile);

    Closeables.closeQuietly(outputChannel);
  }

  @Override
  public void mark() {
    logger.debug("Marking currentFile:{} currentPosition:{}", currentFile,
        currentPosition);

    index.updateIndex(currentFile.getPath(), currentPosition);

    eventCount += eventBatchCount;
    eventBatchCount = 0;

    /*
     * Checkpoint logic:
     * 
     * Since we flush the log on each write and we just updated the index table,
     * we can simply close / open the WAL and reset our counters.
     */
    if (eventCount >= eventLimit) {
      logger.info("Checkpoint write ahead log:{}", this);

      closeWALFile();
      openWALFile();

      eventCount = 0;

      logger.debug("Checkpoint finished");
    }
  }

  @Override
  public void reset() {
    logger.debug("Resetting WAL position from:{} to:{}", currentPosition,
        index.getPosition());

    try {
      outputChannel.truncate(index.getPosition());
      /*
       * Changes to the file size affect the metadata so we need to force that
       * out as well and pay the price of the second IO.
       */
      outputChannel.force(true);
    } catch (IOException e) {
      throw new WALException(
          "Unable to reset WAL to last committed position. Exception follows.",
          e);
    }
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }

  public long getCurrentPosition() {
    return currentPosition;
  }

  public File getCurrentFile() {
    return currentFile;
  }

  public long getMarkPosition() {
    return index.getPosition();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass()).add("currentFile", currentFile)
        .add("currentPosition", currentPosition).add("index", index)
        .add("eventCount", eventCount).add("eventBatchCount", eventBatchCount)
        .add("eventLimit", eventLimit).add("directory", directory).toString();
  }
}
