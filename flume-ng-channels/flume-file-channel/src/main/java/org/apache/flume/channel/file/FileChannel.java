package org.apache.flume.channel.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A durable {@link Channel} implementation that uses the local file system for
 * its storage.
 * </p>
 */
public class FileChannel implements Channel {

  private static final Logger logger = LoggerFactory
      .getLogger(FileChannel.class);

  private static ThreadLocal<FileBackedTransaction> currentTransaction = new ThreadLocal<FileBackedTransaction>();

  private File directory;

  private File openDirectory;
  private File completeDirectory;
  private boolean isInitialized;

  private File currentOutputFile;
  private boolean shouldRotate;

  private void initialize() {
    Preconditions.checkState(directory != null, "Directory must not be null");
    Preconditions.checkState(directory.getParentFile().exists(),
        "Directory %s must exist", directory.getParentFile());

    logger.info("Initializing file channel directory:{}", directory);

    openDirectory = new File(directory, "open");
    completeDirectory = new File(directory, "complete");

    if (!openDirectory.mkdirs()) {
      throw new ChannelException("Unable to create open file directory:"
          + openDirectory);
    }

    if (!completeDirectory.mkdirs()) {
      throw new ChannelException("Unable to create complete file directory:"
          + completeDirectory);
    }

    shouldRotate = false;
    isInitialized = true;
  }

  @Override
  public void put(Event event) throws ChannelException {
    Preconditions.checkState(currentTransaction.get() != null,
        "No transaction currently in progress");

    FileBackedTransaction tx = currentTransaction.get();

    tx.events.add(event);
  }

  @Override
  public Event take() throws ChannelException {
    return null;
  }

  @Override
  public synchronized Transaction getTransaction() {
    if (!isInitialized) {
      /* This is a catch-all to ensure we initialize file system storage once. */
      initialize();
    }

    FileBackedTransaction tx = currentTransaction.get();

    if (shouldRotate) {
      currentOutputFile = null;
    }

    /*
     * If there's no current transaction (which is stored in a threadlocal) OR
     * its current state is CLOSED - which indicates the transaction is in a
     * final state and unusable - we create a new transaction with the current
     * output file and set the thread-local transaction holder to it.
     */
    if (tx == null || tx.state.equals(FileBackedTransaction.State.CLOSED)) {
      FileBackedTransaction transaction = new FileBackedTransaction();

      if (currentOutputFile == null) {
        currentOutputFile = new File(openDirectory, Thread.currentThread()
            .getId() + "-" + System.currentTimeMillis());

        logger.debug("Using new output file:{}", currentOutputFile);
      }

      transaction.currentOutputFile = currentOutputFile;

      currentTransaction.set(transaction);

      logger.debug("Created transaction:{} for channel:{}", transaction, this);
    }

    return currentTransaction.get();
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }

  public File getOpenDirectory() {
    return openDirectory;
  }

  public File getCompleteDirectory() {
    return completeDirectory;
  }

  public boolean isInitialized() {
    return isInitialized;
  }

  /**
   * <p>
   * An implementation of {@link Transaction} for {@link FileChannel}s.
   * </p>
   */
  public static class FileBackedTransaction implements Transaction {

    private List<Event> events;

    private File currentOutputFile;

    private FileOutputStream outputStream;
    private FileInputStream inputStream;

    private State state;

    public FileBackedTransaction() {
      events = new LinkedList<Event>();
      state = State.NEW;
    }

    @Override
    public void begin() {
      if (state.equals(State.CLOSED)) {
        throw new IllegalStateException(
            "Illegal to begin a transaction with state:" + state);
      }

      logger.debug("Beginning a new transaction");

      try {
        outputStream = new FileOutputStream(currentOutputFile, true);
      } catch (FileNotFoundException e) {
        throw new ChannelException("Unable to open new output file:"
            + currentOutputFile, e);
      }

      state = State.OPEN;
    }

    @Override
    public void commit() {
      Preconditions.checkState(state.equals(State.OPEN),
          "Attempt to commit a transaction that isn't open (state:" + state
              + ")");

      logger.debug("Commiting current transaction (size:{})", events.size());

      try {
        for (Event event : events) {
          // TODO: Serialize event properly (avro?)
          outputStream.write((event.toString() + "\n").getBytes());
          outputStream.flush();
        }

        // TODO: Write checksum.
        outputStream.write("---\n".getBytes());

        events.clear();
      } catch (IOException e) {
        throw new ChannelException("Unable to write to output file", e);
      }

      state = State.COMPLETED;
    }

    @Override
    public void rollback() {
      Preconditions.checkState(state.equals(State.OPEN),
          "Attempt to rollback a transaction that isn't open (state:" + state
              + ")");

      logger.debug("Rolling back current transaction (size:{})", events.size());

      events.clear();

      state = State.COMPLETED;
    }

    @Override
    public void close() {
      Preconditions
          .checkState(
              state.equals(State.COMPLETED),
              "Attempt to close a transaction that isn't completed - you must either commit or rollback (state:"
                  + state + ")");

      logger.debug("Closing current transaction");

      try {
        outputStream.close();
      } catch (IOException e) {
        throw new ChannelException("Unable to close current output file", e);
      }

      state = State.CLOSED;
    }

    /**
     * <p>
     * The state of the {@link Transaction} to which it belongs.
     * </p>
     * <dl>
     * <dt>NEW</dt>
     * <dd>A newly created transaction that has not yet begun.</dd>
     * <dt>OPEN</dt>
     * <dd>A transaction that is open. It is permissible to commit or rollback.</dd>
     * <dt>COMPLETED</dt>
     * <dd>This transaction has been committed or rolled back. It is illegal to
     * perform any further operations beyond closing it.</dd>
     * <dt>CLOSED</dt>
     * <dd>A closed transaction. No further operations are permitted.</dd>
     */
    private static enum State {
      NEW, OPEN, COMPLETED, CLOSED
    }

  }

}
