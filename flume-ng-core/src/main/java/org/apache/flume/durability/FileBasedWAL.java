package org.apache.flume.durability;

import java.io.File;
import java.io.IOException;

import org.apache.flume.durability.FileBasedWALManager.FileBasedWALWriter;
import org.apache.flume.formatter.output.EventFormatter;
import org.apache.flume.formatter.output.TextDelimitedOutputFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FileBasedWAL {

  private static final Logger logger = LoggerFactory
      .getLogger(FileBasedWAL.class);

  private File baseDirectory;
  private EventFormatter formatter;

  private File openDirectory;
  private File pendingDirectory;
  private File sentDirectory;
  private File completeDirectory;
  private boolean isInitialized;

  public FileBasedWAL(File baseDirectory) {
    Preconditions.checkNotNull(baseDirectory,
        "WAL base directory may not be null");

    this.baseDirectory = baseDirectory;

    openDirectory = new File(baseDirectory, "open");
    pendingDirectory = new File(baseDirectory, "pending");
    sentDirectory = new File(baseDirectory, "sent");
    completeDirectory = new File(baseDirectory, "complete");

    /* FIXME: This shouldn't be hardcoded (and shouldn't be text!). */
    formatter = new TextDelimitedOutputFormatter();

    isInitialized = false;
  }

  public void initialize() throws IOException {
    logger.info("Initializing file-based WAL at {}", baseDirectory);

    /*
     * NB: We purposefully check pathological (hey, that's a pun!) error cases
     * to improve error messages. Resist the urge to condense these checks.
     * Resist the urge to just use mkdirs(); it could potentially expose
     * sensitive data (i.e. path creation / permission setting races).
     */
    File parentDirectory = baseDirectory.getParentFile();

    Preconditions.checkState(parentDirectory.exists(), "WAL parent directory ("
        + parentDirectory + ") does not exist");
    Preconditions.checkState(parentDirectory.isDirectory(), "WAL parent ("
        + parentDirectory + " ) is not a directory");

    if (!baseDirectory.exists()) {
      if (!baseDirectory.mkdir()) {
        throw new IOException("Unable to create WAL base directory "
            + baseDirectory);
      }
    } else {
      Preconditions.checkState(baseDirectory.isDirectory(),
          "WAL base directory " + baseDirectory
              + " exists but it isn't a directory");
    }

    openDirectory.mkdir();
    Preconditions.checkState(openDirectory.exists(),
        "Directory doesn't exist: %s", openDirectory);

    pendingDirectory.mkdir();
    Preconditions.checkState(pendingDirectory.exists(),
        "Directory doesn't exist: %s", pendingDirectory);

    sentDirectory.mkdir();
    Preconditions.checkState(sentDirectory.exists(),
        "Directory doesn't exist: %s", sentDirectory);

    completeDirectory.mkdir();
    Preconditions.checkState(completeDirectory.exists(),
        "Directory doesn't exist: %s", completeDirectory);
  }

  public FileBasedWALWriter getWriter() throws IOException {
    FileBasedWALWriter writer = new FileBasedWALWriter();

    writer.setFormatter(formatter);

    if (!isInitialized) {
      initialize();
    }

    return writer;
  }

  @Override
  public String toString() {
    return "{ baseDirectory:" + baseDirectory + " openDirectory:"
        + openDirectory + " pendingDirectory:" + pendingDirectory
        + " sentDirectory:" + sentDirectory + " completeDirectory:"
        + completeDirectory + " isInitialized:" + isInitialized + " }";
  }

  public File getBaseDirectory() {
    return baseDirectory;
  }

  public File getOpenDirectory() {
    return openDirectory;
  }

  public File getPendingDirectory() {
    return pendingDirectory;
  }

  public File getSentDirectory() {
    return sentDirectory;
  }

  public File getCompleteDirectory() {
    return completeDirectory;
  }

}
