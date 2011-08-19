package org.apache.flume.formatter.output;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

public class PathManager {

  private long seriesTimestamp;
  private File baseDirectory;
  private AtomicInteger fileIndex;

  private File currentFile;

  public PathManager() {
    seriesTimestamp = System.currentTimeMillis();
    fileIndex = new AtomicInteger();
  }

  public File nextFile() {
    currentFile = new File(baseDirectory, seriesTimestamp + "-"
        + fileIndex.incrementAndGet());

    return currentFile;
  }

  public File getCurrentFile() {
    if (currentFile == null) {
      return nextFile();
    }

    return currentFile;
  }

  public void rotate() {
    currentFile = null;
  }

  public File getBaseDirectory() {
    return baseDirectory;
  }

  public void setBaseDirectory(File baseDirectory) {
    this.baseDirectory = baseDirectory;
  }

  public long getSeriesTimestamp() {
    return seriesTimestamp;
  }

  public AtomicInteger getFileIndex() {
    return fileIndex;
  }

}
