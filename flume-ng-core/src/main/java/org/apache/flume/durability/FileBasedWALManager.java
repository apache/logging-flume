package org.apache.flume.durability;

import java.io.File;

public class FileBasedWALManager {

  private File directory;

  public FileBasedWAL getWAL(String name) {
    File walDirectory = new File(directory, name);
    FileBasedWAL wal = new FileBasedWAL(walDirectory);

    return wal;
  }

  @Override
  public String toString() {
    return "{ directory:" + directory + " }";
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }

}
