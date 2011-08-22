package org.apache.flume.durability.file;

import java.io.File;

import org.apache.flume.durability.WALManager;

public class FileBasedWALManager implements WALManager {

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
