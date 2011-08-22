package org.apache.flume.durability;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.formatter.output.EventFormatter;

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

  public static class FileBasedWALWriter {

    private File file;
    private BufferedOutputStream outputStream;
    private EventFormatter formatter;

    public void open() throws FileNotFoundException {
      outputStream = new BufferedOutputStream(new FileOutputStream(file));
    }

    public void write(Event event) throws IOException {
      outputStream.write(formatter.format(event));
    }

    public void close() throws IOException {
      outputStream.close();
    }

    public void flush() throws IOException {
      outputStream.flush();
    }

    public File getFile() {
      return file;
    }

    public void setFile(File file) {
      this.file = file;
    }

    public EventFormatter getFormatter() {
      return formatter;
    }

    public void setFormatter(EventFormatter formatter) {
      this.formatter = formatter;
    }

  }

}
