package org.apache.flume.durability.file;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.durability.WALWriter;
import org.apache.flume.formatter.output.EventFormatter;

public class FileBasedWALWriter implements WALWriter {

  private File file;
  private BufferedOutputStream outputStream;
  private EventFormatter formatter;

  @Override
  public void open() throws FileNotFoundException {
    outputStream = new BufferedOutputStream(new FileOutputStream(file));
  }

  @Override
  public void write(Event event) throws IOException {
    outputStream.write(formatter.format(event));
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }

  @Override
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
