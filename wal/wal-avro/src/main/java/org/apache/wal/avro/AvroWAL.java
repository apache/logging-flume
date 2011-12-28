package org.apache.wal.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.wal.WAL;
import org.apache.wal.WALException;
import org.apache.wal.WALReader;
import org.apache.wal.WALWriter;

public class AvroWAL implements WAL {

  private File directory;

  private WALIndex index;

  @Override
  public void configure(Map<String, String> properties) {
    index = new WALIndex();

    directory = new File(properties.get("directory"));

    index.setDirectory(directory);
  }

  @Override
  public void open() {
    try {
      index.open();
    } catch (FileNotFoundException e) {
      throw new WALException("Unable to open WAL index. Exception follows.", e);
    } catch (IOException e) {
      throw new WALException("Unable to open WAL index. Exception follows.", e);
    }
  }

  @Override
  public void close() {
  }

  @Override
  public WALReader getReader() {
    AvroWALReader reader = new AvroWALReader();

    reader.setDirectory(directory);
    reader.setIndex(index);

    return reader;
  }

  @Override
  public WALWriter getWriter() {
    AvroWALWriter writer = new AvroWALWriter();

    writer.setDirectory(directory);
    writer.setIndex(index);

    return writer;
  }

}
