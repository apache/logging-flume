package org.apache.wal.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import org.apache.wal.WAL;
import org.apache.wal.WALReader;
import org.apache.wal.WALWriter;

import com.google.common.io.Closeables;
import com.google.common.io.Files;

public class AvroWAL implements WAL {

  private File directory;

  private RandomAccessFile walRAF;

  private MappedByteBuffer indexBuffer;

  @Override
  public void configure(Map<String, String> properties) {
    // TODO Auto-generated method stub

  }

  @Override
  public void open() {
    try {
      indexBuffer = (MappedByteBuffer) Files.map(new File(directory, "index"),
          FileChannel.MapMode.READ_WRITE, 8);

      walRAF = new RandomAccessFile(new File(directory, "data.wal"), "rwd");
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    Closeables.closeQuietly(walRAF);
  }

  @Override
  public WALReader getReader() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public WALWriter getWriter() {
    // TODO Auto-generated method stub
    return null;
  }

}
