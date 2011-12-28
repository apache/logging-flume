package org.apache.wal.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class WALIndex {

  private static final String indexFileName = "wal-idx";
  private static final Logger logger = LoggerFactory.getLogger(WALIndex.class);

  private File directory;

  private File indexFile;
  private MappedByteBuffer indexBuffer;

  public synchronized void open() throws FileNotFoundException, IOException {
    indexFile = new File(directory, indexFileName);

    indexBuffer = Files.map(indexFile, FileChannel.MapMode.READ_WRITE,
        16 * 1024);
  }

  public synchronized void updateIndex(String file, long position) {
    logger.info("Updating WAL index with file:{} position:{}", file, position);

    indexBuffer.putLong(position).putInt(file.length()).put(file.getBytes());
    indexBuffer.force();
    indexBuffer.position(0);
  }

  public synchronized File getDirectory() {
    return directory;
  }

  public synchronized void setDirectory(File directory) {
    this.directory = directory;
  }

  public synchronized File getIndexFile() {
    return indexFile;
  }

}
