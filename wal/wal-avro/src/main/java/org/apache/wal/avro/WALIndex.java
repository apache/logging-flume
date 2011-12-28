package org.apache.wal.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.io.Files;

public class WALIndex {

  private static final String indexFileName = "wal-idx";
  private static final Logger logger = LoggerFactory.getLogger(WALIndex.class);

  private File directory;

  private String file;
  private long position;

  private File indexFile;
  private MappedByteBuffer indexBuffer;

  public synchronized void open() throws FileNotFoundException, IOException {
    indexFile = new File(directory, indexFileName);

    logger.info("Opening WAL index table file:{}", indexFile);

    indexBuffer = Files.map(indexFile, FileChannel.MapMode.READ_WRITE,
        16 * 1024);

    position = indexBuffer.getLong();
    int fileNameLength = indexBuffer.getInt();

    if (fileNameLength > 0) {
      byte[] buffer = new byte[fileNameLength];
      indexBuffer.get(buffer);
      file = new String(buffer);
    }

    logger.debug("Loaded position:{} fileNameLength:{} file:{}", new Object[] {
        position, fileNameLength, file });

    indexBuffer.position(0);
  }

  public synchronized void updateIndex(String file, long position) {
    indexBuffer.putLong(position).putInt(file.length()).put(file.getBytes());
    indexBuffer.force();
    indexBuffer.position(0);

    this.file = file;
    this.position = position;
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

  public String getFile() {
    return file;
  }

  public void setFile(String file) {
    this.file = file;
  }

  public long getPosition() {
    return position;
  }

  public void setPosition(long position) {
    this.position = position;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass()).add("file", file)
        .add("position", position).add("directory", directory)
        .add("indexFile", indexFile).toString();
  }
}
