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

  private static final String writeIndexFileName = "write.idx";
  private static final String readIndexFileName = "read.idx";

  private static final Logger logger = LoggerFactory.getLogger(WALIndex.class);

  private File directory;

  private File writeIndexFile;
  private MappedByteBuffer writeIndexBuffer;
  private File readIndexFile;
  private MappedByteBuffer readIndexBuffer;

  private String writeFile;
  private long writePosition;
  private String readFile;
  private long readPosition;

  public WALIndex() {
  }

  public synchronized void open() throws FileNotFoundException, IOException {
    writeIndexFile = new File(directory, writeIndexFileName);
    readIndexFile = new File(directory, readIndexFileName);

    logger.info("Opening WAL index table writeFile:{}", writeIndexFile);

    writeIndexBuffer = Files.map(writeIndexFile,
        FileChannel.MapMode.READ_WRITE, 4 * 1024);
    readIndexBuffer = Files.map(readIndexFile, FileChannel.MapMode.READ_WRITE,
        4 * 1024);

    writePosition = writeIndexBuffer.getLong();
    int writeFileNameLength = writeIndexBuffer.getInt();

    if (writeFileNameLength > 0) {
      byte[] buffer = new byte[writeFileNameLength];
      writeIndexBuffer.get(buffer);
      writeFile = new String(buffer);
    }

    writeIndexBuffer.position(0);

    readPosition = readIndexBuffer.getLong();
    int readFileNameLength = readIndexBuffer.getInt();

    if (readFileNameLength > 0) {
      byte[] buffer = new byte[readFileNameLength];
      readIndexBuffer.get(buffer);
      readFile = new String(buffer);
    }

    readIndexBuffer.position(0);

    logger.debug("Loaded index:{}", this);
  }

  public synchronized void updateWriteIndex(String file, long position) {
    writeIndexBuffer.putLong(position).putInt(file.length())
        .put(file.getBytes());
    writeIndexBuffer.force();
    writeIndexBuffer.position(0);

    this.writeFile = file;
    this.writePosition = position;
  }

  public synchronized void updateReadIndex(String file, long position) {
    readIndexBuffer.putLong(position).putInt(file.length())
        .put(file.getBytes());
    readIndexBuffer.force();
    readIndexBuffer.position(0);

    this.readFile = file;
    this.readPosition = position;
  }

  public synchronized File getDirectory() {
    return directory;
  }

  public synchronized void setDirectory(File directory) {
    this.directory = directory;
  }

  public String getWriteFile() {
    return writeFile;
  }

  public void setWriteFile(String file) {
    this.writeFile = file;
  }

  public long getWritePosition() {
    return writePosition;
  }

  public void setWritePosition(long position) {
    this.writePosition = position;
  }

  public String getReadFile() {
    return readFile;
  }

  public void setReadFile(String readFile) {
    this.readFile = readFile;
  }

  public long getReadPosition() {
    return readPosition;
  }

  public void setReadPosition(long readPosition) {
    this.readPosition = readPosition;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(getClass()).add("writeFile", writeFile)
        .add("writePosition", writePosition).add("readFile", readFile)
        .add("readPosition", readPosition).add("directory", directory)
        .add("writeIndexFile", writeIndexFile)
        .add("readIndexFile", readIndexFile).toString();
  }
}
