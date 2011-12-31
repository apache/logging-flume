package org.apache.wal.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
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

  private ReentrantLock readLock;
  private ReentrantLock writeLock;

  private String writeFile;
  private long writePosition;
  private String readFile;
  private long readPosition;

  public WALIndex() {
    readLock = new ReentrantLock();
    writeLock = new ReentrantLock();
  }

  public synchronized void open() throws FileNotFoundException, IOException {

    try {
      writeLock.lock();
      writeIndexFile = new File(directory, writeIndexFileName);

      logger.info("Opening WAL index table writeFile:{}", writeIndexFile);

      writeIndexBuffer = Files.map(writeIndexFile,
          FileChannel.MapMode.READ_WRITE, 4 * 1024);

      writePosition = writeIndexBuffer.getLong();
      int writeFileNameLength = writeIndexBuffer.getInt();

      if (writeFileNameLength > 0) {
        byte[] buffer = new byte[writeFileNameLength];
        writeIndexBuffer.get(buffer);
        writeFile = new String(buffer);
      }
      writeIndexBuffer.position(0);
    } finally {
      writeLock.unlock();
    }

    try {
      readLock.lock();
      readIndexFile = new File(directory, readIndexFileName);

      readIndexBuffer = Files.map(readIndexFile,
          FileChannel.MapMode.READ_WRITE, 4 * 1024);

      readPosition = readIndexBuffer.getLong();
      int readFileNameLength = readIndexBuffer.getInt();

      if (readFileNameLength > 0) {
        byte[] buffer = new byte[readFileNameLength];
        readIndexBuffer.get(buffer);
        readFile = new String(buffer);
      }

      readIndexBuffer.position(0);
    } finally {
      readLock.unlock();
    }

    logger.debug("Loaded index:{}", this);
  }

  public void updateWriteIndex(String file, long position) {
    try {
      writeLock.lock();
      writeIndexBuffer.putLong(position).putInt(file.length())
          .put(file.getBytes());
      writeIndexBuffer.force();
      writeIndexBuffer.position(0);

      this.writeFile = file;
      this.writePosition = position;
    } finally {
      writeLock.unlock();
    }
  }

  public void updateReadIndex(String file, long position) {
    try {
      readLock.lock();
      readIndexBuffer.putLong(position).putInt(file.length())
          .put(file.getBytes());
      readIndexBuffer.force();
      readIndexBuffer.position(0);

      this.readFile = file;
      this.readPosition = position;
    } finally {
      readLock.unlock();
    }
  }

  public synchronized File getDirectory() {
    return directory;
  }

  public synchronized void setDirectory(File directory) {
    this.directory = directory;
  }

  public String getWriteFile() {
    try {
      writeLock.lock();
      return writeFile;
    } finally {
      writeLock.unlock();
    }
  }

  public long getWritePosition() {
    try {
      writeLock.lock();
      return writePosition;
    } finally {
      writeLock.unlock();
    }
  }

  public String getReadFile() {
    try {
      readLock.lock();
      return readFile;
    } finally {
      readLock.unlock();
    }
  }

  public long getReadPosition() {
    try {
      readLock.lock();
      return readPosition;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public synchronized String toString() {
    ToStringHelper stringHelper = Objects.toStringHelper(getClass()).add(
        "directory", directory);

    try {
      writeLock.lock();
      stringHelper.add("writeFile", writeFile)
          .add("writePosition", writePosition)
          .add("writeIndexFile", writeIndexFile);
    } finally {
      writeLock.unlock();
    }

    try {
      readLock.lock();
      stringHelper.add("readFile", readFile).add("readPosition", readPosition)
          .add("readIndexFile", readIndexFile);
    } finally {
      readLock.unlock();
    }

    return stringHelper.toString();
  }

}
