/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.file;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.flume.tools.DirectMemoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a single data file on disk. Has methods to write,
 * read sequentially (replay), and read randomly (channel takes).
 */
class LogFile {

  private static final Logger LOG = LoggerFactory
      .getLogger(LogFile.class);
  /**
   * This class preallocates the data files 1MB at time to avoid
   * the updating of the inode on each write and to avoid the disk
   * filling up during a write. It's also faster, so there.
   */
  private static final ByteBuffer FILL = DirectMemoryUtils.
      allocate(1024 * 1024); // preallocation, 1MB

  private static final byte OP_RECORD = Byte.MAX_VALUE;
  private static final byte OP_EOF = Byte.MIN_VALUE;

  static {
    for (int i = 0; i < FILL.capacity(); i++) {
      FILL.put(OP_EOF);
    }
  }
  private static final int VERSION = 2;


  static class Writer {
    private final int fileID;
    private final File file;
    private final long maxFileSize;
    private final RandomAccessFile writeFileHandle;
    private final FileChannel writeFileChannel;
    private final long checkpointPositionMarker;

    private volatile boolean open;

    Writer(File file, int logFileID, long maxFileSize)
        throws IOException {
      this(file, logFileID, maxFileSize, true);
    }

    Writer(File file, int logFileID, long maxFileSize, boolean active)
        throws IOException {
      this.file = file;
      fileID = logFileID;
      this.maxFileSize = Math.min(maxFileSize,
          FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE);
      writeFileHandle = new RandomAccessFile(file, "rw");
      if (active) {
        writeFileHandle.writeInt(VERSION);
        writeFileHandle.writeInt(fileID);
        checkpointPositionMarker = writeFileHandle.getFilePointer();
        // checkpoint marker
        writeFileHandle.writeLong(0L);
        // timestamp placeholder
        writeFileHandle.writeLong(0L);
        writeFileChannel = writeFileHandle.getChannel();
        writeFileChannel.force(true);
      } else {
        int version = writeFileHandle.readInt();
        if (version != VERSION) {
          throw new IOException("The version of log file: "
              + file.getCanonicalPath() + " is different from expected "
              + " version: expected = " + VERSION + ", found = " + version);
        }
        int fid = writeFileHandle.readInt();
        if (fid != logFileID) {
          throw new IOException("The file id of log file: "
              + file.getCanonicalPath() + " is different from expected "
              + " id: expected = " + logFileID + ", found = " + fid);
        }
        checkpointPositionMarker = writeFileHandle.getFilePointer();
        long chkptMarker = writeFileHandle.readLong();
        long chkptTimestamp = writeFileHandle.readLong();
        LOG.info("File: " + file.getCanonicalPath() + " was last checkpointed "
             + "at position: " + chkptMarker + ", ts: " + chkptTimestamp);
        writeFileChannel = writeFileHandle.getChannel();

        // Jump to the last position
        writeFileChannel.position(chkptMarker);
      }
      LOG.info("Opened " + file);
      open = true;
    }

    File getFile() {
      return file;
    }

    synchronized void markCheckpoint(long logWriteOrderID) throws IOException {
      long currentPosition = writeFileChannel.position();
      writeFileHandle.seek(checkpointPositionMarker);
      writeFileHandle.writeLong(currentPosition);
      writeFileHandle.writeLong(logWriteOrderID);
      writeFileChannel.position(currentPosition);
      LOG.info("Noted checkpoint for file: " + file + ", id: " + fileID
          + ", checkpoint position: " + currentPosition);
    }

    String getParent() {
      return file.getParent();
    }

    synchronized void close() {
      if(open) {
        open = false;
        if(writeFileChannel.isOpen()) {
          LOG.info("Closing " + file);
          try {
            writeFileChannel.force(true);
          } catch (IOException e) {
            LOG.warn("Unable to flush to disk", e);
          }
          try {
            writeFileHandle.close();
          } catch (IOException e) {
            LOG.info("Unable to close", e);
          }
        }
      }
    }

    synchronized long length() throws IOException {
      return writeFileChannel.position();
    }

    synchronized FlumeEventPointer put(ByteBuffer buffer) throws IOException {
      Pair<Integer, Integer> pair = write(buffer);
      return new FlumeEventPointer(pair.getLeft(), pair.getRight());
    }
    synchronized void take(ByteBuffer buffer) throws IOException {
      write(buffer);
    }
    synchronized void rollback(ByteBuffer buffer) throws IOException {
      write(buffer);
    }
    synchronized void commit(ByteBuffer buffer) throws IOException {
      write(buffer);
      sync();
    }

    synchronized boolean isRollRequired(ByteBuffer buffer) throws IOException {
      return open && length() + (long) buffer.capacity() > maxFileSize;
    }

    int getFileID() {
      return fileID;
    }
    private void sync() throws IOException {
      Preconditions.checkState(open, "File closed");
      writeFileChannel.force(false);
    }
    private Pair<Integer, Integer> write(ByteBuffer buffer) throws IOException {
      Preconditions.checkState(open, "File closed");
      long length = length();
      long expectedLength = length + (long) buffer.capacity();
      Preconditions.checkArgument(expectedLength < (long) Integer.MAX_VALUE);
      int offset = (int)length;
      Preconditions.checkState(offset > 0);
      int recordLength = 1 + buffer.capacity();
      preallocate(recordLength);
      ByteBuffer toWrite = ByteBuffer.allocate(recordLength);
      toWrite.put(OP_RECORD);
      toWrite.put(buffer);
      toWrite.position(0);
      int wrote = writeFileChannel.write(toWrite);
      Preconditions.checkState(wrote == toWrite.limit());
      return Pair.of(fileID, offset);
    }
    private void preallocate(int size) throws IOException {
      long position = writeFileChannel.position();
      if(position + size > writeFileChannel.size()) {
        LOG.debug("Preallocating at position " + position);
        synchronized (FILL) {
          FILL.position(0);
          writeFileChannel.write(FILL, position);
        }
      }
    }

  }

  static class RandomReader {
    private final File file;
    private final BlockingQueue<RandomAccessFile> readFileHandles =
        new ArrayBlockingQueue<RandomAccessFile>(50, true);

    private volatile boolean open;
    public RandomReader(File file) throws IOException {
      this.file = file;
      readFileHandles.add(open());
      open = true;
    }

    File getFile() {
      return file;
    }

    FlumeEvent get(int offset) throws IOException, InterruptedException {
      Preconditions.checkState(open, "File closed");
      RandomAccessFile fileHandle = checkOut();
      boolean error = true;
      try {
        fileHandle.seek(offset);
        byte operation = fileHandle.readByte();
        Preconditions.checkState(operation == OP_RECORD, Integer.toHexString(operation));
        TransactionEventRecord record = TransactionEventRecord.
            fromDataInput(fileHandle);
        if(!(record instanceof Put)) {
          Preconditions.checkState(false, "Record is " +
              record.getClass().getSimpleName());
        }
        error = false;
        return ((Put)record).getEvent();
      } finally {
        if(error) {
          close(fileHandle);
        } else {
          checkIn(fileHandle);
        }
      }
    }
    synchronized void close() {
      if(open) {
        open = false;
        LOG.info("Closing RandomReader " + file);
        List<RandomAccessFile> fileHandles = Lists.newArrayList();
        while(readFileHandles.drainTo(fileHandles) > 0) {
          for(RandomAccessFile fileHandle : fileHandles) {
            synchronized (fileHandle) {
              try {
                fileHandle.close();
              } catch (IOException e) {
                LOG.info("Unable to close fileHandle for " + file);
              }
            }
          }
          fileHandles.clear();
          try {
            Thread.sleep(100L);
          } catch (InterruptedException e) {
            // this is uninterruptable
          }
        }
      }
    }
    private RandomAccessFile open() throws IOException {
      return new RandomAccessFile(file, "r");
    }

    private void checkIn(RandomAccessFile fileHandle) {
      if(!readFileHandles.offer(fileHandle)) {
        close(fileHandle);
      }
    }
    private RandomAccessFile checkOut()
        throws IOException, InterruptedException {
      RandomAccessFile fileHandle = readFileHandles.poll();
      if(fileHandle != null) {
        return fileHandle;
      }
      int remaining = readFileHandles.remainingCapacity();
      if(remaining > 0) {
        LOG.info("Opening " + file + " for read, remaining capacity is "
            + remaining);
        return open();
      }
      return readFileHandles.take();
    }
    private static void close(RandomAccessFile fileHandle) {
      if(fileHandle != null) {
        try {
          fileHandle.close();
        } catch (IOException e) {}
      }
    }
  }

  static class SequentialReader {
    private final RandomAccessFile fileHandle;
    private final FileChannel fileChannel;
    private final int version;
    private final int logFileID;
    private final long lastCheckpointPosition;
    private final long lastCheckpointTimestamp;
    private final File file;

    /**
     * Construct a Sequential Log Reader object
     * @param file
     * @throws IOException if an I/O error occurs
     * @throws EOFException if the file is empty
     */
    SequentialReader(File file) throws IOException, EOFException {
      this.file = file;
      fileHandle = new RandomAccessFile(file, "r");
      fileChannel = fileHandle.getChannel();
      version = fileHandle.readInt();
      if(version != VERSION) {
        throw new IOException("Version is " + Integer.toHexString(version) +
            " expected " + Integer.toHexString(VERSION)
            + " file: " + file.getCanonicalPath());
      }
      logFileID = fileHandle.readInt();
      lastCheckpointPosition = fileHandle.readLong();
      lastCheckpointTimestamp = fileHandle.readLong();

      Preconditions.checkArgument(logFileID >= 0, "LogFileID is not positive: "
          + Integer.toHexString(logFileID));
    }
    int getVersion() {
      return version;
    }
    int getLogFileID() {
      return logFileID;
    }
    void skipToLastCheckpointPosition(long checkpointTimestamp)
        throws IOException {
      if (lastCheckpointPosition > 0L
          && lastCheckpointTimestamp <= checkpointTimestamp) {
        LOG.info("fast-forward to checkpoint position: "
                  + lastCheckpointPosition);
        fileChannel.position(lastCheckpointPosition);
      } else {
        LOG.warn("Checkpoint for file(" + file.getAbsolutePath() + ") "
            + "is: " + lastCheckpointTimestamp + ", which is beyond the "
            + "requested checkpoint time: " + checkpointTimestamp + ". ");
      }
    }
    LogRecord next() throws IOException {
      int offset = -1;
      try {
        long position = fileChannel.position();
        if (position > FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE) {
          LOG.warn("File position exceeds the threshold: "
                + FileChannelConfiguration.DEFAULT_MAX_FILE_SIZE
                + ", position: " + position);
        }
        offset = (int) position;
        byte operation = fileHandle.readByte();
        if(operation != OP_RECORD) {
          if(operation == OP_EOF) {
            LOG.info("Encountered EOF at " + offset + " in " + file);
          } else {
            LOG.error("Encountered non op-record at " + offset + " " +
                Integer.toHexString(operation) + " in " + file);
          }
          return null;
        }
        TransactionEventRecord record = TransactionEventRecord.
            fromDataInput(fileHandle);
        Preconditions.checkState(offset > 0);
        return new LogRecord(logFileID, offset, record);
      } catch(EOFException e) {
        return null;
      } catch (IOException e) {
        throw new IOException("Unable to read next Transaction from log file " +
            file.getCanonicalPath() + " at offset " + offset, e);
      }
    }
    void close() {
      if(fileHandle != null) {
        try {
          fileHandle.close();
        } catch (IOException e) {}
      }
    }
  }

  public static void main(String[] args) throws EOFException, IOException {
    File file = new File(args[0]);
    LogFile.SequentialReader reader = null;
    try {
      reader = new LogFile.SequentialReader(file);
      LogRecord entry;
      FlumeEventPointer ptr;
      // for puts the fileId is the fileID of the file they exist in
      // for takes the fileId and offset are pointers to a put
      int fileId = reader.getLogFileID();
      int count = 0;
      int readCount = 0;
      int putCount = 0;
      int takeCount = 0;
      int rollbackCount = 0;
      int commitCount = 0;
      while ((entry = reader.next()) != null) {
        int offset = entry.getOffset();
        TransactionEventRecord record = entry.getEvent();
        short type = record.getRecordType();
        long trans = record.getTransactionID();
        long ts = record.getLogWriteOrderID();
        readCount++;
        ptr = null;
        if (type == TransactionEventRecord.Type.PUT.get()) {
          putCount++;
          ptr = new FlumeEventPointer(fileId, offset);
        } else if (type == TransactionEventRecord.Type.TAKE.get()) {
          takeCount++;
          Take take = (Take) record;
          ptr = new FlumeEventPointer(take.getFileID(), take.getOffset());
        } else if (type == TransactionEventRecord.Type.ROLLBACK.get()) {
          rollbackCount++;
        } else if (type == TransactionEventRecord.Type.COMMIT.get()) {
          commitCount++;
        } else {
          Preconditions.checkArgument(false, "Unknown record type: "
              + Integer.toHexString(type));
        }
        System.out.println(Joiner.on(", ").skipNulls().join(
            trans, ts, fileId, offset, TransactionEventRecord.getName(type), ptr));

      }
      System.out.println("Replayed " + count + " from " + file + " read: " + readCount
          + ", put: " + putCount + ", take: "
          + takeCount + ", rollback: " + rollbackCount + ", commit: "
          + commitCount);
    } catch (EOFException e) {
      System.out.println("Hit EOF on " + file);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
