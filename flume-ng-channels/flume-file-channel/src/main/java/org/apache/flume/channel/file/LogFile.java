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
  public static final long MAX_FILE_SIZE =
      Integer.MAX_VALUE - (1024L * 1024L);

  private static final byte OP_RECORD = Byte.MAX_VALUE;
  private static final byte OP_EOF = Byte.MIN_VALUE;

  static {
    for (int i = 0; i < FILL.capacity(); i++) {
      FILL.put(OP_EOF);
    }
  }
  private static final int VERSION = 1;


  static class Writer {
    private final int fileID;
    private final File file;
    private final long maxFileSize;
    private final RandomAccessFile writeFileHandle;
    private final FileChannel writeFileChannel;

    private volatile boolean open;

    Writer(File file, int logFileID, long maxFileSize) throws IOException {
      this.file = file;
      fileID = logFileID;
      this.maxFileSize = Math.min(maxFileSize, MAX_FILE_SIZE);
      writeFileHandle = new RandomAccessFile(file, "rw");
      writeFileHandle.writeInt(VERSION);
      writeFileHandle.writeInt(fileID);
      writeFileChannel = writeFileHandle.getChannel();
      writeFileChannel.force(true);
      LOG.info("Opened " + file);
      open = true;
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
            writeFileChannel.force(false);
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
      preallocate(1 + buffer.capacity());
      writeFileHandle.writeByte(OP_RECORD);
      int wrote = writeFileChannel.write(buffer);
      Preconditions.checkState(wrote == buffer.limit());
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
    FlumeEvent get(int offset) throws IOException, InterruptedException {
      Preconditions.checkState(open, "File closed");
      RandomAccessFile fileHandle = checkOut();
      boolean error = true;
      try {
        fileHandle.seek(offset);
        byte operation = fileHandle.readByte();
        Preconditions.checkState(operation == OP_RECORD);
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

    /**
     * Construct a Sequential Log Reader object
     * @param file
     * @throws IOException if an I/O error occurs
     * @throws EOFException if the file is empty
     */
    SequentialReader(File file) throws IOException, EOFException {
      fileHandle = new RandomAccessFile(file, "r");
      fileChannel = fileHandle.getChannel();
      version = fileHandle.readInt();
      if(version != VERSION) {
        throw new IOException("Version is " + Integer.toHexString(version) +
            " expected " + Integer.toHexString(VERSION));
      }
      logFileID = fileHandle.readInt();
      Preconditions.checkArgument(logFileID >= 0, "LogFileID is not positive: "
          + Integer.toHexString(logFileID));
    }
    int getVersion() {
      return version;
    }
    int getLogFileID() {
      return logFileID;
    }
    Pair<Integer, TransactionEventRecord> next() throws IOException {
      try {
        long position = fileChannel.position();
        Preconditions.checkState(position < MAX_FILE_SIZE,
            String.valueOf(position));
        int offset = (int) position;
        byte operation = fileHandle.readByte();
        if(operation != OP_RECORD) {
          return null;
        }
        TransactionEventRecord record = TransactionEventRecord.
            fromDataInput(fileHandle);
        Preconditions.checkState(offset > 0);
        return Pair.of(offset, record);
      } catch(EOFException e) {
        return null;
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
}
