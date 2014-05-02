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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a single data file on disk. Has methods to write,
 * read sequentially (replay), and read randomly (channel takes).
 */
@Deprecated
class LogFileV2 extends LogFile {
  protected static final Logger LOGGER =
      LoggerFactory.getLogger(LogFileV2.class);

  private static final long OFFSET_CHECKPOINT = 2 * Serialization.SIZE_OF_INT;

  private LogFileV2() {}

  static class MetaDataWriter extends LogFile.MetaDataWriter {

    protected MetaDataWriter(File file, int logFileID) throws IOException {
      super(file, logFileID);
      boolean error = true;
      try {
        RandomAccessFile writeFileHandle = getFileHandle();
        int version = writeFileHandle.readInt();
        if (version != getVersion()) {
          throw new IOException("The version of log file: "
              + file.getCanonicalPath() + " is different from expected "
              + " version: expected = " + getVersion() + ", found = " + version);
        }
        int fid = writeFileHandle.readInt();
        if (fid != logFileID) {
          throw new IOException("The file id of log file: "
              + file.getCanonicalPath() + " is different from expected "
              + " id: expected = " + logFileID + ", found = " + fid);
        }
        setLastCheckpointOffset(writeFileHandle.readLong());
        setLastCheckpointWriteOrderID(writeFileHandle.readLong());
        LOGGER.info("File: " + file.getCanonicalPath() + " was last checkpointed "
            + "at position: " + getLastCheckpointOffset()
            + ", logWriteOrderID: " + getLastCheckpointWriteOrderID());
        error = false;
      } finally {
        if(error) {
          close();
        }
      }
    }

    @Override
    int getVersion() {
      return Serialization.VERSION_2;
    }

    @Override
    void markCheckpoint(long currentPosition, long logWriteOrderID)
        throws IOException {
      RandomAccessFile writeFileHandle = getFileHandle();
      writeFileHandle.seek(OFFSET_CHECKPOINT);
      writeFileHandle.writeLong(currentPosition);
      writeFileHandle.writeLong(logWriteOrderID);
      writeFileHandle.getChannel().force(true);
      LOGGER.info("Noted checkpoint for file: " + getFile() + ", id: "
          + getLogFileID() + ", checkpoint position: " + currentPosition
          + ", logWriteOrderID: " + logWriteOrderID);

    }
  }

  static class Writer extends LogFile.Writer {

    Writer(File file, int logFileID, long maxFileSize,
        long usableSpaceRefreshInterval)
        throws IOException {
      super(file, logFileID, maxFileSize, null, usableSpaceRefreshInterval,
        true, 0);
      RandomAccessFile writeFileHandle = getFileHandle();
      writeFileHandle.writeInt(getVersion());
      writeFileHandle.writeInt(logFileID);
      // checkpoint marker
      writeFileHandle.writeLong(0L);
      // timestamp placeholder
      writeFileHandle.writeLong(0L);
      getFileChannel().force(true);

    }
    @Override
    int getVersion() {
      return Serialization.VERSION_2;
    }
  }

  static class RandomReader extends LogFile.RandomReader {
    RandomReader(File file)
        throws IOException {
      super(file, null, true);
    }
    @Override
    int getVersion() {
      return Serialization.VERSION_2;
    }
    @Override
    protected TransactionEventRecord doGet(RandomAccessFile fileHandle)
        throws IOException {
      return TransactionEventRecord.fromDataInputV2(fileHandle);
    }
  }

  static class SequentialReader extends LogFile.SequentialReader {

    SequentialReader(File file)
        throws EOFException, IOException {
      super(file, null);
      RandomAccessFile fileHandle = getFileHandle();
      int version = fileHandle.readInt();
      if(version != getVersion()) {
        throw new IOException("Version is " + Integer.toHexString(version) +
            " expected " + Integer.toHexString(getVersion())
            + " file: " + file.getCanonicalPath());
      }
      setLogFileID(fileHandle.readInt());
      setLastCheckpointPosition(fileHandle.readLong());
      setLastCheckpointWriteOrderID(fileHandle.readLong());
    }
    @Override
    public int getVersion() {
      return Serialization.VERSION_2;
    }
    @Override
    LogRecord doNext(int offset) throws IOException {
      TransactionEventRecord event =
          TransactionEventRecord.fromDataInputV2(getFileHandle());
      return new LogRecord(getLogFileID(), offset, event);
    }
  }
}
