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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Represents a copy of FlumeEventQueue written out to a specific
 * directory. Two checkpoints will be used at all times, writes
 * will be alternated between the two Checkpoint objects/files.
 */
class Checkpoint {

  private static final Logger LOG = LoggerFactory
      .getLogger(Checkpoint.class);
  private static final long OFFSET_TIMESTAMP = 0;
  private final File file;
  private final int queueCapacity;

  private long timestamp;
  Checkpoint(File file, int queueCapacity) {
    this.file = file;
    this.queueCapacity = queueCapacity;
    timestamp = -1;
  }

  void write(FlumeEventQueue queue) throws IOException {
    LOG.info("Writing checkoint to " + file + ", size = " + queue.size());
    RandomAccessFile dataOutput = new RandomAccessFile(file, "rw");
    try {
      // write out a small number
      dataOutput.writeLong(Long.MIN_VALUE);
      // write a the queue itself
      dataOutput.writeInt(queue.getCapacity());
      queue.write(dataOutput);
      // force all changes to disk
      dataOutput.getChannel().force(true);
      // now update the timestamp saying we are successful
      dataOutput.seek(OFFSET_TIMESTAMP);
      dataOutput.writeLong(timestamp = System.currentTimeMillis());
      dataOutput.getChannel().force(true);
      dataOutput.close();
    } finally {
      if(dataOutput != null) {
        try {
          dataOutput.close();
        } catch (IOException e) {}
      }
    }
  }

  File getFile() {
    return file;
  }

  FlumeEventQueue read() throws IOException {
    FileInputStream fileInput = new FileInputStream(file);
    try {
      DataInputStream dataInput = new DataInputStream(fileInput);
      long timestamp = dataInput.readLong();
      Preconditions.checkState(timestamp > 0, "Timestamp is invalid " + timestamp);
      int capacity = Math.max(dataInput.readInt(), queueCapacity);
      FlumeEventQueue queue = new FlumeEventQueue(capacity);
      queue.readFields(dataInput);
      return queue;
    } finally {
      if(fileInput != null) {
        try {
          fileInput.close();
        } catch (IOException e) {}
      }
    }
  }

  long getTimestamp() throws IOException {
    if(timestamp > 0) {
      return timestamp;
    }
    timestamp = 0;
    RandomAccessFile fileHandle = null;
    try {
      fileHandle = new RandomAccessFile(file, "r");
      fileHandle.seek(OFFSET_TIMESTAMP);
      timestamp = fileHandle.readLong();
    } catch(EOFException e) {
      // no checkpoint taken
    } catch (FileNotFoundException e) {
      // no checkpoint taken
    } finally {
      if(fileHandle != null) {
        try {
          fileHandle.close();
        } catch (IOException x) {}
      }
    }
    return timestamp;
  }
}
