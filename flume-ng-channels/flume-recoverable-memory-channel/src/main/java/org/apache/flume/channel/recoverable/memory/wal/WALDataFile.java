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
package org.apache.flume.channel.recoverable.memory.wal;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.collect.Lists;

class WALDataFile<T extends Writable> {

  private static final int VERSION = 1;

  private static final int RECORD_TYPE_EVENT = 1;
  private static final int RECORD_TYPE_COMMIT = 2;

  static class Reader<T extends Writable> implements Closeable {
    Class<T> clazz;
    DataInputStream input;
    private Configuration conf = new Configuration();
    Reader(File path, Class<T> clazz) throws IOException {
      this.clazz = clazz;
      input = new DataInputStream(new FileInputStream(path));
      int version = input.readInt();
      if(version != VERSION) {
        throw new IOException("Expected " + VERSION + " and got " + version);
      }
    }

    List<WALEntry<T>> nextBatch() throws IOException {
      List<WALEntry<T>> batch = Lists.newArrayList();
      // read until we hit a commit marker or until the
      // commit marker is encountered
      while(true) {
        try {
          int type = input.readInt();
          if(type == RECORD_TYPE_EVENT) {
            WALEntry<T> entry = newWALEntry(clazz, conf);
            entry.readFields(input);
            batch.add(entry);
          } else if(type == RECORD_TYPE_COMMIT) {
            // we only return what we have read if we find a command entry
            return batch;
          } else {
            throw new IOException("Unknown record type " + Integer.toHexString(type));
          }
        } catch(EOFException e) {
          // in the EOF case, we crashed or shutdown while writing a batch
          // and were unable to complete that batch. As such the client
          // would have gotten an exception and retried or locally
          // stored the batch for resending later
          return null;
        }
      }
    }

    @Override
    public void close() throws IOException {
      if (input != null) {
        input.close();
      }
    }
  }

  /**
   * Append and flush operations are synchronized as we are modifying
   * a file in said methods.
   */
  static class Writer<T extends Writable> implements Closeable {
    private FileOutputStream fileOutput;
    private DataOutputStream dataOutput;
    private AtomicLong largestSequenceID = new AtomicLong(0);
    private File path;

    Writer(File path) throws IOException {
      this.path = path;
      fileOutput = new FileOutputStream(path);
      dataOutput = new DataOutputStream(fileOutput);
      dataOutput.writeInt(VERSION);
      flush();
    }

    // TODO group commit
    synchronized void append(List<WALEntry<T>> entries) throws IOException {
      for (WALEntry<T> entry : entries) {
        largestSequenceID.set(Math.max(entry.getSequenceID(), largestSequenceID.get()));
        dataOutput.writeInt(RECORD_TYPE_EVENT);
        entry.write(dataOutput);
      }
      // if this is successful, the events have been
      // successfully persisted and will be replayed
      // in the case of a crash
      dataOutput.writeInt(RECORD_TYPE_COMMIT);
      flush(false);
    }

    synchronized void flush() throws IOException {
      flush(true);
    }
    synchronized void flush(boolean metadata) throws IOException {
      fileOutput.getChannel().force(metadata);
    }

    public long getLargestSequenceID() {
      return largestSequenceID.get();
    }
    public File getPath() {
      return path;
    }

    public long getSize() {
      return dataOutput.size();
    }

    @Override
    public synchronized void close() throws IOException {
      if (dataOutput != null) {
        flush();
        dataOutput.close();
      }
    }
  }
  private static <T extends Writable> WALEntry<T> newWALEntry(Class<T> clazz, Configuration conf) {
    return new WALEntry<T>(ReflectionUtils.newInstance(clazz, conf));
  }
}