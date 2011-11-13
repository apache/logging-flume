/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.hdfs;

import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;

public class BucketWriter {

  public static enum BucketFlushStatus {
    BatchStarted, BatchPending, BatchFlushed
  }

  private static final String IN_USE_EXT = ".tmp";
  private HDFSWriter writer;
  private FlumeFormatter formatter;
  private long eventCounter;
  private long processSize;
  private long lastProcessTime;
  private long fileExentionCounter;
  private long batchCounter;
  private String filePath;
  private long rollInterval;
  private long rollSize;
  private long rollCount;
  private long batchSize;
  private CompressionCodec codeC;
  private CompressionType compType;
  private String bucketPath;

  // clear the class counters
  private void resetCounters() {
    eventCounter = 0;
    processSize = 0;
    lastProcessTime = 0;
    batchCounter = 0;
  }

  // constructor. initialize the thresholds and open the file handle
  public BucketWriter(long rollInt, long rollSz, long rollCnt, long bSize)
      throws IOException {
    rollInterval = rollInt;
    rollSize = rollSz;
    rollCount = rollCnt;
    batchSize = bSize;

    resetCounters();
    fileExentionCounter = 0;
    // open();
  }

  public void open() throws IOException {
    if ((filePath == null) || (writer == null) || (formatter == null)) {
      throw new IOException("Invalid file settings");
    }

    if (codeC == null) {
      bucketPath = filePath + "." + fileExentionCounter;
      writer.open(bucketPath + IN_USE_EXT, formatter);
    } else {
      bucketPath = filePath + "." + fileExentionCounter
          + codeC.getDefaultExtension();
      writer.open(bucketPath + IN_USE_EXT, codeC, compType, formatter);
    }
  }

  public void open(String fPath, HDFSWriter hWriter, FlumeFormatter fmt)
      throws IOException {
    open(fPath, null, CompressionType.NONE, hWriter, fmt);
  }

  public void open(String fPath, CompressionCodec codec, CompressionType cType,
      HDFSWriter hWriter, FlumeFormatter fmt) throws IOException {
    filePath = fPath;
    codeC = codec;
    compType = cType;
    writer = hWriter;
    formatter = fmt;
    open();
  }

  // close the file handle
  public void close() throws IOException {
    resetCounters();
    if (writer != null) {
      writer.close();
      fileExentionCounter++;
    }
    renameBucket();
  }

  // close the file, ignore the IOException
  // ideally the underlying writer should discard unwritten data
  public void abort() {
    try {
      close();
      open();
    } catch (IOException eIO) {
      // Ignore it
    }
  }

  // flush the data
  public void flush() throws IOException {
    writer.sync();
    batchCounter = 0;
  }

  // handle the batching, do the real flush if its time
  public BucketFlushStatus sync() throws IOException {
    BucketFlushStatus syncStatus;

    if ((batchCounter == batchSize)) {
      flush();
      syncStatus = BucketFlushStatus.BatchFlushed;
    } else {
      if (batchCounter == 1) {
        syncStatus = BucketFlushStatus.BatchStarted;
      } else {
        syncStatus = BucketFlushStatus.BatchPending;
      }
    }
    return syncStatus;
  }

  // append the data, update stats, handle roll and batching
  public BucketFlushStatus append(Event e) throws IOException {
    BucketFlushStatus syncStatus;

    writer.append(e, formatter);

    // update statistics
    processSize += e.getBody().length;
    lastProcessTime = System.currentTimeMillis() * 1000;
    eventCounter++;
    batchCounter++;

    // check if its time to rotate the file
    if (shouldRotate()) {
      close();
      open();
      syncStatus = BucketFlushStatus.BatchFlushed;
    } else {
      syncStatus = sync();
    }

    return syncStatus;
  }

  // check if time to rotate the file
  public boolean shouldRotate() {
    boolean doRotate = false;

    if ((rollInterval > 0)
        && (rollInterval < (System.currentTimeMillis() - lastProcessTime) / 1000))
      doRotate = true;
    if ((rollCount > 0) && (rollCount <= eventCounter)) {
      eventCounter = 0;
      doRotate = true;
    }
    if ((rollSize > 0) && (rollSize <= processSize)) {
      processSize = 0;
      doRotate = true;
    }

    return doRotate;
  }

  public String getFilePath() {
    return filePath;
  }

  private void renameBucket() throws IOException {
    Configuration conf = new Configuration();
    Path srcPath = new Path(bucketPath + IN_USE_EXT);
    Path dstPath = new Path(bucketPath);
    FileSystem hdfs = dstPath.getFileSystem(conf);

    hdfs.rename(srcPath, dstPath);
  }
}
