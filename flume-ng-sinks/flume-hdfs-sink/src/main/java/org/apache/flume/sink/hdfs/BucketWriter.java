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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Event;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketWriter {


  private static final Logger LOG = LoggerFactory
      .getLogger(BucketWriter.class);

  private static final String IN_USE_EXT = ".tmp";
  /**
   * In case of an error writing to HDFS (it hangs) this instance will be
   * tossed away and we will create a new instance. Gurantee unique files
   * in this case.
   */
  private static final AtomicLong fileExentionCounter = new AtomicLong(0);
  private HDFSWriter writer;
  private FlumeFormatter formatter;
  private long eventCounter;
  private long processSize;
  private long lastRollTime;
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
    lastRollTime = System.currentTimeMillis();
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
    // open();
  }

  public void open() throws IOException {
    if ((filePath == null) || (writer == null) || (formatter == null)) {
      throw new IOException("Invalid file settings");
    }

    long counter = fileExentionCounter.incrementAndGet();
    if (codeC == null) {
      bucketPath = filePath + "." + counter;
      writer.open(bucketPath + IN_USE_EXT, formatter);
    } else {
      bucketPath = filePath + "." + counter
          + codeC.getDefaultExtension();
      writer.open(bucketPath + IN_USE_EXT, codeC, compType, formatter);
    }
    batchCounter = 0;
    LOG.info("Creating " + bucketPath + IN_USE_EXT);
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
    LOG.debug("Closing " + bucketPath);
    resetCounters();
    if (writer != null) {
      writer.close(); // could block
    }
    renameBucket();
  }

  // close the file, ignore the IOException
  // ideally the underlying writer should discard unwritten data
  public void abort() {
    try {
      close();
    } catch (IOException ex) {
      LOG.info("Exception during close on abort", ex);
    }
    try {
      open();
    } catch (IOException ex) {
      LOG.warn("Exception during opem on abort", ex);
    }
  }

  // flush the data
  public void flush() throws IOException {
    writer.sync(); // could block
    batchCounter = 0;
  }

  // append the data, update stats, handle roll and batching
  public void append(Event e) throws IOException {
    writer.append(e, formatter); // could block

    // update statistics
    processSize += e.getBody().length;
    eventCounter++;
    batchCounter++;

    // check if its time to rotate the file
    if (shouldRotate()) {
      close();
      open();
    } else if ((batchCounter == batchSize)) {
      flush();
    }

  }

  // check if time to rotate the file
  private boolean shouldRotate() {
    boolean doRotate = false;

    long elapsed = (System.currentTimeMillis() - lastRollTime) / 1000L;
    if ((rollInterval > 0) && (rollInterval <= elapsed)) {
      LOG.debug("rolling: rollTime: {}, elapsed: {}", rollInterval, elapsed);
      doRotate = true;
    }

    if ((rollCount > 0) && (rollCount <= eventCounter)) {
      LOG.debug("rolling: rollCount: {}, events: {}", rollCount, eventCounter);
      doRotate = true;
    }

    if ((rollSize > 0) && (rollSize <= processSize)) {
      LOG.debug("rolling: rollSize: {}, bytes: {}", rollSize, processSize);
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

    if(hdfs.exists(srcPath)) { // could block
      LOG.info("Renaming " + srcPath + " to " + dstPath);
      hdfs.rename(srcPath, dstPath); // could block
    }
  }

  @Override
  public String toString() {
    return "[ " + this.getClass().getSimpleName() + " filePath = " + filePath +
        ", bucketPath = " + bucketPath + " ]";
  }
}
