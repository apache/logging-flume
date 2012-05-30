
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
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal API intended for HDFSSink use.
 * This class does file rolling and handles file formats and serialization.
 * The methods in this class are NOT THREAD SAFE.
 */
class BucketWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(BucketWriter.class);

  private static final String IN_USE_EXT = ".tmp";
  /**
   * This lock ensures that only one thread can open a file at a time.
   */
  private static final Integer staticLock = new Integer(1);
  private HDFSWriter writer;
  private FlumeFormatter formatter;
  private long eventCounter;
  private long processSize;
  private long lastRollTime;
  private long rollInterval;
  private long rollSize;
  private long rollCount;
  private long batchSize;
  private CompressionCodec codeC;
  private CompressionType compType;
  private FileSystem fileSystem;
  private Context context;

  private volatile String filePath;
  private volatile String bucketPath;
  private volatile long batchCounter;
  private volatile boolean isOpen;

  private final AtomicLong fileExtensionCounter;

  // clear the class counters
  private void resetCounters() {
    eventCounter = 0;
    processSize = 0;
    lastRollTime = System.currentTimeMillis();
    batchCounter = 0;
  }

  BucketWriter(long rollInt, long rollSz, long rollCnt, long bSize,
      Context ctx, String fPath, CompressionCodec codec, CompressionType cType,
      HDFSWriter hWriter, FlumeFormatter fmt) {
    rollInterval = rollInt;
    rollSize = rollSz;
    rollCount = rollCnt;
    batchSize = bSize;
    context = ctx;
    filePath = fPath;
    codeC = codec;
    compType = cType;
    writer = hWriter;
    formatter = fmt;
    isOpen = false;

    fileExtensionCounter = new AtomicLong(System.currentTimeMillis());
    writer.configure(context);
  }

  /**
   * open() is called by append()
   * @throws IOException
   */
  private void open() throws IOException {
    if ((filePath == null) || (writer == null) || (formatter == null)) {
      throw new IOException("Invalid file settings");
    }

    Configuration config = new Configuration();
    // disable FileSystem JVM shutdown hook
    config.setBoolean("fs.automatic.close", false);

    // Hadoop is not thread safe when doing certain RPC operations,
    // including getFileSystem(), when running under Kerberos.
    // open() must be called by one thread at a time in the JVM.
    // NOTE: tried synchronizing on the underlying Kerberos principal previously
    // which caused deadlocks. See FLUME-1231.
    synchronized (staticLock) {
      long counter = fileExtensionCounter.incrementAndGet();
      if (codeC == null) {
        bucketPath = filePath + "." + counter;
        // Need to get reference to FS using above config before underlying
        // writer does in order to avoid shutdown hook & IllegalStateExceptions
        fileSystem = new Path(bucketPath).getFileSystem(config);
        LOG.info("Creating " + bucketPath + IN_USE_EXT);
        writer.open(bucketPath + IN_USE_EXT, formatter);
      } else {
        bucketPath = filePath + "." + counter
            + codeC.getDefaultExtension();
        // need to get reference to FS before writer does to avoid shutdown hook
        fileSystem = new Path(bucketPath).getFileSystem(config);
        LOG.info("Creating " + bucketPath + IN_USE_EXT);
        writer.open(bucketPath + IN_USE_EXT, codeC, compType, formatter);
      }
    }

    resetCounters();
    isOpen = true;
  }

  /**
   * Close the file handle and rename the temp file to the permanent filename.
   * Safe to call multiple times. Logs HDFSWriter.close() exceptions.
   * @throws IOException On failure to rename if temp file exists.
   */
  public synchronized void close() throws IOException {
    LOG.debug("Closing {}", bucketPath + IN_USE_EXT);
    if (isOpen) {
      try {
        writer.close(); // could block
      } catch (IOException e) {
        LOG.warn("failed to close() HDFSWriter for file (" + bucketPath +
            IN_USE_EXT + "). Exception follows.", e);
      }
      isOpen = false;
    } else {
      LOG.info("HDFSWriter is already closed: {}", bucketPath + IN_USE_EXT);
    }
    if (bucketPath != null && fileSystem != null) {
      renameBucket(); // could block or throw IOException
      fileSystem = null;
    }
  }

  /**
   * flush the data
   */
  public synchronized void flush() throws IOException {
    writer.sync(); // could block
    batchCounter = 0;
  }

  /**
   * Open file handles, write data, update stats, handle file rolling and
   * batching / flushing. <br />
   * If the write fails, the file is implicitly closed and then the IOException
   * is rethrown. <br />
   * We rotate before append, and not after, so that the lastRollTime counter
   * that is reset by the open() call approximately reflects when the first
   * event was written to it.
   */
  public synchronized void append(Event event) throws IOException {
    if (!isOpen) {
      open();
    }

    // check if it's time to rotate the file
    if (shouldRotate()) {
      close();
      open();
    }

    // write the event
    try {
      writer.append(event, formatter); // could block
    } catch (IOException e) {
      LOG.warn("Caught IOException writing to HDFSWriter ({}). Closing file (" +
          bucketPath + IN_USE_EXT + ") and rethrowing exception.",
          e.getMessage());
      try {
        close();
      } catch (IOException e2) {
        LOG.warn("Caught IOException while closing file (" +
             bucketPath + IN_USE_EXT + "). Exception follows.", e2);
      }
      throw e;
    }

    // update statistics
    processSize += event.getBody().length;
    eventCounter++;
    batchCounter++;

    if (batchCounter == batchSize) {
      flush();
    }
  }

  /**
   * check if time to rotate the file
   */
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

  /**
   * Rename bucketPath file from .tmp to permanent location.
   */
  private void renameBucket() throws IOException {
    Path srcPath = new Path(bucketPath + IN_USE_EXT);
    Path dstPath = new Path(bucketPath);

    if(fileSystem.exists(srcPath)) { // could block
      LOG.info("Renaming " + srcPath + " to " + dstPath);
      fileSystem.rename(srcPath, dstPath); // could block
    }
  }

  @Override
  public String toString() {
    return "[ " + this.getClass().getSimpleName() + " filePath = " + filePath +
        ", bucketPath = " + bucketPath + " ]";
  }

  public boolean isBatchComplete() {
    return (batchCounter == 0);
  }
}
