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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.FlumeFormatter;
import org.apache.flume.sink.hdfs.BucketWriter.BucketFlushStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSEventSink extends AbstractSink implements Configurable {
  private static final Logger LOG = LoggerFactory
      .getLogger(HDFSEventSink.class);

  static final long defaultRollInterval = 30;
  static final long defaultRollSize = 1024;
  static final long defaultRollCount = 10;
  static final String defaultFileName = "FlumeData";
  static final String defaultBucketFormat = "%yyyy-%mm-%dd/%HH";
  static final long defaultBatchSize = 1;
  static final long defaultTxnEventMax = 100;
  static final String defaultFileType = HDFSWriterFactory.SequenceFileType;
  static final int defaultMaxOpenFiles = 5000;
  static final String defaultWriteFormat = HDFSFormatterFactory.hdfsWritableFormat;
  static final long defaultAppendTimeout = 1000;

  private long rollInterval;
  private long rollSize;
  private long rollCount;
  private long txnEventMax;
  private long batchSize;
  private CompressionCodec codeC;
  private CompressionType compType;
  private String fileType;
  private String path;
  private int maxOpenFiles;
  private String writeFormat;
  private HDFSWriterFactory myWriterFactory;
  private ExecutorService executor;
  private long appendTimeout;

  /*
   * Extended Java LinkedHashMap for open file handle LRU queue We want to clear
   * the oldest file handle if there are too many open ones
   */
  private class WriterLinkedHashMap extends LinkedHashMap<String, BucketWriter> {
    private static final long serialVersionUID = 1L;

    protected boolean removeEldestEntry(Entry<String, BucketWriter> eldest) {
      /*
       * FIXME: We probably shouldn't shared state this way. Make this class
       * private static and explicitly expose maxOpenFiles.
       */
      if (super.size() > maxOpenFiles) {
        // If we have more that max open files, then close the last one and
        // return true
        try {
          eldest.getValue().close();
        } catch (IOException eI) {
          LOG.warn(eldest.getKey().toString(), eI);
        }
        return true;
      } else {
        return false;
      }
    }
  }

  final WriterLinkedHashMap sfWriters = new WriterLinkedHashMap();

  // Used to short-circuit around doing regex matches when we know there are
  // no templates to be replaced.
  // private boolean shouldSub = false;

  public HDFSEventSink() {
    myWriterFactory = new HDFSWriterFactory();
  }
  
  public HDFSEventSink(HDFSWriterFactory newWriterFactory) {
    myWriterFactory = newWriterFactory;
  }

  // read configuration and setup thresholds
  @Override
  public void configure(Context context) {
    String dirpath = context.getString("hdfs.path");
    String fileName = context.getString("hdfs.filePrefix");
    String rollInterval = context.getString("hdfs.rollInterval");
    String rollSize = context.getString("hdfs.rollSize");
    String rollCount = context.getString("hdfs.rollCount");
    String batchSize = context.getString("hdfs.batchSize");
    String txnEventMax = context.getString("hdfs.txnEventMax");
    String codecName = context.getString("hdfs.codeC");
    String fileType = context.getString("hdfs.fileType");
    String maxOpenFiles = context.getString("hdfs.maxOpenFiles");
    String writeFormat = context.getString("hdfs.writeFormat");
    String appendTimeout = context.getString("hdfs.appendTimeout");

    if (fileName == null)
      fileName = defaultFileName;
    // FIXME: Not transportable.
    this.path = new String(dirpath + "/" + fileName);

    if (rollInterval == null) {
      this.rollInterval = defaultRollInterval;
    } else {
      this.rollInterval = Long.parseLong(rollInterval);
    }

    if (rollSize == null) {
      this.rollSize = defaultRollSize;
    } else {
      this.rollSize = Long.parseLong(rollSize);
    }

    if (rollCount == null) {
      this.rollCount = defaultRollCount;
    } else {
      this.rollCount = Long.parseLong(rollCount);
    }

    if ((batchSize == null) || batchSize.equals("0")) {
      this.batchSize = defaultBatchSize;
    } else {
      this.batchSize = Long.parseLong(batchSize);
    }

    if ((txnEventMax == null) || txnEventMax.equals("0")) {
      this.txnEventMax = defaultTxnEventMax;
    } else {
      this.txnEventMax = Long.parseLong(txnEventMax);
    }

    if (codecName == null) {
      codeC = null;
      compType = CompressionType.NONE;
    } else {
      codeC = getCodec(codecName);
      // TODO : set proper compression type
      compType = CompressionType.BLOCK;
    }

    if (fileType == null) {
      this.fileType = defaultFileType;
    } else {
      this.fileType = fileType;
    }

    if (maxOpenFiles == null) {
      this.maxOpenFiles = defaultMaxOpenFiles;
    } else {
      this.maxOpenFiles = Integer.parseInt(maxOpenFiles);
    }

    if (writeFormat == null) {
      this.writeFormat = defaultWriteFormat;
    } else {
      this.writeFormat = writeFormat;
    }

    if (appendTimeout == null) {
      this.appendTimeout = defaultAppendTimeout;
    } else {
      this.appendTimeout = Long.parseLong(appendTimeout);
    }
  }

  private static boolean codecMatches(Class<? extends CompressionCodec> cls,
      String codecName) {
    String simpleName = cls.getSimpleName();
    if (cls.getName().equals(codecName)
        || simpleName.equalsIgnoreCase(codecName)) {
      return true;
    }
    if (simpleName.endsWith("Codec")) {
      String prefix = simpleName.substring(0,
          simpleName.length() - "Codec".length());
      if (prefix.equalsIgnoreCase(codecName)) {
        return true;
      }
    }
    return false;
  }

  private static CompressionCodec getCodec(String codecName) {
    Configuration conf = new Configuration();
    List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory
        .getCodecClasses(conf);
    // Wish we could base this on DefaultCodec but appears not all codec's
    // extend DefaultCodec(Lzo)
    CompressionCodec codec = null;
    ArrayList<String> codecStrs = new ArrayList<String>();
    codecStrs.add("None");
    for (Class<? extends CompressionCodec> cls : codecs) {
      codecStrs.add(cls.getSimpleName());

      if (codecMatches(cls, codecName)) {
        try {
          codec = cls.newInstance();
        } catch (InstantiationException e) {
          LOG.error("Unable to instantiate " + cls + " class");
        } catch (IllegalAccessException e) {
          LOG.error("Unable to access " + cls + " class");
        }
      }
    }

    if (codec == null) {
      if (!codecName.equalsIgnoreCase("None")) {
        throw new IllegalArgumentException("Unsupported compression codec "
            + codecName + ".  Please choose from: " + codecStrs);
      }
    } else if (codec instanceof org.apache.hadoop.conf.Configurable) {
      // Must check instanceof codec as BZip2Codec doesn't inherit Configurable
      // Must set the configuration for Configurable objects that may or do use
      // native libs
      ((org.apache.hadoop.conf.Configurable) codec).setConf(conf);
    }
    return codec;
  }

  /* 
   * Execute the append on a separate thread and wait for the completion for the specified amount of time
   * In case of timeout, cancel the append and throw an IOException
   */
  private BucketFlushStatus backgroundAppend(final BucketWriter bw, final Event e) throws IOException, InterruptedException {
    Future<BucketFlushStatus> future = executor.submit(new Callable<BucketFlushStatus>() {
      public BucketFlushStatus call() throws Exception {
        return bw.append(e);
      }
    });

    try {
      if (appendTimeout > 0) {
        return future.get(appendTimeout, TimeUnit.MILLISECONDS);
      } else {
        return future.get();
      }
    } catch (TimeoutException eT) {
      future.cancel(true);
      throw new IOException("Append timed out", eT);
    } catch (ExecutionException e1) {
      Throwable cause = e1.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        // we have a throwable that is not an exception. (such as a
        // NoClassDefFoundError)
        LOG.error("Got a throwable that is not an exception! Bailing out!",
            e1.getCause());
        throw new RuntimeException(e1.getCause());
      }
    } catch (CancellationException ce) {
      throw new InterruptedException(
          "Blocked append interrupted by rotation event");
    } catch (InterruptedException ex) {
      LOG.warn("Unexpected Exception " + ex.getMessage(), ex);
      throw (InterruptedException) ex;
    }
  }

  /**
   * Pull events out of channel and send it to HDFS - take at the most
   * txnEventMax, that's the maximum #events to hold in channel for a given
   * transaction - find the corresponding bucket for the event, ensure the file
   * is open - extract the pay-load and append to HDFS file
   */
  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Map<String, BucketWriter> batchMap = new HashMap<String, BucketWriter>();
    BucketFlushStatus syncedUp;

    try {
      transaction.begin();
      Event event = null;
      for (int txnEventCount = 0; txnEventCount < txnEventMax; txnEventCount++) {
        event = null;
        event = channel.take();
        if (event == null) {
          break;
        }

        // reconstruct the path name by substituting place holders
        String realPath = BucketPath.escapeString(path, event.getHeaders());
        BucketWriter bw = sfWriters.get(realPath);

        // we haven't seen this file yet, so open it and cache the handle
        if (bw == null) {
          HDFSWriter writer = myWriterFactory.getWriter(fileType);
          FlumeFormatter formatter = HDFSFormatterFactory
              .getFormatter(writeFormat);
          bw = new BucketWriter(rollInterval, rollSize, rollCount, batchSize);
          bw.open(realPath, codeC, compType, writer, formatter);
          sfWriters.put(realPath, bw);
        }

        // Write the data to HDFS
        syncedUp = backgroundAppend(bw, event);

        // keep track of the files in current batch that are not flushed
        // we need to flush all those at the end of the transaction
        if (syncedUp == BucketFlushStatus.BatchStarted)
          batchMap.put(bw.getFilePath(), bw);
        else if ((batchSize > 1)
            && (syncedUp == BucketFlushStatus.BatchFlushed))
          batchMap.remove(bw.getFilePath());
      }

      // flush any pending writes in the given transaction
      for (Entry<String, BucketWriter> e : batchMap.entrySet()) {
        e.getValue().flush();
      }
      batchMap.clear();
      transaction.commit();
      if(event == null) {
        return Status.BACKOFF;
      }
      return Status.READY;
    } catch (IOException eIO) {
      transaction.rollback();
      LOG.warn("HDFS IO error", eIO);
      return Status.BACKOFF;
    } catch (Exception e) {
      transaction.rollback();
      LOG.error("process failed", e);
      throw new EventDeliveryException(e.getMessage());
    } finally {
      // clear any leftover writes in the given transaction
      for (Entry<String, BucketWriter> e : batchMap.entrySet()) {
        e.getValue().abort();
      }
      transaction.close();
    }
  }

  @Override
  public void stop() {
    try {
      for (Entry<String, BucketWriter> e : sfWriters.entrySet()) {
        LOG.info("Closing " + e.getKey());
        e.getValue().close();
      }
    } catch (IOException eIO) {
      LOG.warn("IOException in opening file", eIO);
    }
    executor.shutdown();
    try {
      while (executor.isTerminated() == false) {
        executor.awaitTermination(defaultAppendTimeout, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException ex) {
      LOG.warn("shutdown interrupted" + ex.getMessage(), ex);
    }

    executor = null;
    super.stop();
  }

  @Override
  public void start() {
    executor = Executors.newFixedThreadPool(1);
    for (Entry<String, BucketWriter> e : sfWriters.entrySet()) {
      try {
        e.getValue().open();
      } catch (IOException eIO) {
        LOG.warn("IOException in opening file", eIO);
      }
    }
    super.start();
  }

}