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
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

/**
 * Provides Write Ahead Log functionality for a generic Writable. All entries
 * stored in the WAL must be assigned a unique increasing sequence id. WAL
 * files will be removed when the following condition holds (defaults):
 *
 * At least 512MB of WAL's exist, the file in question is greater than
 * five minutes old and the largest committed sequence id is greater
 * than the largest sequence id in the file.
 *
 * <pre>
 *  WAL wal = new WAL(path, Writable.class);
 *  wal.writeEvent(event, 1);
 *  wal.writeEvent(event, 2);
 *  wal.writeSequenceID(1);
 *  wal.writeEvent(event, 3);
 *
 *  System crashes or shuts down...
 *
 *  WAL wal = new WAL(path, Writable.class);
 *  [Event 2, Event 3]  = wal.replay();
 * </pre>
 *
 * WAL files will be created in the specified data directory. They will be
 * rolled at 64MB and deleted five minutes after they are no longer needed.
 * that is the current sequence id) is greater than the greatest sequence id
 *  in the file.
 *
 * The only synchronization this class does is around rolling log files. When
 * a roll of the log file is required, the thread which discovers this
 * will execute the roll. Any threads calling a write*() method during
 * the roll will block until the roll is complete.
 */
public class WAL<T extends Writable> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(WAL.class);

  private File path;
  private File dataPath;
  private File sequenceIDPath;
  private Class<T> clazz;
  private WALDataFile.Writer<T> dataFileWALWriter;
  private WALDataFile.Writer<NullWritable> sequenceIDWALWriter;
  private Map<String, Long> fileLargestSequenceIDMap = Collections
      .synchronizedMap(new HashMap<String, Long>());
  private AtomicLong largestCommitedSequenceID = new AtomicLong(0);
  private volatile boolean rollRequired;
  private volatile boolean rollInProgress;
  private volatile long rollSize;
  private volatile long maxLogsSize;
  private volatile long minLogRetentionPeriod;
  private volatile long workerInterval;
  private int numReplaySequenceIDOverride;
  private Worker backgroundWorker;

  /**
   * Number of bytes before we roll the file.
   */
  public static final long DEFAULT_ROLL_SIZE = 1024L * 1024L * 64L;
  /**
   * Number of bytes, to keep before we start pruning logs.
   */
  public static final long DEFAULT_MAX_LOGS_SIZE = 1024L * 1024L * 512L;
  /**
   * Minimum number of ms to keep a log file.
   */
  public static final long DEFAULT_MIN_LOG_RETENTION_PERIOD = 5L * 60L * 1000L;
  /**
   * How often in ms the background worker runs
   */
  public static final long DEFAULT_WORKER_INTERVAL = 60L * 1000L;

  // used for testing only
  WAL(File path, Class<T> clazz) throws IOException {
    this(path, clazz, DEFAULT_ROLL_SIZE, DEFAULT_MAX_LOGS_SIZE,
        DEFAULT_MIN_LOG_RETENTION_PERIOD, DEFAULT_WORKER_INTERVAL);
  }

  /**
   * Creates a wal object with no defaults, using the specified parameters in
   * the constructor for operation.
   *
   * @param path
   * @param clazz
   * @param rollSize
   *          bytes - max size of a single file before we roll
   * @param maxLogsSize
   *          bytes - total amount of logs to keep excluding the current log
   * @param minLogRentionPeriod
   *          ms - minimum amount of time to keep a log
   * @param workerInterval
   *          ms - how often the background worker checks for old logs
   * @throws IOException
   */
  public WAL(File path, Class<T> clazz, long rollSize,
      long maxLogsSize, long minLogRentionPeriod,
      long workerInterval) throws IOException {
    this.path = path;
    this.rollSize = rollSize;
    this.maxLogsSize = maxLogsSize;
    this.minLogRetentionPeriod = minLogRentionPeriod;
    this.workerInterval = workerInterval;

    StringBuffer buffer = new StringBuffer();
    buffer.append("path = ").append(path).append(", ");
    buffer.append("rollSize = ").append(rollSize).append(", ");
    buffer.append("maxLogsSize = ").append(maxLogsSize).append(", ");
    buffer.append("minLogRentionPeriod = ").append(minLogRentionPeriod).append(", ");
    buffer.append("workerInterval = ").append(workerInterval);
    LOG.info("WAL Parameters: " + buffer);

    File clazzNamePath = new File(path, "clazz");
    createOrDie(path);
    if (clazzNamePath.exists()) {
      String clazzName = Files.readFirstLine(clazzNamePath, Charsets.UTF_8);
      if (!clazzName.equals(clazz.getName())) {
        throw new IOException("WAL is for " + clazzName
            + " and you are passing " + clazz.getName());
      }
    } else {
      Files.write(clazz.getName().getBytes(Charsets.UTF_8), clazzNamePath);
    }

    dataPath = new File(path, "data");
    sequenceIDPath = new File(path, "seq");
    createOrDie(dataPath);
    createOrDie(sequenceIDPath);
    this.clazz = clazz;

    backgroundWorker = new Worker(this);
    backgroundWorker.setName("WAL-Worker-" + path.getAbsolutePath());
    backgroundWorker.setDaemon(true);
    backgroundWorker.start();

    roll();
  }

  private void roll() throws IOException {
    try {
      rollInProgress = true;
      LOG.info("Rolling WAL " + this.path);
      if (dataFileWALWriter != null) {
        fileLargestSequenceIDMap.put(dataFileWALWriter.getPath()
            .getAbsolutePath(), dataFileWALWriter.getLargestSequenceID());
        dataFileWALWriter.close();
      }
      if (sequenceIDWALWriter != null) {
        fileLargestSequenceIDMap.put(sequenceIDWALWriter.getPath()
            .getAbsolutePath(), sequenceIDWALWriter.getLargestSequenceID());
        sequenceIDWALWriter.close();
      }
      long ts = System.currentTimeMillis();
      File dataWalFileName = new File(dataPath, Long.toString(ts));
      File seqWalFileName = new File(sequenceIDPath, Long.toString(ts));
      while (dataWalFileName.exists() || seqWalFileName.exists()) {
        ts++;
        dataWalFileName = new File(dataPath, Long.toString(ts));
        seqWalFileName = new File(sequenceIDPath, Long.toString(ts));
      }

      dataFileWALWriter = new WALDataFile.Writer<T>(dataWalFileName);
      sequenceIDWALWriter = new WALDataFile.Writer<NullWritable>(seqWalFileName);
      rollRequired = false;
    } finally {
      rollInProgress = false;
      // already have lock but is more clear
      synchronized (this) {
        notifyAll();
      }
    }
  }

  public WALReplayResult<T> replay() throws IOException {
    final AtomicLong sequenceID = new AtomicLong(0);
    final Map<String, Long> fileLargestSequenceIDMap = Maps.newHashMap();
    final AtomicLong totalBytes = new AtomicLong(0);
    // first get the total amount of data we have to read in
    readFiles(sequenceIDPath, new Function<File, Void>() {
      @Override
      public Void apply(File input) {
        totalBytes.addAndGet(input.length());
        return null;
      }
    });
    // then estimate the size of the array
    // needed to hold all the sequence ids
    int baseSize = WALEntry.getBaseSize();
    int numEntries = Math.max((int)((totalBytes.get() / baseSize) * 1.05f) + 1,
        numReplaySequenceIDOverride);
    LOG.info("Replay assumptions: baseSize = " + baseSize
        + ", estimatedNumEntries " + numEntries);
    final SequenceIDBuffer sequenceIDs = new SequenceIDBuffer(numEntries);

    // read them all into ram
    final AtomicInteger index = new AtomicInteger(0);
    readFiles(sequenceIDPath, new Function<File, Void>() {
      @Override
      public Void apply(File input) {
        LOG.info("Replaying " + input);
        WALDataFile.Reader<NullWritable> reader = null;
        int localIndex = index.get();
        try {
          // item stored is a NullWritable so we only store the base WALEntry
          reader = new WALDataFile.Reader<NullWritable>(input,
              NullWritable.class);
          List<WALEntry<NullWritable>> batch;
          long largestForFile = Long.MIN_VALUE;
          while ((batch = reader.nextBatch()) != null) {
            for(WALEntry<NullWritable> entry : batch) {
              long current = entry.getSequenceID();
              sequenceIDs.put(localIndex++, current);
              largestForFile = Math.max(largestForFile, current);
            }
          }
          sequenceID.set(Math.max(largestForFile, sequenceID.get()));
          fileLargestSequenceIDMap.put(input.getAbsolutePath(),
              largestForFile);
        } catch (IOException e) {
          Throwables.propagate(e);
        } finally {
          index.set(localIndex);
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException e) {
            }
          }
        }
        return null;
      }
    });

    sequenceIDs.sort();

    // now read all edits storing items with a sequence id
    // which is *not* in the sequenceIDs
    final List<WALEntry<T>> entries = Lists.newArrayList();
    final Class<T> dataClazz = clazz;
    readFiles(dataPath, new Function<File, Void>() {
      @Override
      public Void apply(File input) {
        LOG.info("Replaying " + input);
        WALDataFile.Reader<T> reader = null;
        try {
          reader = new WALDataFile.Reader<T>(input, dataClazz);
          List<WALEntry<T>> batch = Lists.newArrayList();
          long largestForFile = Long.MIN_VALUE;
          while ((batch = reader.nextBatch()) != null) {
            for(WALEntry<T> entry : batch) {
              long current = entry.getSequenceID();
              if (!sequenceIDs.exists(current)) {
                entries.add(entry);
              }
              largestForFile = Math.max(largestForFile, current);
            }
          }
          sequenceID.set(Math.max(largestForFile, sequenceID.get()));
          fileLargestSequenceIDMap.put(input.getAbsolutePath(),
              largestForFile);
        } catch (IOException e) {
          Throwables.propagate(e);
        } finally {
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException e) {
            }
          }
        }
        return null;
      }
    });
    sequenceIDs.close();
    synchronized (this.fileLargestSequenceIDMap) {
      this.fileLargestSequenceIDMap.clear();
      this.fileLargestSequenceIDMap.putAll(fileLargestSequenceIDMap);
      LOG.info("SequenceIDMap " + fileLargestSequenceIDMap);
    }
    largestCommitedSequenceID.set(sequenceID.get());
    LOG.info("Replay complete: LargestCommitedSequenceID = " + largestCommitedSequenceID.get());
    return new WALReplayResult<T>(entries, largestCommitedSequenceID.get());
  }

  public void writeEntries(List<WALEntry<T>> entries) throws IOException {
    Preconditions.checkNotNull(dataFileWALWriter,
        "Write is null, close must have been called");
    synchronized (this) {
      if (isRollRequired()) {
        roll();
      }
    }
    waitWhileRolling();
    boolean error = true;
    try {
      dataFileWALWriter.append(entries);
      error = false;
    } finally {
      if (error) {
        rollRequired = true;
      }
    }
  }

  public void writeEntry(WALEntry<T> entry) throws IOException {
    List<WALEntry<T>> entries = Lists.newArrayList();
    entries.add(entry);
    writeEntries(entries);
  }

  public void writeSequenceID(long sequenceID) throws IOException {
    List<Long> sequenceIDs = Lists.newArrayList();
    sequenceIDs.add(sequenceID);
    writeSequenceIDs(sequenceIDs);
  }
  public void writeSequenceIDs(List<Long> sequenceIDs) throws IOException {
    Preconditions.checkNotNull(sequenceIDWALWriter,
        "Write is null, close must have been called");
    synchronized (this) {
      if (isRollRequired()) {
        roll();
      }
    }
    waitWhileRolling();
    boolean error = true;
    try {
      List<WALEntry<NullWritable>> entries = Lists.newArrayList();
      for(Long sequenceID : sequenceIDs) {
      largestCommitedSequenceID.set(Math.max(sequenceID,
          largestCommitedSequenceID.get()));
      entries.add(new WALEntry<NullWritable>(NullWritable.get(), sequenceID));
      sequenceIDWALWriter.append(entries);
      }
      error = false;
    } finally {
      if (error) {
        rollRequired = true;
      }
    }
  }

  private void waitWhileRolling() {
    synchronized (this) {
      while (rollInProgress) {
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (backgroundWorker != null) {
      backgroundWorker.shutdown();
    }
    if (sequenceIDWALWriter != null) {
      sequenceIDWALWriter.close();
      sequenceIDWALWriter = null;
    }
    if (dataFileWALWriter != null) {
      dataFileWALWriter.close();
      dataFileWALWriter = null;
    }
  }

  private boolean isRollRequired() throws IOException {
    if (rollRequired) {
      return true;
    }
    return Math.max(dataFileWALWriter.getSize(), sequenceIDWALWriter.getSize()) > rollSize;
  }

  private void readFiles(File path, Function<File, Void> function)
      throws IOException {
    File[] dataFiles = path.listFiles();
    List<File> files = Lists.newArrayList();
    if (dataFiles != null) {
      for (File dataFile : dataFiles) {
        if (!dataFile.isFile()) {
          throw new IOException("Not file " + dataFile);
        }
        files.add(dataFile);
      }
    }
    for (File dataFile : files) {
      function.apply(dataFile);
    }
  }

  private void createOrDie(File path) throws IOException {
    if (!path.isDirectory()) {
      if (!path.mkdirs()) {
        throw new IOException("Unable to create " + path);
      }
    }
  }

  private static class Worker extends Thread {
    private WAL<? extends Writable> wal;
    private volatile boolean run = true;
    public Worker(WAL<? extends Writable> wal) {
      this.wal = wal;
    }

    @Override
    public void run() {
      LOG.info("Background worker reporting for duty");
      while (run) {
        try {
          try {
            Thread.sleep(wal.workerInterval);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          if (!run) {
            continue;
          }
          List<String> filesToRemove = Lists.newArrayList();
          long totalSize = 0;
          synchronized (wal.fileLargestSequenceIDMap) {
            for (String key : wal.fileLargestSequenceIDMap.keySet()) {
              File file = new File(key);
              totalSize += file.length();
            }
            if (totalSize >= wal.maxLogsSize) {
              for (String key : wal.fileLargestSequenceIDMap.keySet()) {
                File file = new File(key);
                Long seqid = wal.fileLargestSequenceIDMap.get(key);
                long largestCommitedSeqID = wal.largestCommitedSequenceID.get();
                if (file.exists()
                    // has not been modified in 5 minutes
                    && System.currentTimeMillis() - file.lastModified() > wal.minLogRetentionPeriod
                    // current seqid is greater than the largest seqid in the file
                    && largestCommitedSeqID > seqid) {
                  filesToRemove.add(key);
                  LOG.info("Removing expired file " + key + ", seqid = "
                      + seqid + ", result = " + file.delete());
                }
              }
              for (String key : filesToRemove) {
                wal.fileLargestSequenceIDMap.remove(key);
              }
            }
          }
        } catch (Exception ex) {
          LOG.error("Uncaught exception in background worker", ex);
        }
      }
      LOG.warn(this.getClass().getSimpleName()
          + " moving on due to stop request");
    }

    public void shutdown() {
      run = false;
      this.interrupt();
    }
  }


  public void setRollSize(long rollSize) {
    this.rollSize = rollSize;
  }

  public void setMaxLogsSize(long maxLogsSize) {
    this.maxLogsSize = maxLogsSize;
  }

  public void setMinLogRetentionPeriod(long minLogRetentionPeriod) {
    this.minLogRetentionPeriod = minLogRetentionPeriod;
  }

  public void setWorkerInterval(long workerInterval) {
    this.workerInterval = workerInterval;
  }

  /**
   * Reads in a WAL and writes out a new WAL. Used if for some reason a replay
   * cannot occur due to the size of the WAL or assumptions about the number of
   * sequenceids.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public static void main(String[] args) throws IOException,
  ClassNotFoundException {
    Preconditions.checkPositionIndex(0, args.length,
        "input directory is a required arg");
    Preconditions.checkPositionIndex(1, args.length,
        "output directory is a required arg");
    Preconditions.checkPositionIndex(2, args.length,
        "classname is a required arg");
    String input = args[0];
    String output = args[1];
    Class clazz = Class.forName(args[2].trim());
    WAL inputWAL = new WAL(new File(input), clazz);
    if (args.length == 4) {
      inputWAL.numReplaySequenceIDOverride = Integer.parseInt(args[3]);
      System.out.println("Overridng numReplaySequenceIDOverride: "
          + inputWAL.numReplaySequenceIDOverride);
    }
    WALReplayResult<?> result = inputWAL.replay();
    inputWAL.close();
    System.out.println("     SeqID: " + result.getSequenceID());
    System.out.println("NumEntries: " + result.getResults().size());
    WAL outputWAL = new WAL(new File(output), clazz);
    outputWAL.writeEntries(result.getResults());
    outputWAL.close();
  }
}
