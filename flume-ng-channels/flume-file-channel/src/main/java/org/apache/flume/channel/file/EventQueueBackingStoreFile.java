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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;


abstract class EventQueueBackingStoreFile extends EventQueueBackingStore {
  private static final Logger LOG = LoggerFactory
      .getLogger(EventQueueBackingStoreFile.class);
  private static final int MAX_ALLOC_BUFFER_SIZE = 2*1024*1024; // 2MB
  protected static final int HEADER_SIZE = 1029;
  protected static final int INDEX_VERSION = 0;
  protected static final int INDEX_WRITE_ORDER_ID = 1;
  protected static final int INDEX_CHECKPOINT_MARKER = 4;
  protected static final int CHECKPOINT_COMPLETE = 0;
  protected static final int CHECKPOINT_INCOMPLETE = 1;

  protected static final String COMPRESSED_FILE_EXTENSION = ".snappy";

  protected LongBuffer elementsBuffer;
  protected final Map<Integer, Long> overwriteMap = new HashMap<Integer, Long>();
  protected final Map<Integer, AtomicInteger> logFileIDReferenceCounts = Maps.newHashMap();
  protected final MappedByteBuffer mappedBuffer;
  protected final RandomAccessFile checkpointFileHandle;
  protected final File checkpointFile;
  private final Semaphore backupCompletedSema = new Semaphore(1);
  protected final boolean shouldBackup;
  protected final boolean compressBackup;
  private final File backupDir;
  private final ExecutorService checkpointBackUpExecutor;

  protected EventQueueBackingStoreFile(int capacity, String name,
      File checkpointFile) throws IOException,
      BadCheckpointException {
    this(capacity, name, checkpointFile, null, false, false);
  }

  protected EventQueueBackingStoreFile(int capacity, String name,
      File checkpointFile, File checkpointBackupDir,
      boolean backupCheckpoint, boolean compressBackup)
    throws IOException, BadCheckpointException {
    super(capacity, name);
    this.checkpointFile = checkpointFile;
    this.shouldBackup = backupCheckpoint;
    this.compressBackup = compressBackup;
    this.backupDir = checkpointBackupDir;
    checkpointFileHandle = new RandomAccessFile(checkpointFile, "rw");
    long totalBytes = (capacity + HEADER_SIZE) * Serialization.SIZE_OF_LONG;
    if(checkpointFileHandle.length() == 0) {
      allocate(checkpointFile, totalBytes);
      checkpointFileHandle.seek(INDEX_VERSION * Serialization.SIZE_OF_LONG);
      checkpointFileHandle.writeLong(getVersion());
      checkpointFileHandle.getChannel().force(true);
      LOG.info("Preallocated " + checkpointFile + " to " + checkpointFileHandle.length()
          + " for capacity " + capacity);
    }
    if(checkpointFile.length() != totalBytes) {
      String msg = "Configured capacity is " + capacity + " but the "
          + " checkpoint file capacity is " +
          ((checkpointFile.length() / Serialization.SIZE_OF_LONG) - HEADER_SIZE)
          + ". See FileChannel documentation on how to change a channels" +
          " capacity.";
      throw new BadCheckpointException(msg);
    }
    mappedBuffer = checkpointFileHandle.getChannel().map(MapMode.READ_WRITE, 0,
        checkpointFile.length());
    elementsBuffer = mappedBuffer.asLongBuffer();

    long version = elementsBuffer.get(INDEX_VERSION);
    if(version != (long) getVersion()) {
      throw new BadCheckpointException("Invalid version: " + version + " " +
              name + ", expected " + getVersion());
    }
    long checkpointComplete = elementsBuffer.get(INDEX_CHECKPOINT_MARKER);
    if(checkpointComplete != (long) CHECKPOINT_COMPLETE) {
      throw new BadCheckpointException("Checkpoint was not completed correctly,"
              + " probably because the agent stopped while the channel was"
              + " checkpointing.");
    }
    if (shouldBackup) {
      checkpointBackUpExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat(
          getName() + " - CheckpointBackUpThread").build());
    } else {
      checkpointBackUpExecutor = null;
    }
  }

  protected long getCheckpointLogWriteOrderID() {
    return elementsBuffer.get(INDEX_WRITE_ORDER_ID);
  }

  protected abstract void writeCheckpointMetaData() throws IOException;

  /**
   * This method backs up the checkpoint and its metadata files. This method
   * is called once the checkpoint is completely written and is called
   * from a separate thread which runs in the background while the file channel
   * continues operation.
   *
   * @param backupDirectory - the directory to which the backup files should be
   *                        copied.
   * @throws IOException - if the copy failed, or if there is not enough disk
   * space to copy the checkpoint files over.
   */
  protected void backupCheckpoint(File backupDirectory) throws IOException {
    int availablePermits = backupCompletedSema.drainPermits();
    Preconditions.checkState(availablePermits == 0,
      "Expected no permits to be available in the backup semaphore, " +
        "but " + availablePermits + " permits were available.");
    if (slowdownBackup) {
      try {
        TimeUnit.SECONDS.sleep(10);
      } catch (Exception ex) {
        Throwables.propagate(ex);
      }
    }
    File backupFile = new File(backupDirectory, BACKUP_COMPLETE_FILENAME);
    if (backupExists(backupDirectory)) {
      if (!backupFile.delete()) {
        throw new IOException("Error while doing backup of checkpoint. Could " +
          "not remove" + backupFile.toString() + ".");
      }
    }
    Serialization.deleteAllFiles(backupDirectory, Log.EXCLUDES);
    File checkpointDir = checkpointFile.getParentFile();
    File[] checkpointFiles = checkpointDir.listFiles();
    Preconditions.checkNotNull(checkpointFiles, "Could not retrieve files " +
      "from the checkpoint directory. Cannot complete backup of the " +
      "checkpoint.");
    for (File origFile : checkpointFiles) {
      if(Log.EXCLUDES.contains(origFile.getName())) {
        continue;
      }
      if (compressBackup && origFile.equals(checkpointFile)) {
        Serialization.compressFile(origFile, new File(backupDirectory,
          origFile.getName() + COMPRESSED_FILE_EXTENSION));
      } else {
        Serialization.copyFile(origFile, new File(backupDirectory,
          origFile.getName()));
      }
    }
    Preconditions.checkState(!backupFile.exists(), "The backup file exists " +
      "while it is not supposed to. Are multiple channels configured to use " +
      "this directory: " + backupDirectory.toString() + " as backup?");
    if (!backupFile.createNewFile()) {
      LOG.error("Could not create backup file. Backup of checkpoint will " +
        "not be used during replay even if checkpoint is bad.");
    }
  }

  /**
   * Restore the checkpoint, if it is found to be bad.
   * @return true - if the previous backup was successfully completed and
   * restore was successfully completed.
   * @throws IOException - If restore failed due to IOException
   *
   */
  public static boolean restoreBackup(File checkpointDir, File backupDir)
    throws IOException {
    if (!backupExists(backupDir)) {
      return false;
    }
    Serialization.deleteAllFiles(checkpointDir, Log.EXCLUDES);
    File[] backupFiles = backupDir.listFiles();
    if (backupFiles == null) {
      return false;
    } else {
      for (File backupFile : backupFiles) {
        String fileName = backupFile.getName();
        if (!fileName.equals(BACKUP_COMPLETE_FILENAME) &&
          !fileName.equals(Log.FILE_LOCK)) {
          if (fileName.endsWith(COMPRESSED_FILE_EXTENSION)){
            Serialization.decompressFile(
              backupFile, new File(checkpointDir,
              fileName.substring(0, fileName.lastIndexOf("."))));
          } else {
            Serialization.copyFile(backupFile, new File(checkpointDir,
              fileName));
          }
        }
      }
      return true;
    }
  }

  @Override
  void beginCheckpoint() throws IOException {
    LOG.info("Start checkpoint for " + checkpointFile +
        ", elements to sync = " + overwriteMap.size());

    if (shouldBackup) {
      int permits = backupCompletedSema.drainPermits();
      Preconditions.checkState(permits <= 1, "Expected only one or less " +
        "permits to checkpoint, but got " + String.valueOf(permits) +
        " permits");
      if(permits < 1) {
        // Force the checkpoint to not happen by throwing an exception.
        throw new IOException("Previous backup of checkpoint files is still " +
          "in progress. Will attempt to checkpoint only at the end of the " +
          "next checkpoint interval. Try increasing the checkpoint interval " +
          "if this error happens often.");
      }
    }
    // Start checkpoint
    elementsBuffer.put(INDEX_CHECKPOINT_MARKER, CHECKPOINT_INCOMPLETE);
    mappedBuffer.force();
  }

  @Override
  void checkpoint()  throws IOException {

    setLogWriteOrderID(WriteOrderOracle.next());
    LOG.info("Updating checkpoint metadata: logWriteOrderID: "
        + getLogWriteOrderID() + ", queueSize: " + getSize() + ", queueHead: "
          + getHead());
    elementsBuffer.put(INDEX_WRITE_ORDER_ID, getLogWriteOrderID());
    try {
      writeCheckpointMetaData();
    } catch (IOException e) {
      throw new IOException("Error writing metadata", e);
    }

    Iterator<Integer> it = overwriteMap.keySet().iterator();
    while (it.hasNext()) {
      int index = it.next();
      long value = overwriteMap.get(index);
      elementsBuffer.put(index, value);
      it.remove();
    }

    Preconditions.checkState(overwriteMap.isEmpty(),
        "concurrent update detected ");

    // Finish checkpoint
    elementsBuffer.put(INDEX_CHECKPOINT_MARKER, CHECKPOINT_COMPLETE);
    mappedBuffer.force();
    if (shouldBackup) {
      startBackupThread();
    }
  }

  /**
   * This method starts backing up the checkpoint in the background.
   */
  private void startBackupThread() {
    Preconditions.checkNotNull(checkpointBackUpExecutor,
      "Expected the checkpoint backup exector to be non-null, " +
        "but it is null. Checkpoint will not be backed up.");
    LOG.info("Attempting to back up checkpoint.");
    checkpointBackUpExecutor.submit(new Runnable() {

      @Override
      public void run() {
        boolean error = false;
        try {
          backupCheckpoint(backupDir);
        } catch (Throwable throwable) {
          error = true;
          LOG.error("Backing up of checkpoint directory failed.", throwable);
        } finally {
          backupCompletedSema.release();
        }
        if (!error) {
          LOG.info("Checkpoint backup completed.");
        }
      }
    });
  }

  @Override
  void close() {
    mappedBuffer.force();
    try {
      checkpointFileHandle.close();
    } catch (IOException e) {
      LOG.info("Error closing " + checkpointFile, e);
    }
    if(checkpointBackUpExecutor != null && !checkpointBackUpExecutor
      .isShutdown()) {
      checkpointBackUpExecutor.shutdown();
      try {
        // Wait till the executor dies.
        while (!checkpointBackUpExecutor.awaitTermination(1,
          TimeUnit.SECONDS));
      } catch (InterruptedException ex) {
        LOG.warn("Interrupted while waiting for checkpoint backup to " +
          "complete");
      }
    }
  }

  @Override
  long get(int index) {
    int realIndex = getPhysicalIndex(index);
    long result = EMPTY;
    if (overwriteMap.containsKey(realIndex)) {
      result = overwriteMap.get(realIndex);
    } else {
      result = elementsBuffer.get(realIndex);
    }
    return result;
  }

  @Override
  ImmutableSortedSet<Integer> getReferenceCounts() {
    return ImmutableSortedSet.copyOf(logFileIDReferenceCounts.keySet());
  }

  @Override
  void put(int index, long value) {
    int realIndex = getPhysicalIndex(index);
    overwriteMap.put(realIndex, value);
  }

  @Override
  boolean syncRequired() {
    return overwriteMap.size() > 0;
  }

  @Override
  protected void incrementFileID(int fileID) {
    AtomicInteger counter = logFileIDReferenceCounts.get(fileID);
    if(counter == null) {
      counter = new AtomicInteger(0);
      logFileIDReferenceCounts.put(fileID, counter);
    }
    counter.incrementAndGet();
  }
  @Override
  protected void decrementFileID(int fileID) {
    AtomicInteger counter = logFileIDReferenceCounts.get(fileID);
    Preconditions.checkState(counter != null, "null counter ");
    int count = counter.decrementAndGet();
    if(count == 0) {
      logFileIDReferenceCounts.remove(fileID);
    }
  }

  protected int getPhysicalIndex(int index) {
    return HEADER_SIZE + (getHead() + index) % getCapacity();
  }

  protected static void allocate(File file, long totalBytes) throws IOException {
    RandomAccessFile checkpointFile = new RandomAccessFile(file, "rw");
    boolean success = false;
    try {
      if (totalBytes <= MAX_ALLOC_BUFFER_SIZE) {
        /*
         * totalBytes <= MAX_ALLOC_BUFFER_SIZE, so this can be cast to int
         * without a problem.
         */
        checkpointFile.write(new byte[(int)totalBytes]);
      } else {
        byte[] initBuffer = new byte[MAX_ALLOC_BUFFER_SIZE];
        long remainingBytes = totalBytes;
        while (remainingBytes >= MAX_ALLOC_BUFFER_SIZE) {
          checkpointFile.write(initBuffer);
          remainingBytes -= MAX_ALLOC_BUFFER_SIZE;
        }
        /*
         * At this point, remainingBytes is < MAX_ALLOC_BUFFER_SIZE,
         * so casting to int is fine.
         */
        if (remainingBytes > 0) {
          checkpointFile.write(initBuffer, 0, (int)remainingBytes);
        }
      }
      success = true;
    } finally {
      try {
        checkpointFile.close();
      } catch (IOException e) {
        if(success) {
          throw e;
        }
      }
    }
  }

  public static boolean backupExists(File backupDir) {
    return new File(backupDir, BACKUP_COMPLETE_FILENAME).exists();
  }

  public static void main(String[] args) throws Exception {
    File file = new File(args[0]);
    File inflightTakesFile = new File(args[1]);
    File inflightPutsFile = new File(args[2]);
    File queueSetDir = new File(args[3]);
    if (!file.exists()) {
      throw new IOException("File " + file + " does not exist");
    }
    if (file.length() == 0) {
      throw new IOException("File " + file + " is empty");
    }
    int capacity = (int) ((file.length() - (HEADER_SIZE * 8L)) / 8L);
    EventQueueBackingStoreFile backingStore = (EventQueueBackingStoreFile)
        EventQueueBackingStoreFactory.get(file,capacity, "debug", false);
    System.out.println("File Reference Counts"
            + backingStore.logFileIDReferenceCounts);
    System.out.println("Queue Capacity " + backingStore.getCapacity());
    System.out.println("Queue Size " + backingStore.getSize());
    System.out.println("Queue Head " + backingStore.getHead());
    for (int index = 0; index < backingStore.getCapacity(); index++) {
      long value = backingStore.get(backingStore.getPhysicalIndex(index));
      int fileID = (int) (value >>> 32);
      int offset = (int) value;
      System.out.println(index + ":" + Long.toHexString(value) + " fileID = "
              + fileID + ", offset = " + offset);
    }
    FlumeEventQueue queue =
        new FlumeEventQueue(backingStore, inflightTakesFile, inflightPutsFile,
            queueSetDir);
    SetMultimap<Long, Long> putMap = queue.deserializeInflightPuts();
    System.out.println("Inflight Puts:");

    for (Long txnID : putMap.keySet()) {
      Set<Long> puts = putMap.get(txnID);
      System.out.println("Transaction ID: " + String.valueOf(txnID));
      for (long value : puts) {
        int fileID = (int) (value >>> 32);
        int offset = (int) value;
        System.out.println(Long.toHexString(value) + " fileID = "
                + fileID + ", offset = " + offset);
      }
    }
    SetMultimap<Long, Long> takeMap = queue.deserializeInflightTakes();
    System.out.println("Inflight takes:");
    for (Long txnID : takeMap.keySet()) {
      Set<Long> takes = takeMap.get(txnID);
      System.out.println("Transaction ID: " + String.valueOf(txnID));
      for (long value : takes) {
        int fileID = (int) (value >>> 32);
        int offset = (int) value;
        System.out.println(Long.toHexString(value) + " fileID = "
                + fileID + ", offset = " + offset);
      }
    }
  }
}
