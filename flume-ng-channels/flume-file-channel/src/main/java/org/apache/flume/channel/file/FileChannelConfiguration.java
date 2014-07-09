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

public class FileChannelConfiguration {
  /**
   * Directory Checkpoints will be written in
   */
  public static final String CHECKPOINT_DIR = "checkpointDir";

  /**
   * The directory to which the checkpoint must be backed up
   */
  public static final String BACKUP_CHECKPOINT_DIR = "backupCheckpointDir";

  /**
   * Directories data files will be written in. Multiple directories
   * can be specified as comma separated values. Writes will
   * be written in a round robin fashion.
   */
  public static final String DATA_DIRS = "dataDirs";
  /**
   * Maximum number of put/take events in a transaction. Default: 1000
   */
  public static final String TRANSACTION_CAPACITY = "transactionCapacity";
  public static final int DEFAULT_TRANSACTION_CAPACITY = 10000;
  /**
   * Interval at which checkpoints should be taken. Default 30s (ms)
   */
  public static final String CHECKPOINT_INTERVAL = "checkpointInterval";
  public static final long DEFAULT_CHECKPOINT_INTERVAL = 30L * 1000L;
  /**
   * Max file size for data files, cannot exceed the default. Default~ 1.5GB
   */
  public static final String MAX_FILE_SIZE = "maxFileSize";
  public static final long DEFAULT_MAX_FILE_SIZE =
        Integer.MAX_VALUE - (500L * 1024L * 1024L); // ~1.52 G

  public static final String MINIMUM_REQUIRED_SPACE = "minimumRequiredSpace";
  /**
   * Minimum space required defaults to 500MB
   */
  public static final long DEFAULT_MINIMUM_REQUIRED_SPACE = 500L * 1024L * 1024L;
  /**
   * Minimum space floor is 1MB
   */
  public static final long FLOOR_MINIMUM_REQUIRED_SPACE = 1L * 1024L * 1024L;

  /**
   * Maximum capacity of the channel.
   * Default: 1,000,000
   */
  public static final String CAPACITY = "capacity";
  public static final int DEFAULT_CAPACITY = 1000000;
  /**
   * The length of time we will wait for space available to do a Put.
   * Default: 3 (seconds)
   */
  public static final String KEEP_ALIVE = "keep-alive";
  public static final int DEFAULT_KEEP_ALIVE = 3;

  /**
   * Turn on Flume 1.2 log replay logic
   */
  public static final String USE_LOG_REPLAY_V1 = "use-log-replay-v1";
  public static final boolean DEFAULT_USE_LOG_REPLAY_V1 = false;

  public static final String USE_FAST_REPLAY = "use-fast-replay";
  public static final boolean DEFAULT_USE_FAST_REPLAY = false;

  public static final String USE_DUAL_CHECKPOINTS = "useDualCheckpoints";
  public static final boolean DEFAULT_USE_DUAL_CHECKPOINTS = false;

  public static final String COMPRESS_BACKUP_CHECKPOINT =
    "compressBackupCheckpoint";
  public static final boolean DEFAULT_COMPRESS_BACKUP_CHECKPOINT
    = false;

  public static final String FSYNC_PER_TXN = "fsyncPerTransaction";
  public static final boolean DEFAULT_FSYNC_PRE_TXN = true;

  public static final String FSYNC_INTERVAL = "fsyncInterval";
  public static final int DEFAULT_FSYNC_INTERVAL = 5; // seconds.


}
