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
package org.apache.flume.channel.file.instrumentation;

import org.apache.flume.Event;
import org.apache.flume.instrumentation.ChannelCounterMBean;

public interface FileChannelCounterMBean extends ChannelCounterMBean {

  boolean isOpen();

  /**
   * The numeric representation (0/1) of the negated value of the open flag.
   */
  int getClosed();

  /**
   * A value of 0 represents that the channel is in a healthy state: it is either starting
   * up (i.e. the replay is running) or already started up successfully.
   * A value of 1 represents that the channel is in a permanently failed state, which means that
   * the startup was unsuccessful due to an exception during the replay.
   * Once the channel started up successfully the *ErrorCount (or the ratio of the *AttemptCount
   * and *SuccessCount) counters should be used to check whether it is functioning properly.
   *
   * Note: this flag doesn't report the channel as unhealthy if the configuration failed because the
   * ChannelCounter might not have been instantiated/started yet.
   */
  int getUnhealthy();

  /**
   * A count of the number of IOExceptions encountered while trying to put() onto the channel.
   * @see org.apache.flume.channel.file.FileChannel.FileBackedTransaction#doPut(Event)
   */
  long getEventPutErrorCount();

  /**
   * A count of the number of errors encountered while trying to take() from the channel,
   * including IOExceptions and corruption-related errors.
   * @see org.apache.flume.channel.file.FileChannel.FileBackedTransaction#doTake()
   */
  long getEventTakeErrorCount();

  /**
   * A count of the number of errors encountered while trying to write the checkpoints. This
   * includes any Throwables.
   * @see org.apache.flume.channel.file.Log.BackgroundWorker#run()
   */
  long getCheckpointWriteErrorCount();

  /**
   * A count of the number of errors encountered while trying to write the backup checkpoints. This
   * includes any Throwables.
   * @see org.apache.flume.channel.file.EventQueueBackingStoreFile#startBackupThread()
   */
  long getCheckpointBackupWriteErrorCount();
}
