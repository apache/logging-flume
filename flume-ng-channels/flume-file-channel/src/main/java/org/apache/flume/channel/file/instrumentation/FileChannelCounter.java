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

import org.apache.flume.instrumentation.ChannelCounter;

public class FileChannelCounter extends ChannelCounter implements FileChannelCounterMBean {

  private boolean open;
  private int unhealthy;

  private static final String EVENT_PUT_ERROR_COUNT = "channel.file.event.put.error";
  private static final String EVENT_TAKE_ERROR_COUNT = "channel.file.event.take.error";
  private static final String CHECKPOINT_WRITE_ERROR_COUNT = "channel.file.checkpoint.write.error";
  private static final String CHECKPOINT_BACKUP_WRITE_ERROR_COUNT
      = "channel.file.checkpoint.backup.write.error";

  public FileChannelCounter(String name) {
    super(name, new String[] {
        EVENT_PUT_ERROR_COUNT, EVENT_TAKE_ERROR_COUNT,
        CHECKPOINT_WRITE_ERROR_COUNT, CHECKPOINT_BACKUP_WRITE_ERROR_COUNT
        }
    );
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  public void setOpen(boolean open) {
    this.open = open;
  }

  @Override
  public int getClosed() {
    return open ? 0 : 1;
  }

  @Override
  public int getUnhealthy() {
    return unhealthy;
  }

  public void setUnhealthy(int unhealthy) {
    this.unhealthy = unhealthy;
  }

  @Override
  public long getEventPutErrorCount() {
    return get(EVENT_PUT_ERROR_COUNT);
  }

  public void incrementEventPutErrorCount() {
    increment(EVENT_PUT_ERROR_COUNT);
  }

  @Override
  public long getEventTakeErrorCount() {
    return get(EVENT_TAKE_ERROR_COUNT);
  }

  public void incrementEventTakeErrorCount() {
    increment(EVENT_TAKE_ERROR_COUNT);
  }

  @Override
  public long getCheckpointWriteErrorCount() {
    return get(CHECKPOINT_WRITE_ERROR_COUNT);
  }

  public void incrementCheckpointWriteErrorCount() {
    increment(CHECKPOINT_WRITE_ERROR_COUNT);
  }

  @Override
  public long getCheckpointBackupWriteErrorCount() {
    return get(CHECKPOINT_BACKUP_WRITE_ERROR_COUNT);
  }

  public void incrementCheckpointBackupWriteErrorCount() {
    increment(CHECKPOINT_BACKUP_WRITE_ERROR_COUNT);
  }
}
