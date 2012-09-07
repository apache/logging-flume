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

import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.collect.Lists;

public class TestLogRecord {


  @Test
  public void testConstructor() {
    long now = System.currentTimeMillis();
    Commit commit = new Commit(now, now + 1);
    LogRecord logRecord = new LogRecord(1, 2, commit);
    Assert.assertTrue(now == commit.getTransactionID());
    Assert.assertTrue(now + 1 == commit.getLogWriteOrderID());
    Assert.assertTrue(1 == logRecord.getFileID());
    Assert.assertTrue(2 == logRecord.getOffset());
    Assert.assertTrue(commit == logRecord.getEvent());
  }

  @Test
  public void testSortOrder() {
    // events should sort in the reverse order we put them on
    long now = System.currentTimeMillis();
    List<LogRecord> records = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      Commit commit = new Commit((long)i, now - i);
      LogRecord logRecord = new LogRecord(1, i, commit);
      records.add(logRecord);
    }
    LogRecord logRecord;

    logRecord = Collections.min(records);
    Assert.assertTrue(String.valueOf(logRecord.getOffset()),
        2 == logRecord.getOffset());
    records.remove(logRecord);

    logRecord = Collections.min(records);
    Assert.assertTrue(String.valueOf(logRecord.getOffset()),
        1 == logRecord.getOffset());
    records.remove(logRecord);

    logRecord = Collections.min(records);
    Assert.assertTrue(String.valueOf(logRecord.getOffset()),
        0 == logRecord.getOffset());
    records.remove(logRecord);
  }
}
