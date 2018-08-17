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

import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.SystemClock;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Wrapper class for {@link BucketWriter}'s constructor parameters.
 */
class BucketWriterOptions {

  long rollInterval;
  long rollSize;
  long rollCount;
  long batchSize;
  Context context;
  String filePath;
  String fileName;
  String inUsePrefix;
  String inUseSuffix;
  String fileSuffix;
  CompressionCodec compressionCodec;
  SequenceFile.CompressionType compressionType;
  HDFSWriter hdfsWriter;
  ScheduledExecutorService timedRollerPool;
  PrivilegedExecutor proxyUser;
  SinkCounter sinkCounter;
  int idleTimeout;
  HDFSEventSink.WriterCallback onCloseCallback;
  String onCloseCallbackPath;
  long callTimeout;
  ExecutorService callTimeoutPool;
  long retryInterval;
  int maxCloseTries;
  Clock clock = new SystemClock();

  BucketWriterOptions rollInterval(long rollInterval) {
    this.rollInterval = rollInterval;
    return this;
  }

  BucketWriterOptions rollSize(long rollSize) {
    this.rollSize = rollSize;
    return this;
  }

  BucketWriterOptions rollCount(long rollCount) {
    this.rollCount = rollCount;
    return this;
  }

  BucketWriterOptions batchSize(long batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  BucketWriterOptions context(Context context) {
    this.context = context;
    return this;
  }

  BucketWriterOptions filePath(String filePath) {
    this.filePath = filePath;
    return this;
  }

  BucketWriterOptions fileName(String fileName) {
    this.fileName = fileName;
    return this;
  }

  BucketWriterOptions inUsePrefix(String inUsePrefix) {
    this.inUsePrefix = inUsePrefix;
    return this;
  }

  BucketWriterOptions inUseSuffix(String inUseSuffix) {
    this.inUseSuffix = inUseSuffix;
    return this;
  }

  BucketWriterOptions fileSuffix(String fileSuffix) {
    this.fileSuffix = fileSuffix;
    return this;
  }

  BucketWriterOptions compressionCodec(CompressionCodec compressionCodec) {
    this.compressionCodec = compressionCodec;
    return this;
  }

  BucketWriterOptions compressionType(SequenceFile.CompressionType compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  BucketWriterOptions hdfsWriter(HDFSWriter hdfsWriter) {
    this.hdfsWriter = hdfsWriter;
    return this;
  }

  BucketWriterOptions timedRollerPool(ScheduledExecutorService timedRollerPool) {
    this.timedRollerPool = timedRollerPool;
    return this;
  }

  BucketWriterOptions proxyUser(PrivilegedExecutor proxyUser) {
    this.proxyUser = proxyUser;
    return this;
  }

  BucketWriterOptions sinkCounter(SinkCounter sinkCounter) {
    this.sinkCounter = sinkCounter;
    return this;
  }

  BucketWriterOptions idleTimeout(int idleTimeout) {
    this.idleTimeout = idleTimeout;
    return this;
  }

  BucketWriterOptions onCloseCallback(HDFSEventSink.WriterCallback onCloseCallback) {
    this.onCloseCallback = onCloseCallback;
    return this;
  }

  BucketWriterOptions onCloseCallbackPath(String onCloseCallbackPath) {
    this.onCloseCallbackPath = onCloseCallbackPath;
    return this;
  }

  BucketWriterOptions callTimeout(long callTimeout) {
    this.callTimeout = callTimeout;
    return this;
  }

  BucketWriterOptions callTimeoutPool(ExecutorService callTimeoutPool) {
    this.callTimeoutPool = callTimeoutPool;
    return this;
  }

  BucketWriterOptions retryInterval(long retryInterval) {
    this.retryInterval = retryInterval;
    return this;
  }

  BucketWriterOptions maxCloseTries(int maxCloseTries) {
    this.maxCloseTries = maxCloseTries;
    return this;
  }

  BucketWriterOptions clock(Clock clock) {
    this.clock = clock;
    return this;
  }
}
