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

package org.apache.flume.client.scribe;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractScribeClient {

  private static final Logger logger = LoggerFactory
      .getLogger(AbstractScribeClient.class);

  protected final ScribeCLIClientConfiguration conf;
  protected long startTimeMs;
  protected static final AtomicLong totalEventsSend = new AtomicLong(0);
  protected static final AtomicLong totalRpcLatency = new AtomicLong(0);

  public AbstractScribeClient(ScribeCLIClientConfiguration configuration) {
    Preconditions.checkNotNull(configuration, "configuration shouldn't be null");
    conf = configuration;
  }

  public boolean isTimeOut() {
    if (conf.runTimeSec <= 0) {
      return false;
    }
    return  startTimeMs + conf.runTimeSec * 1000L <= System.currentTimeMillis();
  }

  public boolean isEnoughEventSent() {
    if (conf.eventNumberToSend <= 0) {
      return false;
    }
    return  totalEventsSend.get() >= conf.eventNumberToSend;
  }

  public boolean shouldStop() {
    if (isTimeOut() || isEnoughEventSent()) {
      return true;
    }
    return false;
  }

  public void run() throws InterruptedException, ExecutionException, IOException {
    startTimeMs = System.currentTimeMillis();
    doRun();
    printStats();
  }

  protected abstract void doRun() throws InterruptedException, ExecutionException, IOException;

  public static long increaseTotalEventSent(long number) {
    return totalEventsSend.addAndGet(number);
  }

  public static void setTotalEventsSend(long eventsSend) {
    totalEventsSend.set(eventsSend);
  }

  public static void setTotalRpcLatency(long rpcLatency) {
    totalRpcLatency.set(rpcLatency);
  }

  public static long increaseTotalRpcLatency(long latency) {
    return totalRpcLatency.addAndGet(latency);
  }

  public static AtomicLong getTotalEventsSend() {
    return totalEventsSend;
  }

  public void printStats() {
    String avgLatencyStr = ScribeCLIClientHelper.getLatencyString(
        totalRpcLatency.get(), totalEventsSend.get());
    logger.info("Run time(ms):" + (System.currentTimeMillis() - startTimeMs) + ", "
        + conf.categoryName + " sent:" + totalEventsSend.toString()
        + ", avgLatency(ms):" + avgLatencyStr);
  }
}
