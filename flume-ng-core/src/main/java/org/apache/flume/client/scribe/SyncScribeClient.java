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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncScribeClient extends AbstractScribeClient {
  private static final Logger logger = LoggerFactory
      .getLogger(AbstractScribeClient.class);

  private ScheduledExecutorService scheduledExecutorService;

  public SyncScribeClient(final ScribeCLIClientConfiguration configuration) {
    super(configuration);
    String threadName = "SyncScribeClient-" + "%d";
    scheduledExecutorService =
        Executors.newScheduledThreadPool(conf.ioDepth,
            new ThreadFactoryBuilder().setNameFormat(threadName).build());
  }

  @Override
  public void doRun() throws InterruptedException, ExecutionException {
    List<ScheduledFuture> futureList = new ArrayList<>(conf.ioDepth);
    Preconditions.checkArgument(conf.ioDepth > 0, "IODepth must be positive");
    long eventToSendPreThread = conf.eventNumberToSend / conf.ioDepth;
    for (int i = 0; i < conf.ioDepth; i++) {
      if (eventToSendPreThread + eventToSendPreThread * i > conf.eventNumberToSend) {
        eventToSendPreThread = conf.eventNumberToSend - eventToSendPreThread * i;
      }
      ScheduledFuture future = scheduledExecutorService.schedule(
          new SyncScribeClientRunnable(eventToSendPreThread), 0, TimeUnit.SECONDS);
      futureList.add(future);
    }
    for (ScheduledFuture future : futureList) {
      future.get();
    }
    scheduledExecutorService.shutdown();
    scheduledExecutorService.awaitTermination(120, TimeUnit.SECONDS);
  }

  public ResultCode syncSendEvents(Scribe.Client client, int rpcBatchSize)
      throws TException {
    List<LogEntry> logEntries =
        ScribeCLIClientHelper.generateLogEntryList(
            conf.categoryName, rpcBatchSize, conf.eventLength);
    return client.Log(logEntries);
  }

  public class SyncScribeClientRunnable implements Runnable {
    private long eventNumberToSend;
    private String engineName;
    private TTransport transport;
    private TProtocol protocol;
    private  Scribe.Client client;

    public SyncScribeClientRunnable(long eventsNumber) {
      eventNumberToSend = eventsNumber;
    }

    @Override
    public void run() {
      if (engineName == null) {
        engineName = Thread.currentThread().getName();
      }
      try {
        transport = new TFramedTransport(new TSocket(conf.server, conf.port));
        protocol = new TBinaryProtocol(transport);
        client = new Scribe.Client(protocol);
        transport.open();
        long eventSent = 0;
        long totalLatency = 0;
        while (!shouldStop() && eventSent < this.eventNumberToSend) {
          int rpcBatchSize = conf.rpcBatchSize;
          if (rpcBatchSize > eventNumberToSend - eventSent) {
            rpcBatchSize = (int) (eventNumberToSend - eventSent);
          }
          long rpcStartTime = System.currentTimeMillis();
          ResultCode resultCode = syncSendEvents(client, rpcBatchSize);
          if (resultCode == ResultCode.TRY_LATER) {
            logger.error("Got retry later in syncSendEvents");
            continue;
          }
          long latency = System.currentTimeMillis() - rpcStartTime;
          totalLatency += latency;
          eventSent += rpcBatchSize;
        }
        increaseTotalEventSent(eventSent);
        increaseTotalRpcLatency(totalLatency);
        logger.info(engineName + " sent " + eventSent + " events, avg Latency(ms):"
            + ScribeCLIClientHelper.getLatencyString(totalLatency, eventSent)
            + ", ran in threadId:" + Thread.currentThread().getId());
      } catch (TTransportException e) {
        logger.error("Exception in ScribeMessageGenerator ", e);
      } catch (TException t) {
        logger.error("Exception in ScribeMessageGenerator ", t);
      }
    }
  }

}
