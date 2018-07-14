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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncScribeClient extends AbstractScribeClient {
  private static final Logger logger = LoggerFactory
      .getLogger(AbstractScribeClient.class);

  private long totalLatency = 0;

  private ArrayBlockingQueue<Scribe.AsyncClient> clientArrayBlockingQueue;

  public AsyncScribeClient(ScribeCLIClientConfiguration configuration) {
    super(configuration);
    clientArrayBlockingQueue = new ArrayBlockingQueue<Scribe.AsyncClient>(conf.ioDepth);
  }

  protected void doRun() throws InterruptedException, IOException {
    for (int i = 0; i < conf.ioDepth; ++i) {
      Scribe.AsyncClient client = createAsyncClient();
      clientArrayBlockingQueue.add(client);
    }
    asyncSendEvents();
    String avgLatencyStr = getTotalEventsSend().get() == 0 ?
        "NA" :
        Float.toString((float) totalLatency / (float)getTotalEventsSend().get());
    logger.info("ASyncScribeClient quiting. Total sent events:"
        + getTotalEventsSend().get()
        + ", avgLatency:" + avgLatencyStr + "ms.");
    while (clientArrayBlockingQueue.size() != conf.ioDepth) {
      Thread.sleep(1000);
    }
  }

  public Scribe.AsyncClient createAsyncClient() throws IOException {
    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    TAsyncClientManager asyncClientManager = new TAsyncClientManager();
    TNonblockingTransport transport = new TNonblockingSocket(conf.server, conf.port);
    Scribe.AsyncClient client =
        new Scribe.AsyncClient(protocolFactory, asyncClientManager,
            transport);
    return client;
  }

  protected void asyncSendEvents() throws InterruptedException {
    int rpcBatchSize = conf.rpcBatchSize;
    List<LogEntry> logEntries =
        ScribeCLIClientHelper.generateLogEntryList(
            conf.categoryName, rpcBatchSize, conf.eventLength);
    while (!shouldStop()) {
      try {
        Scribe.AsyncClient client = clientArrayBlockingQueue.take();
        LogCallback callback = new LogCallback(rpcBatchSize, client);
        client.Log(logEntries, callback);
      } catch (TException ex) {
        logger.error("Got io exception while asyncSendEvents", ex);
      }
    }
  }

  private class LogCallback implements AsyncMethodCallback<Scribe.AsyncClient.Log_call> {
    private long createTimeMs;
    private Scribe.AsyncClient asyncClient;
    int eventSize;

    public LogCallback(int eventCount, Scribe.AsyncClient client) {
      createTimeMs = System.currentTimeMillis();
      asyncClient = client;
      eventSize = eventCount;
    }

    @Override
    public void onComplete(Scribe.AsyncClient.Log_call logCall) {
      try {
        if (logCall.getResult() == ResultCode.TRY_LATER) {
          logger.warn("Received TRY_LATER");
          return;
        }
        increaseTotalEventSent(eventSize);
        increaseTotalRpcLatency(totalLatency);
        long rpcLatency = System.currentTimeMillis() - createTimeMs;
        totalLatency += rpcLatency;
      } catch (TException e) {
        logger.error("Got exception while on complete", e);
      } finally {
        releaseClient();
      }
    }

    private void releaseClient() {
      if (asyncClient != null) {
        clientArrayBlockingQueue.add(asyncClient);
      }
    }

    @Override
    public void onError(Exception exception) {
      logger.error("Got error in call back", exception);
      releaseClient();
    }
  }
}
