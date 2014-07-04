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

package org.apache.flume.source.scribe;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.scribe.Scribe.Iface;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flume should adopt the Scribe entry {@code LogEntry} from existing
 * Scribe system. Mostly, we may receive message from local Scribe and Flume
 * take responsibility of central Scribe.
 *
 * <p>
 * We use Thrift without deserializing, throughput has 2X increasing
 */
public class ScribeSource extends AbstractSource implements
      EventDrivenSource, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(ScribeSource.class);

  public static final String SCRIBE_CATEGORY = "category";

  private static final int DEFAULT_PORT = 1499;
  private static final int DEFAULT_WORKERS = 5;
  private static final int DEFAULT_MAX_READ_BUFFER_BYTES = 16384000;

  private TServer server;
  private int port;
  private int workers;
  private int maxReadBufferBytes;

  private SourceCounter sourceCounter;

  @Override
  public void configure(Context context) {
    port = context.getInteger("port", DEFAULT_PORT);
    maxReadBufferBytes = context.getInteger("maxReadBufferBytes", DEFAULT_MAX_READ_BUFFER_BYTES);
    if(maxReadBufferBytes <= 0){
      maxReadBufferBytes = DEFAULT_MAX_READ_BUFFER_BYTES;
    }

    workers = context.getInteger("workerThreads", DEFAULT_WORKERS);
    if (workers <= 0) {
      workers = DEFAULT_WORKERS;
    }

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  private class Startup extends Thread {

    public void run() {
      try {
        Scribe.Processor processor = new Scribe.Processor(new Receiver());
        TNonblockingServerTransport transport = new TNonblockingServerSocket(port);
        THsHaServer.Args args = new THsHaServer.Args(transport);

        args.workerThreads(workers);
        args.processor(processor);
        args.transportFactory(new TFramedTransport.Factory(maxReadBufferBytes));
        args.protocolFactory(new TBinaryProtocol.Factory(false, false));
        args.maxReadBufferBytes = maxReadBufferBytes;

        server = new THsHaServer(args);

        LOG.info("Starting Scribe Source on port " + port);

        server.serve();
      } catch (Exception e) {
        LOG.warn("Scribe failed", e);
      }
    }

  }

  @Override
  public void start() {
    Startup startupThread = new Startup();
    startupThread.start();

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {}

    if (!server.isServing()) {
      throw new IllegalStateException("Failed initialization of ScribeSource");
    }

    sourceCounter.start();
    super.start();
  }

  @Override
  public void stop() {
    LOG.info("Scribe source stopping");

    if (server != null) {
      server.stop();
    }

    sourceCounter.stop();
    super.stop();

    LOG.info("Scribe source stopped. Metrics:{}", sourceCounter);
  }


  class Receiver implements Iface {

    public ResultCode Log(List<LogEntry> list) throws TException {
      if (list != null) {
        sourceCounter.addToEventReceivedCount(list.size());

        try {
          List<Event> events = new ArrayList<Event>(list.size());

          for (LogEntry entry : list) {
            Map<String, String> headers = new HashMap<String, String>(1, 1);
            String category = entry.getCategory();

            if (category != null) {
              headers.put(SCRIBE_CATEGORY, category);
            }

            Event event = EventBuilder.withBody(entry.getMessage().getBytes(), headers);
            events.add(event);
          }

          if (events.size() > 0) {
            getChannelProcessor().processEventBatch(events);
          }

          sourceCounter.addToEventAcceptedCount(list.size());
          return ResultCode.OK;
        } catch (Exception e) {
          LOG.warn("Scribe source handling failure", e);
        }
      }

      return ResultCode.TRY_LATER;
    }
  }

}
