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
package org.apache.flume.api;

import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.thrift.Status;
import org.apache.flume.thrift.ThriftFlumeEvent;
import org.apache.flume.thrift.ThriftSourceProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ThriftTestingSource {
  public final Queue<Event> flumeEvents = new ConcurrentLinkedQueue<Event>();
  private final TServer server;
  public int batchCount = 0;
  public int individualCount = 0;
  public int incompleteBatches = 0;
  private AtomicLong delay = null;

  public void setDelay(AtomicLong delay) {
    this.delay = delay;
  }

  private class ThriftOKHandler implements ThriftSourceProtocol.Iface {

    public ThriftOKHandler() {

    }

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      flumeEvents.add(EventBuilder.withBody(event.getBody(),
        event.getHeaders()));
      individualCount++;
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      batchCount++;
      if (events.size() < 10) {
        incompleteBatches++;
      }
      for (ThriftFlumeEvent event : events) {
        flumeEvents.add(EventBuilder.withBody(event.getBody(),
          event.getHeaders()));
      }
      return Status.OK;
    }
  }

  private class ThriftFailHandler implements ThriftSourceProtocol.Iface {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      return Status.FAILED;
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws
                                                             TException {
      return Status.FAILED;
    }
  }

  private class ThriftErrorHandler implements ThriftSourceProtocol.Iface {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      throw new FlumeException("Forced Error");
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      throw new FlumeException("Forced Error");
    }
  }

  private class ThriftSlowHandler extends ThriftOKHandler {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      try {
        TimeUnit.MILLISECONDS.sleep(1550);
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.append(event);
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      try {
        TimeUnit.MILLISECONDS.sleep(1550);
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.appendBatch(events);
    }
  }

  private class ThriftTimeoutHandler extends ThriftOKHandler {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      try {
        TimeUnit.MILLISECONDS.sleep(5000);
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.append(event);
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      try {
        TimeUnit.MILLISECONDS.sleep(5000);
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.appendBatch(events);
    }
  }

  private class ThriftAlternateHandler extends ThriftOKHandler {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      try {
        if (delay != null) {
          TimeUnit.MILLISECONDS.sleep(delay.get());
        }
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.append(event);
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws
      TException {
      try {
        if (delay != null) {
          TimeUnit.MILLISECONDS.sleep(delay.get());
        }
      } catch (InterruptedException e) {
        throw new FlumeException("Error", e);
      }
      return super.appendBatch(events);
    }
  }

  public ThriftTestingSource(String handlerName, int port) throws Exception {
    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(new
      InetSocketAddress("0.0.0.0", port));
    ThriftSourceProtocol.Iface handler = null;
    if (handlerName.equals(HandlerType.OK.name())) {
      handler = new ThriftOKHandler();
    } else if (handlerName.equals(HandlerType.FAIL.name())) {
      handler = new ThriftFailHandler();
    } else if (handlerName.equals(HandlerType.ERROR.name())) {
      handler = new ThriftErrorHandler();
    } else if (handlerName.equals(HandlerType.SLOW.name())) {
      handler = new ThriftSlowHandler();
    } else if (handlerName.equals(HandlerType.TIMEOUT.name())) {
      handler = new ThriftTimeoutHandler();
    } else if (handlerName.equals(HandlerType.ALTERNATE.name())) {
      handler = new ThriftAlternateHandler();
    }

    server = new THsHaServer(new THsHaServer.Args
      (serverTransport).processor(
      new ThriftSourceProtocol.Processor(handler)).protocolFactory(
      new TCompactProtocol.Factory()));
    Executors.newSingleThreadExecutor().submit(new Runnable() {
      @Override
      public void run() {
        server.serve();
      }
    });
  }

  public enum HandlerType {
    OK,
    FAIL,
    ERROR,
    SLOW,
    TIMEOUT,
    ALTERNATE;
  }

  public void stop() {
    server.stop();
  }


}
