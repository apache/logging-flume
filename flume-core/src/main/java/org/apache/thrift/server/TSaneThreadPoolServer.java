/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.handlers.thrift.TStatsTransport;

/**
 * Server which uses Java's built in ThreadPool management to spawn off a worker
 * pool.
 * 
 * This differs from thrift's TThreadPoolServer by: not being a subclass of
 * TServer, requiring a TSaneServerSocket (which defers binding until open),
 * having a shorter shutdown timeout, being started by start() instead of
 * serve()
 * 
 */
public class TSaneThreadPoolServer {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(TSaneThreadPoolServer.class);

  // The following metrics values need to be projected by this maplock.
  private Object maplock = new Object();
  private Map<Long, WorkerProcess> map = new HashMap<Long, WorkerProcess>();
  private long doneBytesRead = 0;
  private long doneBytesWritten = 0;

  /**
   * Core processor
   */
  protected TProcessorFactory processorFactory_;

  /**
   * Server transport
   */
  protected TServerTransport serverTransport_;

  /**
   * Input Transport Factory
   */
  protected TTransportFactory inputTransportFactory_;

  /**
   * Output Transport Factory
   */
  protected TTransportFactory outputTransportFactory_;

  /**
   * Input Protocol Factory
   */
  protected TProtocolFactory inputProtocolFactory_;

  /**
   * Output Protocol Factory
   */
  protected TProtocolFactory outputProtocolFactory_;

  // Executor service for handling client connections
  private ExecutorService executorService_;

  // Flag for stopping the server
  private volatile boolean stopped_;

  // Server options
  private Options options_;

  // Customizable server options
  public static class Options {
    public int minWorkerThreads = 5;
    public int maxWorkerThreads = Integer.MAX_VALUE;
    public int stopTimeoutVal = 1; // why was this set to 60 seconds?
    public TimeUnit stopTimeoutUnit = TimeUnit.SECONDS;
  }

  /**
   * Commonly used constructor.
   **/
  public TSaneThreadPoolServer(TProcessor processor,
      TServerTransport serverTransport, TProtocolFactory protocolFactory) {
    this(new TProcessorFactory(processor), serverTransport,
        new TTransportFactory(), new TTransportFactory(), protocolFactory,
        protocolFactory);
  }

  /**
   * Completely generic and plug-able constructor
   */
  public TSaneThreadPoolServer(TProcessorFactory processorFactory,
      TServerTransport serverTransport,
      TTransportFactory inputTransportFactory,
      TTransportFactory outputTransportFactory,
      TProtocolFactory inputProtocolFactory,
      TProtocolFactory outputProtocolFactory) {
    processorFactory_ = processorFactory;
    serverTransport_ = serverTransport;
    inputTransportFactory_ = inputTransportFactory;
    outputTransportFactory_ = outputTransportFactory;
    inputProtocolFactory_ = inputProtocolFactory;
    outputProtocolFactory_ = outputProtocolFactory;
    options_ = new Options();
    executorService_ = Executors.newCachedThreadPool();
  }

  /**
   * This version starts a server and immediately returns.
   */
  public void start() {
    try {
      serverTransport_.listen(); // opens, binds and listens
      stopped_ = false;
    } catch (TTransportException ttx) {
      LOGGER.error("Error occurred during listening.", ttx);
      return;
    }

    new Thread("Thrift server:" + processorFactory_.getClass() + " on "
        + serverTransport_.getClass()) {
      public void run() {
        while (!stopped_) {
          int failureCount = 0;
          try {
            TTransport client = serverTransport_.accept();
            TStatsTransport stats = new TStatsTransport(client);
            WorkerProcess wp = new WorkerProcess(stats);
            executorService_.execute(wp);
          } catch (TTransportException ttx) {
            if (!stopped_) {
              ++failureCount;
              LOGGER
                  .warn(
                      "Transport error occurred during acceptance of message.",
                      ttx);
            }
          }
        }
      }
    }.start();

  }

  /**
   * This stops the server and waits until the executor service terminates or a
   * timeoyut has been reached before returning.
   */
  public void stop() {
    stopped_ = true;
    serverTransport_.interrupt();
    executorService_.shutdown();

    // Loop until awaitTermination finally does return without a interrupted
    // exception. If we don't do this, then we'll shut down prematurely. We
    // want to let the executorService clear its task queue, closing client
    // sockets appropriately.
    long timeoutMS = options_.stopTimeoutUnit.toMillis(options_.stopTimeoutVal);
    long now = System.currentTimeMillis();
    while (timeoutMS >= 0) {
      try {
        executorService_.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
        break;
      } catch (InterruptedException ix) {
        long newnow = System.currentTimeMillis();
        timeoutMS -= (newnow - now);
        now = newnow;
      }
    }
  }

  public long getBytesSent() {
    synchronized (maplock) {
      long total = doneBytesWritten;
      for (WorkerProcess wp : map.values()) {
        total += wp.client_.getBytesWritten();
      }
      return total;
    }
  }

  public long getBytesReceived() {
    synchronized (maplock) {
      long total = doneBytesRead;
      for (WorkerProcess wp : map.values()) {
        total += wp.client_.getBytesRead();
      }
      return total;
    }
  }

  private class WorkerProcess implements Runnable {

    /**
     * Client that this services.
     */
    private TStatsTransport client_;

    /**
     * Default constructor.
     */
    private WorkerProcess(TStatsTransport client) {
      client_ = client;
    }

    /**
     * Loops on processing a client forever
     */
    public void run() {
      TProcessor processor = null;
      TTransport inputTransport = null;
      TTransport outputTransport = null;
      TProtocol inputProtocol = null;
      TProtocol outputProtocol = null;
      try {
        processor = processorFactory_.getProcessor(client_);
        inputTransport = inputTransportFactory_.getTransport(client_);
        outputTransport = outputTransportFactory_.getTransport(client_);
        inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
        outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);
        // we check stopped_ first to make sure we're not supposed to be
        // shutting down. this is necessary for graceful shutdown.

        synchronized (maplock) {
          // each worker has its own size counter. we need to have all the
          // workers accessible in a place so that we can get the bytes values
          // and aggregate them.
          map.put(Thread.currentThread().getId(), this);
        }

        while (!stopped_ && processor.process(inputProtocol, outputProtocol)) {
        }
      } catch (TTransportException ttx) {
        // Assume the client died and continue silently
      } catch (TException tx) {
        LOGGER.error("Thrift error occurred during processing of message.", tx);
      } catch (Exception x) {
        LOGGER.error("Error occurred during processing of message.", x);
      } finally {
        synchronized (maplock) {
          map.remove(Thread.currentThread().getId());
          doneBytesWritten += this.client_.getBytesWritten();
          doneBytesRead += this.client_.getBytesRead();
        }
      }

      if (inputTransport != null) {
        inputTransport.close();
      }

      if (outputTransport != null) {
        outputTransport.close();
      }
    }
  }
}
