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
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.thrift.Status;
import org.apache.flume.thrift.ThriftFlumeEvent;
import org.apache.flume.thrift.ThriftSourceProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThriftRpcClient extends AbstractRpcClient {
  private static final Logger LOGGER =
    LoggerFactory.getLogger(ThriftRpcClient.class);

  private int batchSize;
  private long requestTimeout;
  private final Lock stateLock;
  private State connState;
  private String hostname;
  private int port;
  private ConnectionPoolManager connectionManager;
  private final ExecutorService callTimeoutPool;
  private final AtomicLong threadCounter;
  private int connectionPoolSize;
  private final Random random = new Random();

  public ThriftRpcClient() {
    stateLock = new ReentrantLock(true);
    connState = State.INIT;

    threadCounter = new AtomicLong(0);
    // OK to use cached threadpool, because this is simply meant to timeout
    // the calls - and is IO bound.
    callTimeoutPool = Executors.newCachedThreadPool(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("Flume Thrift RPC thread - " + String.valueOf(
          threadCounter.incrementAndGet()));
        return t;
      }
    });
  }


  @Override
  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public void append(Event event) throws EventDeliveryException {
    // Thrift IPC client is not thread safe, so don't allow state changes or
    // client.append* calls unless the lock is acquired.
    ClientWrapper client = null;
    boolean destroyedClient = false;
    try {
      if (!isActive()) {
        throw new EventDeliveryException("Client was closed due to error. " +
          "Please create a new client");
      }
      client = connectionManager.checkout();
      final ThriftFlumeEvent thriftEvent = new ThriftFlumeEvent(event
        .getHeaders(), ByteBuffer.wrap(event.getBody()));
      doAppend(client, thriftEvent).get(requestTimeout, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      if (e instanceof ExecutionException) {
        Throwable cause = e.getCause();
        if (cause instanceof EventDeliveryException) {
          throw (EventDeliveryException) cause;
        } else if (cause instanceof TimeoutException) {
          throw new EventDeliveryException("Append call timeout", cause);
        }
      }
      destroyedClient = true;
      // If destroy throws, we still don't want to reuse the client, so mark it
      // as destroyed before we actually do.
      if (client != null) {
        connectionManager.destroy(client);
      }
      if (e instanceof Error) {
        throw (Error) e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new EventDeliveryException("Failed to send event. ", e);
    } finally {
      if (client != null && !destroyedClient) {
        connectionManager.checkIn(client);
      }
    }
  }

  @Override
  public void appendBatch(List<Event> events) throws EventDeliveryException {
    // Thrift IPC client is not thread safe, so don't allow state changes or
    // client.append* calls unless the lock is acquired.
    ClientWrapper client = null;
    boolean destroyedClient = false;
    try {
      if (!isActive()) {
        throw new EventDeliveryException("Client was closed " +
          "due to error or is not yet configured.");
      }
      client = connectionManager.checkout();
      final List<ThriftFlumeEvent> thriftFlumeEvents = new ArrayList
        <ThriftFlumeEvent>();
      Iterator<Event> eventsIter = events.iterator();
      while (eventsIter.hasNext()) {
        thriftFlumeEvents.clear();
        for (int i = 0; i < batchSize && eventsIter.hasNext(); i++) {
          Event event = eventsIter.next();
          thriftFlumeEvents.add(new ThriftFlumeEvent(event.getHeaders(),
            ByteBuffer.wrap(event.getBody())));
        }
        if (!thriftFlumeEvents.isEmpty()) {
          doAppendBatch(client, thriftFlumeEvents).get(requestTimeout,
            TimeUnit.MILLISECONDS);
        }
      }
    } catch (Throwable e) {
      if (e instanceof ExecutionException) {
        Throwable cause = e.getCause();
        if (cause instanceof EventDeliveryException) {
          throw (EventDeliveryException) cause;
        } else if (cause instanceof TimeoutException) {
          throw new EventDeliveryException("Append call timeout", cause);
        }
      }
      destroyedClient = true;
      // If destroy throws, we still don't want to reuse the client, so mark it
      // as destroyed before we actually do.
      if (client != null) {
        connectionManager.destroy(client);
      }
      if (e instanceof Error) {
        throw (Error) e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new EventDeliveryException("Failed to send event. ", e);
    } finally {
      if (client != null && !destroyedClient) {
        connectionManager.checkIn(client);
      }
    }
  }

  private Future<Void> doAppend(final ClientWrapper client,
    final ThriftFlumeEvent e) throws Exception {

    return callTimeoutPool.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Status status = client.client.append(e);
        if (status != Status.OK) {
          throw new EventDeliveryException("Failed to deliver events. Server " +
            "returned status : " + status.name());
        }
        return null;
      }
    });
  }

  private Future<Void> doAppendBatch(final ClientWrapper client,
    final List<ThriftFlumeEvent> e) throws Exception {

    return callTimeoutPool.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Status status = client.client.appendBatch(e);
        if (status != Status.OK) {
          throw new EventDeliveryException("Failed to deliver events. Server " +
            "returned status : " + status.name());
        }
        return null;
      }
    });
  }

  @Override
  public boolean isActive() {
    stateLock.lock();
    try {
      return (connState == State.READY);
    } finally {
      stateLock.unlock();
    }
  }

  @Override
  public void close() throws FlumeException {
    try {
      //Do not release this, because this client is not to be used again
      stateLock.lock();
      connState = State.DEAD;
      connectionManager.closeAll();
      callTimeoutPool.shutdown();
      if(!callTimeoutPool.awaitTermination(5, TimeUnit.SECONDS)) {
        callTimeoutPool.shutdownNow();
      }
    } catch (Throwable ex) {
      if(ex instanceof Error) {
        throw (Error) ex;
      } else if (ex instanceof RuntimeException) {
        throw (RuntimeException) ex;
      }
      throw new FlumeException("Failed to close RPC client. ", ex);
    } finally {
      stateLock.unlock();
    }
  }

  @Override
  protected void configure(Properties properties) throws FlumeException {
    if (isActive()) {
      throw new FlumeException("Attempting to re-configured an already " +
        "configured client!");
    }
    stateLock.lock();
    try {
      HostInfo host = HostInfo.getHostInfoList(properties).get(0);
      hostname = host.getHostName();
      port = host.getPortNumber();
      batchSize = Integer.parseInt(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_BATCH_SIZE,
        RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE.toString()));
      requestTimeout = Long.parseLong(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT,
        String.valueOf(
          RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS)));
      if (requestTimeout < 1000) {
        LOGGER.warn("Request timeout specified less than 1s. " +
          "Using default value instead.");
        requestTimeout =
          RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS;
      }
      connectionPoolSize = Integer.parseInt(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_CONNECTION_POOL_SIZE,
        String.valueOf(RpcClientConfigurationConstants
          .DEFAULT_CONNECTION_POOL_SIZE)));
      if(connectionPoolSize < 1) {
        LOGGER.warn("Connection Pool Size specified is less than 1. " +
          "Using default value instead.");
        connectionPoolSize = RpcClientConfigurationConstants
          .DEFAULT_CONNECTION_POOL_SIZE;
      }
      connectionManager = new ConnectionPoolManager(connectionPoolSize);
      connState = State.READY;
    } catch (Throwable ex) {
      //Failed to configure, kill the client.
      connState = State.DEAD;
      if(ex instanceof Error) {
        throw (Error) ex;
      } else if (ex instanceof RuntimeException) {
        throw (RuntimeException) ex;
      }
      throw new FlumeException("Error while configuring RpcClient. ", ex);
    } finally {
      stateLock.unlock();
    }
  }

  private static enum State {
    INIT, READY, DEAD
  }

  /**
   * Wrapper around a client and transport, so we can clean up when this
   * client gets closed.
   */
  private class ClientWrapper {
    public final ThriftSourceProtocol.Client client;
    public final TFastFramedTransport transport;
    private final int hashCode;

    public ClientWrapper() throws Exception{
      transport = new TFastFramedTransport(new TSocket(hostname, port));
      transport.open();
      client = new ThriftSourceProtocol.Client(new TCompactProtocol
        (transport));
      // Not a great hash code, but since this class is immutable and there
      // is at most one instance of the components of this class,
      // this works fine [If the objects are equal, hash code is the same]
      hashCode = random.nextInt();
    }

    public boolean equals(Object o) {
      if(o == null) {
        return false;
      }
      // Since there is only one wrapper with any given client,
      // direct comparison is good enough.
      if(this == o) {
        return true;
      }
      return false;
    }

    public int hashCode() {
      return hashCode;
    }
  }

  private class ConnectionPoolManager {
    private final Queue<ClientWrapper> availableClients;
    private final Set<ClientWrapper> checkedOutClients;
    private final int maxPoolSize;
    private int currentPoolSize;
    private final Lock poolLock;
    private final Condition availableClientsCondition;

    public ConnectionPoolManager(int poolSize) {
      this.maxPoolSize = poolSize;
      availableClients = new LinkedList<ClientWrapper>();
      checkedOutClients = new HashSet<ClientWrapper>();
      poolLock = new ReentrantLock();
      availableClientsCondition = poolLock.newCondition();
      currentPoolSize = 0;
    }

    public ClientWrapper checkout() throws Exception {

      ClientWrapper ret = null;
      poolLock.lock();
      try {
        if (availableClients.isEmpty() && currentPoolSize < maxPoolSize) {
          ret = new ClientWrapper();
          currentPoolSize++;
          checkedOutClients.add(ret);
          return ret;
        }
        while (availableClients.isEmpty()) {
          availableClientsCondition.await();
        }
        ret = availableClients.poll();
        checkedOutClients.add(ret);
      } finally {
        poolLock.unlock();
      }
      return ret;
    }

    public void checkIn(ClientWrapper client) {
      poolLock.lock();
      try {
        availableClients.add(client);
        checkedOutClients.remove(client);
        availableClientsCondition.signal();
      } finally {
        poolLock.unlock();
      }
    }

    public void destroy(ClientWrapper client) {
      poolLock.lock();
      try {
        checkedOutClients.remove(client);
        currentPoolSize--;
      } finally {
        poolLock.unlock();
      }
      client.transport.close();
    }

    public void closeAll() {
      poolLock.lock();
      try {
        for (ClientWrapper c : availableClients) {
          c.transport.close();
          currentPoolSize--;
        }
      /*
       * Be cruel and close even the checked out clients. The threads writing
       * using these will now get an exception.
       */
        for (ClientWrapper c : checkedOutClients) {
          c.transport.close();
          currentPoolSize--;
        }
      } finally {
        poolLock.unlock();
      }
    }
  }
}
