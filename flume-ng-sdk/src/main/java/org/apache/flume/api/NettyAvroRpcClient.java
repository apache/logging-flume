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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.avro.ipc.CallFuture;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Avro/Netty implementation of {@link RpcClient}.
 * The connections are intended to be opened before clients are given access so
 * that the object cannot ever be in an inconsistent when exposed to users.
 */
public class NettyAvroRpcClient extends AbstractRpcClient
implements RpcClient {

  private ExecutorService callTimeoutPool;
  private final ReentrantLock stateLock = new ReentrantLock();

  /**
   * Guarded by {@code stateLock}
   */
  private ConnState connState;

  private InetSocketAddress address;
  private boolean enableSsl;
  private boolean trustAllCerts;
  private String truststore;
  private String truststorePassword;
  private String truststoreType;

  private Transceiver transceiver;
  private AvroSourceProtocol.Callback avroClient;
  private static final Logger logger = LoggerFactory
      .getLogger(NettyAvroRpcClient.class);
  private boolean enableDeflateCompression;
  private int compressionLevel;
  private int maxIoWorkers;

  /**
   * This constructor is intended to be called from {@link RpcClientFactory}.
   * A call to this constructor should be followed by call to configure().
   */
  protected NettyAvroRpcClient(){
  }

  /**
   * This method should only be invoked by the build function
   * @throws FlumeException
   */
  private void connect() throws FlumeException {
    connect(connectTimeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Internal only, for now
   * @param timeout
   * @param tu
   * @throws FlumeException
   */
  private void connect(long timeout, TimeUnit tu) throws FlumeException {
    callTimeoutPool = Executors.newCachedThreadPool(
        new TransceiverThreadFactory("Flume Avro RPC Client Call Invoker"));
    NioClientSocketChannelFactory socketChannelFactory = null;

    try {

      ExecutorService bossExecutor =
        Executors.newCachedThreadPool(new TransceiverThreadFactory(
          "Avro " + NettyTransceiver.class.getSimpleName() + " Boss"));
      ExecutorService workerExecutor =
        Executors.newCachedThreadPool(new TransceiverThreadFactory(
          "Avro " + NettyTransceiver.class.getSimpleName() + " I/O Worker"));

      if (enableDeflateCompression || enableSsl) {
        if (maxIoWorkers >= 1) {
          socketChannelFactory = new SSLCompressionChannelFactory(
            bossExecutor, workerExecutor,
            enableDeflateCompression, enableSsl, trustAllCerts,
            compressionLevel, truststore, truststorePassword, truststoreType,
            maxIoWorkers);
        } else {
          socketChannelFactory = new SSLCompressionChannelFactory(
            bossExecutor, workerExecutor,
            enableDeflateCompression, enableSsl, trustAllCerts,
            compressionLevel, truststore, truststorePassword, truststoreType);
        }
      } else {
        if (maxIoWorkers >= 1) {
          socketChannelFactory = new NioClientSocketChannelFactory(
              bossExecutor, workerExecutor, maxIoWorkers);
        } else {
          socketChannelFactory = new NioClientSocketChannelFactory(
              bossExecutor, workerExecutor);
        }
      }

      transceiver = new NettyTransceiver(this.address,
          socketChannelFactory,
          tu.toMillis(timeout));
      avroClient =
          SpecificRequestor.getClient(AvroSourceProtocol.Callback.class,
          transceiver);
    } catch (Throwable t) {
      if (callTimeoutPool != null) {
        callTimeoutPool.shutdownNow();
      }
      if (socketChannelFactory != null) {
        socketChannelFactory.releaseExternalResources();
      }
      if (t instanceof IOException) {
        throw new FlumeException(this + ": RPC connection error", t);
      } else if (t instanceof FlumeException) {
        throw (FlumeException) t;
      } else if (t instanceof Error) {
        throw (Error) t;
      } else {
        throw new FlumeException(this + ": Unexpected exception", t);
      }
    }

    setState(ConnState.READY);
  }

  @Override
  public void close() throws FlumeException {
    if (callTimeoutPool != null) {
      callTimeoutPool.shutdown();
      try {
        if (!callTimeoutPool.awaitTermination(requestTimeout,
            TimeUnit.MILLISECONDS)) {
          callTimeoutPool.shutdownNow();
          if (!callTimeoutPool.awaitTermination(requestTimeout,
              TimeUnit.MILLISECONDS)) {
            logger.warn(this + ": Unable to cleanly shut down call timeout " +
                "pool");
          }
        }
      } catch (InterruptedException ex) {
        logger.warn(this + ": Interrupted during close", ex);
        // re-cancel if current thread also interrupted
        callTimeoutPool.shutdownNow();
        // preserve interrupt status
        Thread.currentThread().interrupt();
      }

      callTimeoutPool = null;
    }
    try {
      transceiver.close();
    } catch (IOException ex) {
      throw new FlumeException(this + ": Error closing transceiver.", ex);
    } finally {
      setState(ConnState.DEAD);
    }

  }

  @Override
  public String toString() {
    return "NettyAvroRpcClient { host: " + address.getHostName() + ", port: " +
        address.getPort() + " }";
  }

  @Override
  public void append(Event event) throws EventDeliveryException {
    try {
      append(event, requestTimeout, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      // we mark as no longer active without trying to clean up resources
      // client is required to call close() to clean up resources
      setState(ConnState.DEAD);
      if (t instanceof Error) {
        throw (Error) t;
      }
      if (t instanceof TimeoutException) {
        throw new EventDeliveryException(this + ": Failed to send event. " +
            "RPC request timed out after " + requestTimeout + "ms", t);
      }
      throw new EventDeliveryException(this + ": Failed to send event", t);
    }
  }

  private void append(Event event, long timeout, TimeUnit tu)
      throws EventDeliveryException {

    assertReady();

    final CallFuture<Status> callFuture = new CallFuture<Status>();

    final AvroFlumeEvent avroEvent = new AvroFlumeEvent();
    avroEvent.setBody(ByteBuffer.wrap(event.getBody()));
    avroEvent.setHeaders(toCharSeqMap(event.getHeaders()));

    Future<Void> handshake;
    try {
      // due to AVRO-1122, avroClient.append() may block
      handshake = callTimeoutPool.submit(new Callable<Void>() {

        @Override
        public Void call() throws Exception {
          avroClient.append(avroEvent, callFuture);
          return null;
        }
      });
    } catch (RejectedExecutionException ex) {
      throw new EventDeliveryException(this + ": Executor error", ex);
    }

    try {
      handshake.get(connectTimeout, TimeUnit.MILLISECONDS);
    } catch (TimeoutException ex) {
      throw new EventDeliveryException(this + ": Handshake timed out after " +
          connectTimeout + " ms", ex);
    } catch (InterruptedException ex) {
      throw new EventDeliveryException(this + ": Interrupted in handshake", ex);
    } catch (ExecutionException ex) {
      throw new EventDeliveryException(this + ": RPC request exception", ex);
    } catch (CancellationException ex) {
      throw new EventDeliveryException(this + ": RPC request cancelled", ex);
    } finally {
      if (!handshake.isDone()) {
        handshake.cancel(true);
      }
    }

    waitForStatusOK(callFuture, timeout, tu);
  }

  @Override
  public void appendBatch(List<Event> events) throws EventDeliveryException {
    try {
      appendBatch(events, requestTimeout, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      // we mark as no longer active without trying to clean up resources
      // client is required to call close() to clean up resources
      setState(ConnState.DEAD);
      if (t instanceof Error) {
        throw (Error) t;
      }
      if (t instanceof TimeoutException) {
        throw new EventDeliveryException(this + ": Failed to send event. " +
            "RPC request timed out after " + requestTimeout + " ms", t);
      }
      throw new EventDeliveryException(this + ": Failed to send batch", t);
    }
  }

  private void appendBatch(List<Event> events, long timeout, TimeUnit tu)
      throws EventDeliveryException {

    assertReady();

    Iterator<Event> iter = events.iterator();
    final List<AvroFlumeEvent> avroEvents = new LinkedList<AvroFlumeEvent>();

    // send multiple batches... bail if there is a problem at any time
    while (iter.hasNext()) {
      avroEvents.clear();

      for (int i = 0; i < batchSize && iter.hasNext(); i++) {
        Event event = iter.next();
        AvroFlumeEvent avroEvent = new AvroFlumeEvent();
        avroEvent.setBody(ByteBuffer.wrap(event.getBody()));
        avroEvent.setHeaders(toCharSeqMap(event.getHeaders()));
        avroEvents.add(avroEvent);
      }

      final CallFuture<Status> callFuture = new CallFuture<Status>();

      Future<Void> handshake;
      try {
        // due to AVRO-1122, avroClient.appendBatch() may block
        handshake = callTimeoutPool.submit(new Callable<Void>() {

          @Override
          public Void call() throws Exception {
            avroClient.appendBatch(avroEvents, callFuture);
            return null;
          }
        });
      } catch (RejectedExecutionException ex) {
        throw new EventDeliveryException(this + ": Executor error", ex);
      }

      try {
        handshake.get(connectTimeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException ex) {
        throw new EventDeliveryException(this + ": Handshake timed out after " +
            connectTimeout + "ms", ex);
      } catch (InterruptedException ex) {
        throw new EventDeliveryException(this + ": Interrupted in handshake",
            ex);
      } catch (ExecutionException ex) {
        throw new EventDeliveryException(this + ": RPC request exception", ex);
      } catch (CancellationException ex) {
        throw new EventDeliveryException(this + ": RPC request cancelled", ex);
      } finally {
        if (!handshake.isDone()) {
          handshake.cancel(true);
        }
      }

      waitForStatusOK(callFuture, timeout, tu);
    }
  }

  /**
   * Helper method that waits for a Status future to come back and validates
   * that it returns Status == OK.
   * @param callFuture Future to wait on
   * @param timeout Time to wait before failing
   * @param tu Time Unit of {@code timeout}
   * @throws EventDeliveryException If there is a timeout or if Status != OK
   */
  private void waitForStatusOK(CallFuture<Status> callFuture,
      long timeout, TimeUnit tu) throws EventDeliveryException {
    try {
      Status status = callFuture.get(timeout, tu);
      if (status != Status.OK) {
        throw new EventDeliveryException(this + ": Avro RPC call returned " +
            "Status: " + status);
      }
    } catch (CancellationException ex) {
      throw new EventDeliveryException(this + ": RPC future was cancelled", ex);
    } catch (ExecutionException ex) {
      throw new EventDeliveryException(this + ": Exception thrown from " +
          "remote handler", ex);
    } catch (TimeoutException ex) {
      throw new EventDeliveryException(this + ": RPC request timed out", ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new EventDeliveryException(this + ": RPC request interrupted", ex);
    }
  }

  /**
   * This method should always be used to change {@code connState} so we ensure
   * that invalid state transitions do not occur and that the {@code isIdle}
   * {@link Condition} variable gets signaled reliably.
   * Throws {@code IllegalStateException} when called to transition from CLOSED
   * to another state.
   * @param newState
   */
  private void setState(ConnState newState) {
    stateLock.lock();
    try {
      if (connState == ConnState.DEAD && connState != newState) {
        throw new IllegalStateException("Cannot transition from CLOSED state.");
      }
      connState = newState;
    } finally {
      stateLock.unlock();
    }
  }

  /**
   * If the connection state != READY, throws {@link EventDeliveryException}.
   */
  private void assertReady() throws EventDeliveryException {
    stateLock.lock();
    try {
      ConnState curState = connState;
      if (curState != ConnState.READY) {
        throw new EventDeliveryException("RPC failed, client in an invalid " +
            "state: " + curState);
      }
    } finally {
      stateLock.unlock();
    }
  }

  /**
   * Helper function to convert a map of String to a map of CharSequence.
   */
  private static Map<CharSequence, CharSequence> toCharSeqMap(
      Map<String, String> stringMap) {
    Map<CharSequence, CharSequence> charSeqMap =
        new HashMap<CharSequence, CharSequence>();
    for (Map.Entry<String, String> entry : stringMap.entrySet()) {
      charSeqMap.put(entry.getKey(), entry.getValue());
    }
    return charSeqMap;
  }

  @Override
  public boolean isActive() {
    stateLock.lock();
    try {
      return (connState == ConnState.READY);
    } finally {
      stateLock.unlock();
    }
  }

  private static enum ConnState {
    INIT, READY, DEAD
  }


    /**
   * <p>
   * Configure the actual client using the properties.
   * <tt>properties</tt> should have at least 2 params:
   * <p><tt>hosts</tt> = <i>alias_for_host</i></p>
   * <p><tt>alias_for_host</tt> = <i>hostname:port</i>. </p>
   * Only the first host is added, rest are discarded.</p>
   * <p>Optionally it can also have a <p>
   * <tt>batch-size</tt> = <i>batchSize</i>
   * @param properties The properties to instantiate the client with.
   * @return
     */
  @Override
  public synchronized void configure(Properties properties)
      throws FlumeException {
    stateLock.lock();
    try{
      if(connState == ConnState.READY || connState == ConnState.DEAD){
        throw new FlumeException("This client was already configured, " +
            "cannot reconfigure.");
      }
    } finally {
      stateLock.unlock();
    }

    // batch size
    String strBatchSize = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_BATCH_SIZE);
    logger.debug("Batch size string = " + strBatchSize);
    batchSize = RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE;
    if (strBatchSize != null && !strBatchSize.isEmpty()) {
      try {
        int parsedBatch = Integer.parseInt(strBatchSize);
        if (parsedBatch < 1) {
          logger.warn("Invalid value for batchSize: {}; Using default value.", parsedBatch);
        } else {
          batchSize = parsedBatch;
        }
      } catch (NumberFormatException e) {
        logger.warn("Batchsize is not valid for RpcClient: " + strBatchSize +
            ". Default value assigned.", e);
      }
    }

    // host and port
    String hostNames = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_HOSTS);
    String[] hosts = null;
    if (hostNames != null && !hostNames.isEmpty()) {
      hosts = hostNames.split("\\s+");
    } else {
      throw new FlumeException("Hosts list is invalid: " + hostNames);
    }

    if (hosts.length > 1) {
      logger.warn("More than one hosts are specified for the default client. "
          + "Only the first host will be used and others ignored. Specified: "
          + hostNames + "; to be used: " + hosts[0]);
    }

    String host = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX+hosts[0]);
    if (host == null || host.isEmpty()) {
      throw new FlumeException("Host not found: " + hosts[0]);
    }
    String[] hostAndPort = host.split(":");
    if (hostAndPort.length != 2){
      throw new FlumeException("Invalid hostname: " + hosts[0]);
    }
    Integer port = null;
    try {
      port = Integer.parseInt(hostAndPort[1]);
    } catch (NumberFormatException e) {
      throw new FlumeException("Invalid Port: " + hostAndPort[1], e);
    }
    this.address = new InetSocketAddress(hostAndPort[0], port);

    // connect timeout
    connectTimeout =
        RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS;
    String strConnTimeout = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_CONNECT_TIMEOUT);
    if (strConnTimeout != null && strConnTimeout.trim().length() > 0) {
      try {
        connectTimeout = Long.parseLong(strConnTimeout);
        if (connectTimeout < 1000) {
          logger.warn("Connection timeout specified less than 1s. " +
              "Using default value instead.");
          connectTimeout =
              RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS;
        }
      } catch (NumberFormatException ex) {
        logger.error("Invalid connect timeout specified: " + strConnTimeout);
      }
    }

    // request timeout
    requestTimeout =
        RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS;
    String strReqTimeout = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_REQUEST_TIMEOUT);
    if  (strReqTimeout != null && strReqTimeout.trim().length() > 0) {
      try {
        requestTimeout = Long.parseLong(strReqTimeout);
        if (requestTimeout < 1000) {
          logger.warn("Request timeout specified less than 1s. " +
              "Using default value instead.");
          requestTimeout =
              RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT_MILLIS;
        }
      } catch (NumberFormatException ex) {
        logger.error("Invalid request timeout specified: " + strReqTimeout);
      }
    }

    String enableCompressionStr = properties.getProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_TYPE);
    if (enableCompressionStr != null && enableCompressionStr.equalsIgnoreCase("deflate")) {
      this.enableDeflateCompression = true;
      String compressionLvlStr = properties.getProperty(RpcClientConfigurationConstants.CONFIG_COMPRESSION_LEVEL);
      compressionLevel = RpcClientConfigurationConstants.DEFAULT_COMPRESSION_LEVEL;
      if (compressionLvlStr != null) {
        try {
          compressionLevel = Integer.parseInt(compressionLvlStr);
        } catch (NumberFormatException ex) {
          logger.error("Invalid compression level: " + compressionLvlStr);
        }
      }
    }

    enableSsl = Boolean.parseBoolean(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_SSL));
    trustAllCerts = Boolean.parseBoolean(properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_TRUST_ALL_CERTS));
    truststore = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_TRUSTSTORE);
    truststorePassword = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_TRUSTSTORE_PASSWORD);
    truststoreType = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_TRUSTSTORE_TYPE, "JKS");

    String maxIoWorkersStr = properties.getProperty(
      RpcClientConfigurationConstants.MAX_IO_WORKERS);
    if (!StringUtils.isEmpty(maxIoWorkersStr)) {
      try {
        maxIoWorkers = Integer.parseInt(maxIoWorkersStr);
      } catch (NumberFormatException ex) {
        logger.warn ("Invalid maxIOWorkers:" + maxIoWorkersStr + " Using " +
          "default maxIOWorkers.");
        maxIoWorkers = -1;
      }
    }

    if (maxIoWorkers < 1) {
      logger.warn("Using default maxIOWorkers");
      maxIoWorkers = -1;
    }

    this.connect();
  }

  /**
   * A thread factor implementation modeled after the implementation of
   * NettyTransceiver.NettyTransceiverThreadFactory class which is
   * a private static class. The only difference between that and this
   * implementation is that this implementation marks all the threads daemon
   * which allows the termination of the VM when the non-daemon threads
   * are done.
   */
  private static class TransceiverThreadFactory implements ThreadFactory {
    private final AtomicInteger threadId = new AtomicInteger(0);
    private final String prefix;

    /**
     * Creates a TransceiverThreadFactory that creates threads with the
     * specified name.
     * @param prefix the name prefix to use for all threads created by this
     * ThreadFactory.  A unique ID will be appended to this prefix to form the
     * final thread name.
     */
    public TransceiverThreadFactory(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setDaemon(true);
      thread.setName(prefix + " " + threadId.incrementAndGet());
      return thread;
    }
  }

  /**
   * Factory of SSL-enabled client channels
   * Copied from Avro's org.apache.avro.ipc.TestNettyServerWithSSL test
   */
  private static class SSLCompressionChannelFactory extends NioClientSocketChannelFactory {

    private final boolean enableCompression;
    private final int compressionLevel;
    private final boolean enableSsl;
    private final boolean trustAllCerts;
    private final String truststore;
    private final String truststorePassword;
    private final String truststoreType;

    public SSLCompressionChannelFactory(Executor bossExecutor, Executor workerExecutor,
        boolean enableCompression, boolean enableSsl, boolean trustAllCerts,
        int compressionLevel, String truststore, String truststorePassword,
        String truststoreType) {
      super(bossExecutor, workerExecutor);
      this.enableCompression = enableCompression;
      this.enableSsl = enableSsl;
      this.compressionLevel = compressionLevel;
      this.trustAllCerts = trustAllCerts;
      this.truststore = truststore;
      this.truststorePassword = truststorePassword;
      this.truststoreType = truststoreType;
    }

    public SSLCompressionChannelFactory(Executor bossExecutor, Executor workerExecutor,
        boolean enableCompression, boolean enableSsl, boolean trustAllCerts,
        int compressionLevel, String truststore, String truststorePassword,
        String truststoreType, int maxIOWorkers) {
      super(bossExecutor, workerExecutor, maxIOWorkers);
      this.enableCompression = enableCompression;
      this.enableSsl = enableSsl;
      this.compressionLevel = compressionLevel;
      this.trustAllCerts = trustAllCerts;
      this.truststore = truststore;
      this.truststorePassword = truststorePassword;
      this.truststoreType = truststoreType;
    }

    @Override
    public SocketChannel newChannel(ChannelPipeline pipeline) {
      TrustManager[] managers;
      try {
        if (enableCompression) {
          ZlibEncoder encoder = new ZlibEncoder(compressionLevel);
          pipeline.addFirst("deflater", encoder);
          pipeline.addFirst("inflater", new ZlibDecoder());
        }
        if (enableSsl) {
          if (trustAllCerts) {
            logger.warn("No truststore configured, setting TrustManager to accept"
                + " all server certificates");
            managers = new TrustManager[] { new PermissiveTrustManager() };
          } else {
            KeyStore keystore = null;

            if (truststore != null) {
              if (truststorePassword == null) {
                throw new NullPointerException("truststore password is null");
              }
              InputStream truststoreStream = new FileInputStream(truststore);
              keystore = KeyStore.getInstance(truststoreType);
              keystore.load(truststoreStream, truststorePassword.toCharArray());
            }

            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            // null keystore is OK, with SunX509 it defaults to system CA Certs
            // see http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#X509TrustManager
            tmf.init(keystore);
            managers = tmf.getTrustManagers();
          }

          SSLContext sslContext = SSLContext.getInstance("TLS");
          sslContext.init(null, managers, null);
          SSLEngine sslEngine = sslContext.createSSLEngine();
          sslEngine.setUseClientMode(true);
          // addFirst() will make SSL handling the first stage of decoding
          // and the last stage of encoding this must be added after
          // adding compression handling above
          pipeline.addFirst("ssl", new SslHandler(sslEngine));
        }

        return super.newChannel(pipeline);
      } catch (Exception ex) {
        logger.error("Cannot create SSL channel", ex);
        throw new RuntimeException("Cannot create SSL channel", ex);
      }
    }
  }

  /**
   * Permissive trust manager accepting any certificate
   */
  private static class PermissiveTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] certs, String s) {
      // nothing
    }

    @Override
    public void checkServerTrusted(X509Certificate[] certs, String s) {
      // nothing
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }
}
