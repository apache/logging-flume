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

package org.apache.flume.source;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A {@link Source} implementation that receives Avro events from clients that
 * implement {@link AvroSourceProtocol}.
 * </p>
 * <p>
 * This source forms one half of Flume's tiered collection support. Internally,
 * this source uses Avro's <tt>NettyTransceiver</tt> to listen for, and handle
 * events. It can be paired with the builtin <tt>AvroSink</tt> to create tiered
 * collection topologies. Of course, nothing prevents one from using this source
 * to receive data from other custom built infrastructure that uses the same
 * Avro protocol (specifically {@link AvroSourceProtocol}).
 * </p>
 * <p>
 * Events may be received from the client either singly or in batches.Generally,
 * larger batches are far more efficient, but introduce a slight delay (measured
 * in millis) in delivery. A batch submitted to the configured {@link Channel}
 * atomically (i.e. either all events make it into the channel or none).
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>bind</tt></td>
 * <td>The hostname or IP to which the source will bind.</td>
 * <td>Hostname or IP / String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which the source will bind and listen for events.</td>
 * <td>TCP port / int</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>threads</tt></td>
 * <td>Max number of threads assigned to thread pool, 0 being unlimited</td>
 * <td>Count / int</td>
 * <td>0(optional)</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class AvroSource extends AbstractSource implements EventDrivenSource,
    Configurable, AvroSourceProtocol {

  private static final String THREADS = "threads";

  private static final Logger logger = LoggerFactory
      .getLogger(AvroSource.class);

  private static final String PORT_KEY = "port";
  private static final String BIND_KEY = "bind";
  private static final String COMPRESSION_TYPE = "compression-type";
  private static final String SSL_KEY = "ssl";
  private static final String KEYSTORE_KEY = "keystore";
  private static final String KEYSTORE_PASSWORD_KEY = "keystore-password";
  private static final String KEYSTORE_TYPE_KEY = "keystore-type";
  private int port;
  private String bindAddress;
  private String compressionType;
  private String keystore;
  private String keystorePassword;
  private String keystoreType;
  private boolean enableSsl = false;

  private Server server;
  private SourceCounter sourceCounter;

  private int maxThreads;
  private ScheduledExecutorService connectionCountUpdater;

  @Override
  public void configure(Context context) {
    Configurables.ensureRequiredNonNull(context, PORT_KEY, BIND_KEY);

    port = context.getInteger(PORT_KEY);
    bindAddress = context.getString(BIND_KEY);
    compressionType = context.getString(COMPRESSION_TYPE, "none");

    try {
      maxThreads = context.getInteger(THREADS, 0);
    } catch (NumberFormatException e) {
      logger.warn("AVRO source\'s \"threads\" property must specify an integer value.",
              context.getString(THREADS));
    }

    enableSsl = context.getBoolean(SSL_KEY, false);
    keystore = context.getString(KEYSTORE_KEY);
    keystorePassword = context.getString(KEYSTORE_PASSWORD_KEY);
    keystoreType = context.getString(KEYSTORE_TYPE_KEY, "JKS");

    if (enableSsl) {
      Preconditions.checkNotNull(keystore,
          KEYSTORE_KEY + " must be specified when SSL is enabled");
      Preconditions.checkNotNull(keystorePassword,
          KEYSTORE_PASSWORD_KEY + " must be specified when SSL is enabled");
      try {
        KeyStore ks = KeyStore.getInstance(keystoreType);
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray());
      } catch (Exception ex) {
        throw new FlumeException(
            "Avro source configured with invalid keystore: " + keystore, ex);
      }
    }

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public void start() {
    logger.info("Starting {}...", this);

    Responder responder = new SpecificResponder(AvroSourceProtocol.class, this);

    NioServerSocketChannelFactory socketChannelFactory = initSocketChannelFactory();

    ChannelPipelineFactory pipelineFactory = initChannelPipelineFactory();

    server = new NettyServer(responder, new InetSocketAddress(bindAddress, port),
          socketChannelFactory, pipelineFactory, null);

    connectionCountUpdater = Executors.newSingleThreadScheduledExecutor();
    server.start();
    sourceCounter.start();
    super.start();
    final NettyServer srv = (NettyServer)server;
    connectionCountUpdater.scheduleWithFixedDelay(new Runnable(){

      @Override
      public void run() {
        sourceCounter.setOpenConnectionCount(
                Long.valueOf(srv.getNumActiveConnections()));
      }
    }, 0, 60, TimeUnit.SECONDS);

    logger.info("Avro source {} started.", getName());
  }

  private NioServerSocketChannelFactory initSocketChannelFactory() {
    NioServerSocketChannelFactory socketChannelFactory;
    if (maxThreads <= 0) {
      socketChannelFactory = new NioServerSocketChannelFactory
          (Executors .newCachedThreadPool(), Executors.newCachedThreadPool());
    } else {
      socketChannelFactory = new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newFixedThreadPool(maxThreads));
    }
    return socketChannelFactory;
  }

  private ChannelPipelineFactory initChannelPipelineFactory() {
    ChannelPipelineFactory pipelineFactory;
    boolean enableCompression = compressionType.equalsIgnoreCase("deflate");
    if (enableCompression || enableSsl) {
      pipelineFactory = new SSLCompressionChannelPipelineFactory(
          enableCompression, enableSsl, keystore,
          keystorePassword, keystoreType);
    } else {
      pipelineFactory = new ChannelPipelineFactory() {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
          return Channels.pipeline();
        }
      };
    }
    return pipelineFactory;
  }

  @Override
  public void stop() {
    logger.info("Avro source {} stopping: {}", getName(), this);

    server.close();

    try {
      server.join();
    } catch (InterruptedException e) {
      logger.info("Avro source " + getName() + ": Interrupted while waiting " +
          "for Avro server to stop. Exiting. Exception follows.", e);
    }
    sourceCounter.stop();
    connectionCountUpdater.shutdown();
    while(!connectionCountUpdater.isTerminated()){
      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        logger.error("Interrupted while waiting for connection count executor "
                + "to terminate", ex);
        Throwables.propagate(ex);
      }
    }
    super.stop();
    logger.info("Avro source {} stopped. Metrics: {}", getName(),
        sourceCounter);
  }

  @Override
  public String toString() {
    return "Avro source " + getName() + ": { bindAddress: " + bindAddress +
        ", port: " + port + " }";
  }

  /**
   * Helper function to convert a map of CharSequence to a map of String.
   */
  private static Map<String, String> toStringMap(
      Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap =
        new HashMap<String, String>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }

  @Override
  public Status append(AvroFlumeEvent avroEvent) {
    logger.debug("Avro source {}: Received avro event: {}", getName(),
        avroEvent);
    sourceCounter.incrementAppendReceivedCount();
    sourceCounter.incrementEventReceivedCount();

    Event event = EventBuilder.withBody(avroEvent.getBody().array(),
        toStringMap(avroEvent.getHeaders()));

    try {
      getChannelProcessor().processEvent(event);
    } catch (ChannelException ex) {
      logger.warn("Avro source " + getName() + ": Unable to process event. " +
          "Exception follows.", ex);
      return Status.FAILED;
    }

    sourceCounter.incrementAppendAcceptedCount();
    sourceCounter.incrementEventAcceptedCount();

    return Status.OK;
  }

  @Override
  public Status appendBatch(List<AvroFlumeEvent> events) {
    logger.debug("Avro source {}: Received avro event batch of {} events.",
        getName(), events.size());
    sourceCounter.incrementAppendBatchReceivedCount();
    sourceCounter.addToEventReceivedCount(events.size());

    List<Event> batch = new ArrayList<Event>();

    for (AvroFlumeEvent avroEvent : events) {
      Event event = EventBuilder.withBody(avroEvent.getBody().array(),
          toStringMap(avroEvent.getHeaders()));

      batch.add(event);
    }

    try {
      getChannelProcessor().processEventBatch(batch);
    } catch (Throwable t) {
      logger.error("Avro source " + getName() + ": Unable to process event " +
          "batch. Exception follows.", t);
      if (t instanceof Error) {
        throw (Error) t;
      }
      return Status.FAILED;
    }

    sourceCounter.incrementAppendBatchAcceptedCount();
    sourceCounter.addToEventAcceptedCount(events.size());

    return Status.OK;
  }

  /**
   * Factory of SSL-enabled server worker channel pipelines
   * Copied from Avro's org.apache.avro.ipc.TestNettyServerWithSSL test
   */
  private static class SSLCompressionChannelPipelineFactory
      implements ChannelPipelineFactory {

    private boolean enableCompression;
    private boolean enableSsl;
    private String keystore;
    private String keystorePassword;
    private String keystoreType;

    public SSLCompressionChannelPipelineFactory(boolean enableCompression, boolean enableSsl, String keystore, String keystorePassword, String keystoreType) {
      this.enableCompression = enableCompression;
      this.enableSsl = enableSsl;
      this.keystore = keystore;
      this.keystorePassword = keystorePassword;
      this.keystoreType = keystoreType;
    }

    private SSLContext createServerSSLContext() {
      try {
        KeyStore ks = KeyStore.getInstance(keystoreType);
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray());

        // Set up key manager factory to use our key store
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(getAlgorithm());
        kmf.init(ks, keystorePassword.toCharArray());

        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(kmf.getKeyManagers(), null, null);
        return serverContext;
      } catch (Exception e) {
        throw new Error("Failed to initialize the server-side SSLContext", e);
      }
    }

    private String getAlgorithm() {
      String algorithm = Security.getProperty(
          "ssl.KeyManagerFactory.algorithm");
      if (algorithm == null) {
        algorithm = "SunX509";
      }
      return algorithm;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      if (enableCompression) {
        ZlibEncoder encoder = new ZlibEncoder(6);
        pipeline.addFirst("deflater", encoder);
        pipeline.addFirst("inflater", new ZlibDecoder());
      }
      if (enableSsl) {
        SSLEngine sslEngine = createServerSSLContext().createSSLEngine();
        sslEngine.setUseClientMode(false);
        // addFirst() will make SSL handling the first stage of decoding
        // and the last stage of encoding this must be added after
        // adding compression handling above
        pipeline.addFirst("ssl", new SslHandler(sslEngine));
      }
      return pipeline;
    }
  }
}
