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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.FlumeAuthenticator;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.thrift.Status;
import org.apache.flume.thrift.ThriftSourceProtocol;
import org.apache.flume.thrift.ThriftFlumeEvent;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TSaslServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLServerSocket;
import javax.security.sasl.Sasl;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.security.PrivilegedAction;

public class ThriftSource extends AbstractSource implements Configurable, EventDrivenSource {

  public static final Logger logger = LoggerFactory.getLogger(ThriftSource.class);

  /**
   * Config param for the maximum number of threads this source should use to
   * handle incoming data.
   */
  public static final String CONFIG_THREADS = "threads";
  /**
   * Config param for the hostname to listen on.
   */
  public static final String CONFIG_BIND = "bind";
  /**
   * Config param for the port to listen on.
   */
  public static final String CONFIG_PORT = "port";
  /**
   * Config param for the thrift protocol to use.
   */
  public static final String CONFIG_PROTOCOL = "protocol";
  public static final String BINARY_PROTOCOL = "binary";
  public static final String COMPACT_PROTOCOL = "compact";

  private static final String SSL_KEY = "ssl";
  private static final String KEYSTORE_KEY = "keystore";
  private static final String KEYSTORE_PASSWORD_KEY = "keystore-password";
  private static final String KEYSTORE_TYPE_KEY = "keystore-type";
  private static final String EXCLUDE_PROTOCOLS = "exclude-protocols";

  private static final String KERBEROS_KEY = "kerberos";
  private static final String AGENT_PRINCIPAL = "agent-principal";
  private static final String AGENT_KEYTAB = "agent-keytab";

  private Integer port;
  private String bindAddress;
  private int maxThreads = 0;
  private SourceCounter sourceCounter;
  private TServer server;
  private ExecutorService servingExecutor;
  private String protocol;
  private String keystore;
  private String keystorePassword;
  private String keystoreType;
  private final List<String> excludeProtocols = new LinkedList<String>();
  private boolean enableSsl = false;
  private boolean enableKerberos = false;
  private String principal;
  private FlumeAuthenticator flumeAuth;

  @Override
  public void configure(Context context) {
    logger.info("Configuring thrift source.");
    port = context.getInteger(CONFIG_PORT);
    Preconditions.checkNotNull(port, "Port must be specified for Thrift " +
        "Source.");
    bindAddress = context.getString(CONFIG_BIND);
    Preconditions.checkNotNull(bindAddress, "Bind address must be specified " +
        "for Thrift Source.");

    try {
      maxThreads = context.getInteger(CONFIG_THREADS, 0);
      maxThreads = (maxThreads <= 0) ? Integer.MAX_VALUE : maxThreads;
    } catch (NumberFormatException e) {
      logger.warn("Thrift source\'s \"threads\" property must specify an " +
                  "integer value: " + context.getString(CONFIG_THREADS));
    }

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }

    protocol = context.getString(CONFIG_PROTOCOL);
    if (protocol == null) {
      // default is to use the compact protocol.
      protocol = COMPACT_PROTOCOL;
    }
    Preconditions.checkArgument(
        (protocol.equalsIgnoreCase(BINARY_PROTOCOL) ||
                protocol.equalsIgnoreCase(COMPACT_PROTOCOL)),
        "binary or compact are the only valid Thrift protocol types to " +
                "choose from.");

    enableSsl = context.getBoolean(SSL_KEY, false);
    if (enableSsl) {
      keystore = context.getString(KEYSTORE_KEY);
      keystorePassword = context.getString(KEYSTORE_PASSWORD_KEY);
      keystoreType = context.getString(KEYSTORE_TYPE_KEY, "JKS");
      String excludeProtocolsStr = context.getString(EXCLUDE_PROTOCOLS);
      if (excludeProtocolsStr == null) {
        excludeProtocols.add("SSLv3");
      } else {
        excludeProtocols.addAll(Arrays.asList(excludeProtocolsStr.split(" ")));
        if (!excludeProtocols.contains("SSLv3")) {
          excludeProtocols.add("SSLv3");
        }
      }
      Preconditions.checkNotNull(keystore,
              KEYSTORE_KEY + " must be specified when SSL is enabled");
      Preconditions.checkNotNull(keystorePassword,
              KEYSTORE_PASSWORD_KEY + " must be specified when SSL is enabled");
      try {
        KeyStore ks = KeyStore.getInstance(keystoreType);
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray());
      } catch (Exception ex) {
        throw new FlumeException(
                "Thrift source configured with invalid keystore: " + keystore, ex);
      }
    }

    principal = context.getString(AGENT_PRINCIPAL);
    String keytab = context.getString(AGENT_KEYTAB);
    enableKerberos = context.getBoolean(KERBEROS_KEY, false);
    this.flumeAuth = FlumeAuthenticationUtil.getAuthenticator(principal, keytab);
    if (enableKerberos) {
      if (!flumeAuth.isAuthenticated()) {
        throw new FlumeException("Authentication failed in Kerberos mode for " +
                "principal " + principal + " keytab " + keytab);
      }
      flumeAuth.startCredentialRefresher();
    }
  }

  @Override
  public void start() {
    logger.info("Starting thrift source");

    // create the server
    server = getTThreadedSelectorServer();

    // if in ssl mode or if SelectorServer is unavailable
    if (server == null) {
      server = getTThreadPoolServer();
    }

    servingExecutor = Executors.newSingleThreadExecutor(new
      ThreadFactoryBuilder().setNameFormat("Flume Thrift Source I/O Boss")
      .build());

    /**
     * Start serving.
     */
    servingExecutor.submit(new Runnable() {
      @Override
      public void run() {
        flumeAuth.execute(new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            server.serve();
            return null;
          }
        });
      }
    });

    long timeAfterStart = System.currentTimeMillis();
    while (!server.isServing()) {
      try {
        if (System.currentTimeMillis() - timeAfterStart >= 10000) {
          throw new FlumeException("Thrift server failed to start!");
        }
        TimeUnit.MILLISECONDS.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new FlumeException("Interrupted while waiting for Thrift server" +
            " to start.", e);
      }
    }
    sourceCounter.start();
    logger.info("Started Thrift source.");
    super.start();
  }

  private String getkeyManagerAlgorithm() {
    String algorithm = Security.getProperty(
            "ssl.KeyManagerFactory.algorithm");
    return (algorithm != null) ?
            algorithm : KeyManagerFactory.getDefaultAlgorithm();
  }

  private TServerTransport getSSLServerTransport() {
    try {
      TServerTransport transport;
      TSSLTransportFactory.TSSLTransportParameters params =
              new TSSLTransportFactory.TSSLTransportParameters();

      params.setKeyStore(keystore, keystorePassword, getkeyManagerAlgorithm(), keystoreType);
      transport = TSSLTransportFactory.getServerSocket(
              port, 120000, InetAddress.getByName(bindAddress), params);

      ServerSocket serverSock = ((TServerSocket) transport).getServerSocket();
      if (serverSock instanceof SSLServerSocket) {
        SSLServerSocket sslServerSock = (SSLServerSocket) serverSock;
        List<String> enabledProtocols = new ArrayList<String>();
        for (String protocol : sslServerSock.getEnabledProtocols()) {
          if (!excludeProtocols.contains(protocol)) {
            enabledProtocols.add(protocol);
          }
        }
        sslServerSock.setEnabledProtocols(enabledProtocols.toArray(new String[0]));
      }
      return transport;
    } catch (Throwable throwable) {
      throw new FlumeException("Cannot start Thrift source.", throwable);
    }
  }

  private TServerTransport getTServerTransport() {
    try {
      return new TServerSocket(new InetSocketAddress(bindAddress, port));
    } catch (Throwable throwable) {
      throw new FlumeException("Cannot start Thrift source.", throwable);
    }
  }

  private TProtocolFactory getProtocolFactory() {
    if (protocol.equals(BINARY_PROTOCOL)) {
      logger.info("Using TBinaryProtocol");
      return new TBinaryProtocol.Factory();
    } else {
      logger.info("Using TCompactProtocol");
      return new TCompactProtocol.Factory();
    }
  }

  private TServer getTThreadedSelectorServer() {
    if (enableSsl || enableKerberos) {
      return null;
    }
    Class<?> serverClass;
    Class<?> argsClass;
    TServer.AbstractServerArgs args;
    try {
      serverClass = Class.forName("org.apache.thrift" +
              ".server.TThreadedSelectorServer");
      argsClass = Class.forName("org.apache.thrift" +
              ".server.TThreadedSelectorServer$Args");

      TServerTransport serverTransport = new TNonblockingServerSocket(
              new InetSocketAddress(bindAddress, port));

      ExecutorService sourceService;
      ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
              "Flume Thrift IPC Thread %d").build();
      if (maxThreads == 0) {
        sourceService = Executors.newCachedThreadPool(threadFactory);
      } else {
        sourceService = Executors.newFixedThreadPool(maxThreads, threadFactory);
      }
      args = (TNonblockingServer.AbstractNonblockingServerArgs) argsClass
              .getConstructor(TNonblockingServerTransport.class)
              .newInstance(serverTransport);
      Method m = argsClass.getDeclaredMethod("executorService",
              ExecutorService.class);
      m.invoke(args, sourceService);

      populateServerParams(args);

      /*
       * Both THsHaServer and TThreadedSelectorServer allows us to pass in
       * the executor service to use - unfortunately the "executorService"
       * method does not exist in the parent abstract Args class,
       * so use reflection to pass the executor in.
       *
       */
      server = (TServer) serverClass.getConstructor(argsClass).newInstance(args);
    } catch (ClassNotFoundException e) {
      return null;
    } catch (Throwable ex) {
      throw new FlumeException("Cannot start Thrift Source.", ex);
    }
    return server;
  }

  private TServer getTThreadPoolServer() {
    TServerTransport serverTransport;
    if (enableSsl) {
      serverTransport = getSSLServerTransport();
    } else {
      serverTransport = getTServerTransport();
    }
    TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
    serverArgs.maxWorkerThreads(maxThreads);
    populateServerParams(serverArgs);
    return new TThreadPoolServer(serverArgs);
  }

  private void populateServerParams(TServer.AbstractServerArgs args) {
    //populate the ProtocolFactory
    args.protocolFactory(getProtocolFactory());

    //populate the transportFactory
    if (enableKerberos) {
      args.transportFactory(getSASLTransportFactory());
    } else {
      args.transportFactory(new TFastFramedTransport.Factory());
    }

    // populate the  Processor
    args.processor(new ThriftSourceProtocol
            .Processor<ThriftSourceHandler>(new ThriftSourceHandler()));
  }

  private TTransportFactory getSASLTransportFactory() {
    String[] names;
    try {
      names = FlumeAuthenticationUtil.splitKerberosName(principal);
    } catch (IOException e) {
      throw new FlumeException(
              "Error while trying to resolve Principal name - " + principal, e);
    }
    Map<String, String> saslProperties = new HashMap<String, String>();
    saslProperties.put(Sasl.QOP, "auth");
    TSaslServerTransport.Factory saslTransportFactory =
            new TSaslServerTransport.Factory();
    saslTransportFactory.addServerDefinition(
            "GSSAPI", names[0], names[1], saslProperties,
            FlumeAuthenticationUtil.getSaslGssCallbackHandler());
    return saslTransportFactory;
  }

  @Override
  public void stop() {
    if (server != null && server.isServing()) {
      server.stop();
    }
    if (servingExecutor != null) {
      servingExecutor.shutdown();
      try {
        if (!servingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          servingExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        throw new FlumeException("Interrupted while waiting for server to be " +
          "shutdown.");
      }
    }
    sourceCounter.stop();
    super.stop();
  }

  private class ThriftSourceHandler implements ThriftSourceProtocol.Iface {

    @Override
    public Status append(ThriftFlumeEvent event) throws TException {
      Event flumeEvent = EventBuilder.withBody(event.getBody(), event.getHeaders());

      sourceCounter.incrementAppendReceivedCount();
      sourceCounter.incrementEventReceivedCount();

      try {
        getChannelProcessor().processEvent(flumeEvent);
      } catch (ChannelException ex) {
        logger.warn("Thrift source " + getName() + " could not append events " +
                    "to the channel.", ex);
        return Status.FAILED;
      }
      sourceCounter.incrementAppendAcceptedCount();
      sourceCounter.incrementEventAcceptedCount();
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<ThriftFlumeEvent> events) throws TException {
      sourceCounter.incrementAppendBatchReceivedCount();
      sourceCounter.addToEventReceivedCount(events.size());

      List<Event> flumeEvents = Lists.newArrayList();
      for (ThriftFlumeEvent event : events) {
        flumeEvents.add(EventBuilder.withBody(event.getBody(), event.getHeaders()));
      }

      try {
        getChannelProcessor().processEventBatch(flumeEvents);
      } catch (ChannelException ex) {
        logger.warn("Thrift source %s could not append events to the channel.", getName());
        return Status.FAILED;
      }

      sourceCounter.incrementAppendBatchAcceptedCount();
      sourceCounter.addToEventAcceptedCount(events.size());
      return Status.OK;
    }
  }
}
