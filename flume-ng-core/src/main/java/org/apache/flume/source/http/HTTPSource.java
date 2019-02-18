/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.source.http;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.SslContextAwareAbstractSource;
import org.apache.flume.tools.FlumeBeanConfigurator;
import org.apache.flume.tools.HTTPServerConstraintUtil;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A source which accepts Flume Events by HTTP POST and GET. GET should be used
 * for experimentation only. HTTP requests are converted into flume events by a
 * pluggable "handler" which must implement the
 * {@linkplain HTTPSourceHandler} interface. This handler takes a
 * {@linkplain HttpServletRequest} and returns a list of flume events.
 *
 * The source accepts the following parameters: <p> <tt>port</tt>: port to which
 * the server should bind. Mandatory <p> <tt>handler</tt>: the class that
 * deserializes a HttpServletRequest into a list of flume events. This class
 * must implement HTTPSourceHandler. Default:
 * {@linkplain JSONHandler}. <p> <tt>handler.*</tt> Any configuration
 * to be passed to the handler. <p>
 *
 * All events deserialized from one Http request are committed to the channel in
 * one transaction, thus allowing for increased efficiency on channels like the
 * file channel. If the handler throws an exception this source will return
 * a HTTP status of 400. If the channel is full, or the source is unable to
 * append events to the channel, the source will return a HTTP 503 - Temporarily
 * unavailable status.
 *
 * A JSON handler which converts JSON objects to Flume events is provided.
 *
 */
public class HTTPSource extends SslContextAwareAbstractSource implements
        EventDrivenSource, Configurable {
  /*
   * There are 2 ways of doing this:
   * a. Have a static server instance and use connectors in each source
   *    which binds to the port defined for that source.
   * b. Each source starts its own server instance, which binds to the source's
   *    port.
   *
   * b is more efficient than a because Jetty does not allow binding a
   * servlet to a connector. So each request will need to go through each
   * each of the handlers/servlet till the correct one is found.
   *
   */

  private static final Logger LOG = LoggerFactory.getLogger(HTTPSource.class);
  private volatile Integer port;
  private volatile Server srv;
  private volatile String host;
  private HTTPSourceHandler handler;
  private SourceCounter sourceCounter;

  private Context sourceContext;

  @Override
  public void configure(Context context) {
    configureSsl(context);
    sourceContext = context;
    try {
      port = context.getInteger(HTTPSourceConfigurationConstants.CONFIG_PORT);
      host = context.getString(HTTPSourceConfigurationConstants.CONFIG_BIND,
          HTTPSourceConfigurationConstants.DEFAULT_BIND);

      Preconditions.checkState(host != null && !host.isEmpty(),
                "HTTPSource hostname specified is empty");
      Preconditions.checkNotNull(port, "HTTPSource requires a port number to be"
          + " specified");

      String handlerClassName = context.getString(
              HTTPSourceConfigurationConstants.CONFIG_HANDLER,
              HTTPSourceConfigurationConstants.DEFAULT_HANDLER).trim();

      @SuppressWarnings("unchecked")
      Class<? extends HTTPSourceHandler> clazz =
              (Class<? extends HTTPSourceHandler>)
              Class.forName(handlerClassName);
      handler = clazz.getDeclaredConstructor().newInstance();

      Map<String, String> subProps =
              context.getSubProperties(
              HTTPSourceConfigurationConstants.CONFIG_HANDLER_PREFIX);
      handler.configure(new Context(subProps));
    } catch (ClassNotFoundException ex) {
      LOG.error("Error while configuring HTTPSource. Exception follows.", ex);
      Throwables.propagate(ex);
    } catch (ClassCastException ex) {
      LOG.error("Deserializer is not an instance of HTTPSourceHandler."
              + "Deserializer must implement HTTPSourceHandler.");
      Throwables.propagate(ex);
    } catch (Exception ex) {
      LOG.error("Error configuring HTTPSource!", ex);
      Throwables.propagate(ex);
    }
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public void start() {
    Preconditions.checkState(srv == null,
            "Running HTTP Server found in source: " + getName()
            + " before I started one."
            + "Will not attempt to start.");
    QueuedThreadPool threadPool = new QueuedThreadPool();
    if (sourceContext.getSubProperties("QueuedThreadPool.").size() > 0) {
      FlumeBeanConfigurator.setConfigurationFields(threadPool, sourceContext);
    }
    srv = new Server(threadPool);

    //Register with JMX for advanced monitoring
    MBeanContainer mbContainer = new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
    srv.addEventListener(mbContainer);
    srv.addBean(mbContainer);

    HttpConfiguration httpConfiguration = new HttpConfiguration();
    httpConfiguration.addCustomizer(new SecureRequestCustomizer());

    FlumeBeanConfigurator.setConfigurationFields(httpConfiguration, sourceContext);
    ServerConnector connector = getSslContextSupplier().get().map(sslContext -> {
      SslContextFactory sslCtxFactory = new SslContextFactory();
      sslCtxFactory.setSslContext(sslContext);
      sslCtxFactory.setExcludeProtocols(getExcludeProtocols().toArray(new String[]{}));
      sslCtxFactory.setIncludeProtocols(getIncludeProtocols().toArray(new String[]{}));
      sslCtxFactory.setExcludeCipherSuites(getExcludeCipherSuites().toArray(new String[]{}));
      sslCtxFactory.setIncludeCipherSuites(getIncludeCipherSuites().toArray(new String[]{}));

      FlumeBeanConfigurator.setConfigurationFields(sslCtxFactory, sourceContext);

      httpConfiguration.setSecurePort(port);
      httpConfiguration.setSecureScheme("https");

      return new ServerConnector(srv,
        new SslConnectionFactory(sslCtxFactory, HttpVersion.HTTP_1_1.asString()),
        new HttpConnectionFactory(httpConfiguration));
    }).orElse(
        new ServerConnector(srv, new HttpConnectionFactory(httpConfiguration))
    );

    connector.setPort(port);
    connector.setHost(host);
    connector.setReuseAddress(true);

    FlumeBeanConfigurator.setConfigurationFields(connector, sourceContext);

    srv.addConnector(connector);

    try {
      ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
      context.setContextPath("/");
      srv.setHandler(context);

      context.addServlet(new ServletHolder(new FlumeHTTPServlet()),"/");
      context.setSecurityHandler(HTTPServerConstraintUtil.enforceConstraints());
      srv.start();
    } catch (Exception ex) {
      LOG.error("Error while starting HTTPSource. Exception follows.", ex);
      Throwables.propagate(ex);
    }
    Preconditions.checkArgument(srv.isRunning());
    sourceCounter.start();
    super.start();
  }

  @Override
  public void stop() {
    try {
      srv.stop();
      srv.join();
      srv = null;
    } catch (Exception ex) {
      LOG.error("Error while stopping HTTPSource. Exception follows.", ex);
    }
    sourceCounter.stop();
    LOG.info("Http source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  private class FlumeHTTPServlet extends HttpServlet {

    private static final long serialVersionUID = 4891924863218790344L;

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
      List<Event> events = Collections.emptyList(); //create empty list
      try {
        events = handler.getEvents(request);
      } catch (HTTPBadRequestException ex) {
        LOG.warn("Received bad request from client. ", ex);
        sourceCounter.incrementEventReadFail();
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                "Bad request from client. "
                + ex.getMessage());
        return;
      } catch (Exception ex) {
        LOG.warn("Deserializer threw unexpected exception. ", ex);
        sourceCounter.incrementEventReadFail();
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                "Deserializer threw unexpected exception. "
                + ex.getMessage());
        return;
      }
      sourceCounter.incrementAppendBatchReceivedCount();
      sourceCounter.addToEventReceivedCount(events.size());
      try {
        getChannelProcessor().processEventBatch(events);
      } catch (ChannelException ex) {
        LOG.warn("Error appending event to channel. "
                + "Channel might be full. Consider increasing the channel "
                + "capacity or make sure the sinks perform faster.", ex);
        sourceCounter.incrementChannelWriteFail();
        response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                "Error appending event to channel. Channel might be full."
                + ex.getMessage());
        return;
      } catch (Exception ex) {
        LOG.warn("Unexpected error appending event to channel. ", ex);
        sourceCounter.incrementGenericProcessingFail();
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                "Unexpected error while appending event to channel. "
                + ex.getMessage());
        return;
      }
      response.setCharacterEncoding(request.getCharacterEncoding());
      response.setStatus(HttpServletResponse.SC_OK);
      response.flushBuffer();
      sourceCounter.incrementAppendBatchAcceptedCount();
      sourceCounter.addToEventAcceptedCount(events.size());
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
      doPost(request, response);
    }
  }

  @Override
  protected void configureSsl(Context context) {
    handleDeprecatedParameter(context, "ssl", "enableSSL");
    handleDeprecatedParameter(context, "exclude-protocols", "excludeProtocols");
    handleDeprecatedParameter(context, "keystore-password", "keystorePassword");

    super.configureSsl(context);
  }

  private void handleDeprecatedParameter(Context context, String newParam, String oldParam) {
    if (!context.containsKey(newParam) && context.containsKey(oldParam)) {
      context.put(newParam, context.getString(oldParam));
    }
  }

}
