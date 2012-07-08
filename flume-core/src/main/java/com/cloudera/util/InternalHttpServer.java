/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.cloudera.util;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.BindException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * An embedded Jetty HTTP server.  It defers addition of contexts/handlers to 
 * a callback so that the we can provide a method that increments ports until 
 * a valid port is found.  This is mostly a thin wrapper around Jetty's 
 * {@link Server} class and behaves as Jetty does.
 *
 * Here is an example usage:
 * <pre>
 * InternalHttpServer server = new InternalHttpServer();
 * 
 * // applicationHome/webapps/ will be scanned for war files and directories.
 * server.setWebappDir(new File(applicationHome, &quot;webapps&quot;));
 * server.setPort(8080);
 * server.setBindAddress(&quot;0.0.0.0&quot;);
 * server.setContextCreator(new ContextCreator() {
 *   @Override
 *   public void addContexts(ContextHandlerCollection handlers) {
 *        handlers.addHandler(InternalHttpServer.createLogAppContext());
 *        handlers.addHandler(InternalHttpServer.createStackSevletContext(
 *            StackServlet.class, "/stacks", "/*", "stacks"));
 *        String webAppRoot = FlumeConfiguration.get().getNodeWebappRoot();
 *        InternalHttpServer.addHandlersFromPaths(handlers,
 *            new File(webAppRoot));
 *   }
 * });
 *
 * server.start();
 *
 * // at some later time...
 * server.stop();
 * </pre>
 */
public class InternalHttpServer {

  private static final Logger logger = LoggerFactory
      .getLogger(InternalHttpServer.class);

  private Server server;
  private int port;
  private int boundPort = -1;
  private String bindAddress;
  private ContextHandlerCollection handlers;
  private ContextCreator contextCreator = null;

  public InternalHttpServer() {
    port = 0;
    bindAddress = "0.0.0.0";
    handlers = new ContextHandlerCollection();
  }

  public void initialize() {
    if (server == null) {

      server = new Server();
      Connector connector = new SelectChannelConnector();

      connector.setPort(port);
      connector.setHost(bindAddress);

      server.addConnector(connector);

      if (contextCreator != null) {
        contextCreator.addContexts(handlers);
      }
      server.setHandler(handlers);
    }
  }

  /**
   * <p>
   * Start a configured HTTP server. Users should have already injected all the
   * necessary configuration parameters at this point. It is not considered safe
   * to call start() more than once and will lead to undefined behavior.
   * </p>
   * <p>
   * The configured webappDir is not scanned for applications until start() is
   * called.
   * </p>
   *
   * @throws BindException
   * @throws InternalHttpServerException
   */
  public void start() throws BindException {

    initialize();

    logger.info("Starting internal HTTP server");

    try {
      server.start();
      boundPort = server.getConnectors()[0].getLocalPort();
      logger.info("Server started on port " + boundPort);
    } catch (BindException be) {
      throw be;
    } catch (Exception e) {
      logger.warn("Caught exception during HTTP server start.", e);

      throw new InternalHttpServerException("Unable to start HTTP server", e);
    }
  }

  /**
   * Stop the running HTTP server. If {@link #start()} has never been called,
   * this is a no-op. This call blocks until the internal Jetty instance is
   * stopped (to the extent the Jetty {@link Server#stop()} call blocks).
   * 
   * @throws InternalHttpServerException
   */
  public void stop() {
    if (server == null) {
      return;
    }

    logger.info("Stopping internal HTTP server");

    try {
      server.stop();
    } catch (Exception e) {
      logger.warn("Caught exception during HTTP server stop.", e);

      throw new InternalHttpServerException("Unable to stop HTTP server", e);
    }
  }

  @Override
  public String toString() {
    return "{ bindAddress:" + bindAddress + " port:" + port + " boundPort:"
        + boundPort + " server:" + server + " }";
  }

  public Server getServer() {
    return server;
  }

  public void setServer(Server server) {
    this.server = server;
  }

  public int getPort() {
    return port;
  }

  public int getBoundPort() {
    return boundPort;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getBindAddress() {
    return bindAddress;
  }

  public void setBindAddress(String bindAddress) {
    this.bindAddress = bindAddress;
  }

  public static class InternalHttpServerException extends RuntimeException {

    private static final long serialVersionUID = -4936285404574873547L;

    public InternalHttpServerException() {
      super();
    }

    public InternalHttpServerException(String msg) {
      super(msg);
    }

    public InternalHttpServerException(String msg, Throwable t) {
      super(msg, t);
    }

    public InternalHttpServerException(Throwable t) {
      super(t);
    }

  }

  public void setHandlers(ContextHandlerCollection ctx) {
    if (ctx == null) {
      logger.warn("Attempting to add null webapp context");
      return;
    }
    handlers = ctx;
  }

  public ContextHandlerCollection getHandlers() {
    return handlers;
  }

  protected void addHandler(Context ctx) {
    if (ctx == null) {
      logger.warn("Attempting to add null webapp context");
      return;
    }
    handlers.addHandler(ctx);
  }

  public void setContextCreator(ContextCreator cc) {
    this.contextCreator = cc;
  }

  /**
   * The jetty server cannot properly reload contexts if it attempts to bind to
   * a port and fails. To support automatically going finding a new port, we
   * thus need to parameterize the creation and addition of context. This class
   * provides a call back that gets a instance of the server's
   * ContextHandlerCollection, and gives clients the opportunity to populate it.
   */
  public abstract static class ContextCreator {
    public abstract void addContexts(ContextHandlerCollection handlers);
  }

  public static WebAppContext createWarContext(File path) {
    logger.debug("checking {}", path);

    String name;
    if (path.isFile()) {
      // if not a war file reject
      int idx = path.getName().indexOf(".war");
      if (idx < 0) {
        return null;
      }

      // drop the .war suffix
      name = path.getName().substring(0, idx);
    } else {
      // is a dir
      name = path.getName();
    }

    // WebAppContext is for loading war files.
    logger.debug("creating context {} -> {}", name, path);
    WebAppContext handler = new WebAppContext(path.getPath(), "/" + name);
    handler.setParentLoaderPriority(true);
    return handler;
  }

  /**
   * This method adds support for both normal and exploded war file deployment.
   * <p>
   * This scannings the specified webapp directory for applications
   * Both traditional and exploded war formats are supported in the webapp
   * directory. In the case of exploded directories, the directory name is used
   * as the context. For war files, everything from the first instance of ".war"
   * to the end of the file name (inclusive) is stripped and the remainder is
   * used for the context name.
   * </p>
   * <p>
   * Name examples:
   * </p>
   * <table>
   * <tr>
   * <td>Name</td>
   * <td>Type</td>
   * <td>Context</td>
   * </tr>
   * <tr>
   * <td>app.war</td>
   * <td>file</td>
   * <td>app</td>
   * </tr>
   * <tr>
   * <td>app</td>
   * <td>dir</td>
   * <td>app</td>
   * </tr>
   * <tr>
   * <td>app.war</td>
   * <td>dir</td>
   * <td>app.war</td>
   * </tr>
   * <tr>
   * <td>app.war.war</td>
   * <td>file</td>
   * <td>app</td>
   * </tr>
   * <tr>
   * <td>
   * </table>
   * <p>
   * Example usage:
   * </p>
   */
  public static void addHandlersFromPaths(ContextHandlerCollection handlers,
      File webappDir) {
    logger.debug("Registering webapps in {}", webappDir);

    if (webappDir.isDirectory()) {
      for (File entry : webappDir.listFiles()) {
        Context ctx = createWarContext(entry);
        if (ctx != null) {
          handlers.addHandler(ctx);
        }
      }
    } else {
      Context ctx = createWarContext(webappDir);
      if (ctx != null) {
        handlers.addHandler(ctx);
      }
    }
  }

  /**
   * This creates file listing servlet context that is used to point to the log
   * directory of the daemon via the web interface.
   *
   * @return
   */
  public static Context createLogAppContext() {
    Context ctx = new Context();
    // logs applet
    String logDir = System.getProperty("flume.log.dir");
    if (logDir != null) {
      ctx.setContextPath("/logs");
      ctx.setResourceBase(logDir);
      ctx.addServlet(DefaultServlet.class, "/*");
      ctx.setDisplayName("logs");
    }
    return ctx;
  }

  /**
   * A very simple servlet to serve up a text representation of the current
   * stack traces. It both returns the stacks to the caller and logs them.
   * Currently the stack traces are done sequentially rather than exactly the
   * same data.
   */
  public static class StackServlet extends HttpServlet {
    private static final Log LOG = LogFactory.getLog(InternalHttpServer.class
        .getName());
    private static final long serialVersionUID = -6284183679759467039L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {

      OutputStream outStream = response.getOutputStream();
      ReflectionUtils.printThreadInfo(new PrintWriter(outStream), "");
      outStream.close();
      ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);
    }
  }

  /**
   * This creates a stack dumping servlet that can be used to debug a running
   * daemon via the web interface.
   *
   * @param sltClz
   * @param contextPath
   * @param pathSpec
   * @param name
   * @return
   */
  public static Context createStackSevletContext() {
    Context ctx = new Context();
    ServletHolder holder = new ServletHolder(StackServlet.class);
    ctx.setContextPath("/stacks");
    ctx.addServlet(holder, "/*");
    ctx.setDisplayName("stacks");

    return ctx;
  }

  /**
   * If successful returns the port the http server successfully bound to. If it
   * failed, returns -1
   */
  public static InternalHttpServer startFindPortHttpServer(ContextCreator cc,
      String bindAddress, int nodePort) {
    do {
      try {
        return startHttpServer(cc, bindAddress, nodePort);
      } catch (BindException be) {
        logger.error("Unable to start webserver on " + bindAddress + ":"
            + nodePort + ". Trying next port...");
        nodePort++;
      }
    } while (true);
  }

  /**
   * Single attempt to create an http server for the node.
   * 
   * @param bindAddress
   * @param nodePort
   * @return instance of a started http server or null if failed.
   * @throws BindException
   */
  public static InternalHttpServer startHttpServer(ContextCreator cc,
      String bindAddress, int nodePort) throws BindException {
    InternalHttpServer http = null;
    try {
      http = new InternalHttpServer();
      http.setBindAddress(bindAddress);
      http.setPort(nodePort);
      http.setContextCreator(cc);
      http.start();
      return http;
    } catch (BindException be) {
      http.stop();
      http = null;
      throw be;
    } catch (Throwable t) {
      logger.error("Unexpected exception/error thrown! " + t.getMessage(), t);
      // if any exception happens bail out and cleanup.
      http.stop();
      return null;
    }
  }
}
