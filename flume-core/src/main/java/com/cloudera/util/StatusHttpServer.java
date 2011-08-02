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
package com.cloudera.util;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.InetSocketAddress;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;

import com.google.common.base.Preconditions;

// jon: This is a shamelessly hacked version of the Http status server from the jobtracker,
// simplified for my needs. Originally from apache licensed hadoop 0.18.3, o.a.h.mapred.StatusHttpServer

/**
 * Create a Jetty embedded server to answer http requests. The primary goal is
 * to serve up status information for the server. There are three contexts:
 * "/logs/" -> points to the log directory "/static/" -> points to common static
 * files (src/webapps/static) "/" -> the jsp server code from
 * (src/webapps/<name>)
 */
public class StatusHttpServer {
  private Server webServer;
  private SelectChannelConnector channelConnector;
  private SslSocketConnector sslConnector;
  private boolean findPort;
  private WebAppContext webAppContext;
  private static final Log LOG = LogFactory.getLog(StatusHttpServer.class
      .getName());

  /**
   * Create a status server on the given port. The jsp scripts are taken from
   * src/webapps/<name>.
   * 
   * @param name
   *          The name of the server
   * @param port
   *          The port to use on the server
   * @param findPort
   *          whether the server should start at the given port and increment by
   *          1 until it finds a free port.
   */
  public StatusHttpServer(String name, String webAppsPath, String bindAddress,
      int port, boolean findPort) throws IOException {
    webServer = new org.mortbay.jetty.Server();
    this.findPort = findPort;
    channelConnector = new SelectChannelConnector();
    channelConnector.setPort(port);
    channelConnector.setHost(bindAddress);
    webServer.addConnector(channelConnector);

    String appDir = webAppsPath;
    // set up the context for "/" jsp files
    String webapp = new File(appDir, name).getAbsolutePath();
    LOG.info("starting web app in directory: " + webapp);
    webAppContext = new WebAppContext(webapp, "/");
    webServer.setHandler(webAppContext);
    addServlet("stacks", "/stacks", StackServlet.class);
  }

  /**
   * Sets a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * 
   * @param name
   *          The name of the attribute
   * @param value
   *          The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    webAppContext.setAttribute(name, value);
  }

  /**
   * Add a servlet in the server.
   * 
   * @param name
   *          The name of the servlet (can be passed as null)
   * @param pathSpec
   *          The path spec for the servlet
   * @param servletClass
   *          The servlet class
   */
  public <T extends HttpServlet> void addServlet(String name, String pathSpec,
      Class<T> servletClass) {

    WebAppContext context = webAppContext;
    if (name == null) {
      context.addServlet(pathSpec, servletClass.getName());
    } else {
      context.addServlet(servletClass, pathSpec);
    }
  }

  public void addServlet(Servlet servlet, String pathSpec) {
    webAppContext.addServlet(new ServletHolder(servlet), pathSpec);
  }

  /**
   * Get the value in the webapp context.
   * 
   * @param name
   *          The name of the attribute
   * @return The value of the attribute
   */
  public Object getAttribute(String name) {
    return webAppContext.getAttribute(name);
  }

  /**
   * Get the port that the server is on
   * 
   * @return the port
   */
  public int getPort() {
    return channelConnector.getPort();
  }

  /**
   * Configure an ssl listener on the server.
   * 
   * @param addr
   *          address to listen on
   * @param keystore
   *          location of the keystore
   * @param storPass
   *          password for the keystore
   * @param keyPass
   *          password for the key
   */
  public void addSslListener(InetSocketAddress addr, String keystore,
      String storPass, String keyPass) throws IOException {
    if (sslConnector != null || webServer.isStarted()) {
      throw new IOException("Failed to add ssl listener");
    }
    sslConnector = new SslSocketConnector();
    sslConnector.setHost(addr.getHostName());
    sslConnector.setPort(addr.getPort());
    sslConnector.setKeystore(keystore);
    sslConnector.setPassword(storPass);
    sslConnector.setKeyPassword(keyPass);
    webServer.addConnector(sslConnector);
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      while (true) {
        try {
          webServer.start();
          break;
        } catch (BindException ex) {
          // if the multi exception contains ONLY a bind exception,
          // then try the next port number.
          if (!findPort) {
            throw ex;
          }
          // pick another port
          webServer.stop();
          channelConnector.setPort(channelConnector.getPort() + 1);
        }
      }
    } catch (Exception e) {
      IOException ie = new IOException("Problem starting http server");
      ie.initCause(e);
      throw ie;
    }
  }

  /**
   * stop the server
   */
  public void stop() throws Exception {
    webServer.stop();
  }

  /**
   * A very simple servlet to serve up a text representation of the current
   * stack traces. It both returns the stacks to the caller and logs them.
   * Currently the stack traces are done sequentially rather than exactly the
   * same data.
   */
  public static class StackServlet extends HttpServlet {

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
   * Test harness to get precompiled jsps working.
   * 
   * @param argv
   */
  public static void main(String[] argv) {
    Preconditions.checkArgument(argv.length == 3);
    String name = argv[0];
    String path = argv[1];
    int port = Integer.parseInt(argv[2]);

    try {
      StatusHttpServer http = new StatusHttpServer(name, path, "0.0.0.0", port,
          false);
      http.start();
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
}
