/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.flume.node;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.node.net.AuthorizationProvider;
import org.apache.flume.node.net.BasicAuthorizationProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that files can be loaded via http.
 */
public class TestHttpConfigurationSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestHttpConfigurationSource.class);
  private static final String BASIC = "Basic ";
  private static final String expectedCreds = "flume:flume";
  private static Server server;
  private static Base64.Decoder decoder = Base64.getDecoder();
  private static int port;

  @BeforeClass
  public static void startServer() throws Exception {
    try {
      server = new Server(0);
      ServletContextHandler context = new ServletContextHandler();
      ServletHolder defaultServ = new ServletHolder("default", TestServlet.class);
      defaultServ.setInitParameter("resourceBase", System.getProperty("user.dir"));
      defaultServ.setInitParameter("dirAllowed", "true");
      context.addServlet(defaultServ, "/");
      server.setHandler(context);

      // Start Server
      server.start();
      port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    } catch (Throwable ex) {
      ex.printStackTrace();
      throw ex;
    }
  }

  @AfterClass
  public static void stopServer() throws Exception {
    server.stop();
  }


  @Test(expected = ConfigurationException.class)
  public void testBadCrdentials() throws Exception {
    URI confFile = new URI("http://localhost:" + port + "/flume-conf.properties");
    AuthorizationProvider authProvider = new BasicAuthorizationProvider("foo", "bar");
    ConfigurationSource source = new HttpConfigurationSource(confFile, authProvider, true);
  }

  @Test
  public void testGet() throws Exception {
    URI confFile = new URI("http://localhost:" + port + "/flume-conf.properties");
    AuthorizationProvider authProvider = new BasicAuthorizationProvider("flume", "flume");
    ConfigurationSource source = new HttpConfigurationSource(confFile, authProvider, true);
    Assert.assertNotNull("No configuration returned", source);
    InputStream is = source.getInputStream();
    Assert.assertNotNull("No data returned", is);
    Properties props = new Properties();
    props.load(is);
    String value = props.getProperty("host1.sources");
    Assert.assertNotNull("Missing key", value);
    Assert.assertFalse(source.isModified());
    File file = new File("target/test-classes/flume-conf.properties");
    if (file.setLastModified(System.currentTimeMillis())) {
      Assert.assertTrue(source.isModified());
    }
  }

  public static class TestServlet extends DefaultServlet {

    private static final long serialVersionUID = -2885158530511450659L;

    @Override
    protected void doGet(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
      Enumeration<String> headers = request.getHeaders(HttpHeader.AUTHORIZATION.toString());
      if (headers == null) {
        response.sendError(401, "No Auth header");
        return;
      }
      while (headers.hasMoreElements()) {
        String authData = headers.nextElement();
        Assert.assertTrue("Not a Basic auth header", authData.startsWith(BASIC));
        String credentials = new String(decoder.decode(authData.substring(BASIC.length())));
        if (!expectedCreds.equals(credentials)) {
          response.sendError(401, "Invalid credentials");
          return;
        }
      }
      if (request.getServletPath().equals("/flume-conf.properties")) {
        File file = new File("target/test-classes/flume-conf.properties");
        long modifiedSince = request.getDateHeader(HttpHeader.IF_MODIFIED_SINCE.toString());
        long lastModified = (file.lastModified() / 1000) * 1000;
        LOGGER.debug("LastModified: {}, modifiedSince: {}", lastModified, modifiedSince);
        if (modifiedSince > 0 && lastModified <= modifiedSince) {
          response.setStatus(304);
          return;
        }
        response.setDateHeader(HttpHeader.LAST_MODIFIED.toString(), lastModified);
        response.setContentLengthLong(file.length());
        Files.copy(file.toPath(), response.getOutputStream());
        response.getOutputStream().flush();
        response.setStatus(200);
      } else {
        response.sendError(400, "Unsupported request");
      }
    }
  }
}
