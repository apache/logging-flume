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
package org.apache.flume.instrumentation.http;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Monitor service implementation that runs a web server on a configurable
 * port and returns the metrics for components in JSON format. <p> Optional
 * parameters: <p> <tt>port</tt> : The port on which the server should listen
 * to.<p> Returns metrics in the following format: <p>
 *
 * {<p> "componentName1":{"metric1" : "metricValue1","metric2":"metricValue2"}
 * <p> "componentName1":{"metric3" : "metricValue3","metric4":"metricValue4"}
 * <p> }
 */
public class HTTPMetricsServer implements MonitorService {

  private Server jettyServer;
  private int port;
  private static Logger LOG = LoggerFactory.getLogger(HTTPMetricsServer.class);
  public static int DEFAULT_PORT = 41414;
  public static String CONFIG_PORT = "port";

  @Override
  public void start() {
    jettyServer = new Server();
    //We can use Contexts etc if we have many urls to handle. For one url,
    //specifying a handler directly is the most efficient.
    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setReuseAddress(true);
    connector.setPort(port);
    jettyServer.setConnectors(new Connector[] {connector});
    jettyServer.setHandler(new HTTPMetricsHandler());
    try {
      jettyServer.start();
      while (!jettyServer.isStarted()) {
        Thread.sleep(500);
      }
    } catch (Exception ex) {
      LOG.error("Error starting Jetty. JSON Metrics may not be available.", ex);
    }

  }

  @Override
  public void stop() {
    try {
      jettyServer.stop();
      jettyServer.join();
    } catch (Exception ex) {
      LOG.error("Error stopping Jetty. JSON Metrics may not be available.", ex);
    }

  }

  @Override
  public void configure(Context context) {
    port = context.getInteger(CONFIG_PORT, DEFAULT_PORT);
  }

  private class HTTPMetricsHandler extends AbstractHandler {

    Type mapType =
            new TypeToken<Map<String, Map<String, String>>>() {
            }.getType();
    Gson gson = new Gson();

    @Override
    public void handle(String target,
            HttpServletRequest request,
            HttpServletResponse response,
            int dispatch) throws IOException, ServletException {
      // /metrics is the only place to pull metrics.
      //If we want to use any other url for something else, we should make sure
      //that for metrics only /metrics is used to prevent backward
      //compatibility issues.
      if(request.getMethod().equalsIgnoreCase("TRACE") || request.getMethod()
        .equalsIgnoreCase("OPTIONS")) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN);
        response.flushBuffer();
        ((Request) request).setHandled(true);
        return;
      }
      if (target.equals("/")) {
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().write("For Flume metrics please click"
                + " <a href = \"./metrics\"> here</a>.");
        response.flushBuffer();
        ((Request) request).setHandled(true);
        return;
      } else if (target.equalsIgnoreCase("/metrics")) {
        response.setContentType("application/json;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
        String json = gson.toJson(metricsMap, mapType);
        response.getWriter().write(json);
        response.flushBuffer();
        ((Request) request).setHandled(true);
        return;
      }
      response.sendError(HttpServletResponse.SC_NOT_FOUND);
      response.flushBuffer();
      //Not handling the request returns a Not found error page.
    }
  }
}
