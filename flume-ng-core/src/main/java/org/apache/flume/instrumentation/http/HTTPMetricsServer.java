/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.instrumentation.http;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;
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
    private static int DEFAULT_PORT = 41414;
    public static String CONFIG_PORT = "port";

    @Override
    public void start() {
        jettyServer = new Server();
        // We can use Contexts etc if we have many urls to handle. For one url,
        // specifying a handler directly is the most efficient.
        HttpConfiguration httpConfiguration = new HttpConfiguration();
        ServerConnector connector = new ServerConnector(jettyServer, new HttpConnectionFactory(httpConfiguration));
        connector.setReuseAddress(true);
        connector.setPort(port);
        jettyServer.addConnector(connector);
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

    public int getPort() {
        return port;
    }

    @Override
    public void configure(Context context) {
        port = context.getInteger(CONFIG_PORT, DEFAULT_PORT);
    }

    private class HTTPMetricsHandler extends Handler.Abstract {

        java.lang.reflect.Type mapType = new TypeToken<Map<String, Map<String, String>>>() {}.getType();
        Gson gson = new Gson();

        @Override
        public boolean handle(Request request, Response response, Callback callback) throws Exception {
            String method = request.getMethod();
            String path = Request.getPathInContext(request);

            if (method.equalsIgnoreCase("TRACE") || method.equalsIgnoreCase("OPTIONS")) {
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                callback.succeeded();
                return true;
            }

            if ("/".equals(path)) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.getHeaders().put("Content-Type", "text/html;charset=utf-8");
                String html = "For Flume metrics please click <a href=\"./metrics\"> here</a>.";
                response.write(true, ByteBuffer.wrap(html.getBytes(StandardCharsets.UTF_8)), callback);
                return true;
            }

            if ("/metrics".equalsIgnoreCase(path)) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.getHeaders().put("Content-Type", "application/json;charset=utf-8");

                Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
                String json = gson.toJson(metricsMap, mapType);

                response.write(true, ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8)), callback);
                return true;
            }

            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            callback.succeeded();
            return true;
        }
    }
}
