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

import com.google.common.base.Throwables;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.exporter.servlet.jakarta.PrometheusMetricsServlet;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.flume.instrumentation.MonitorService;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Monitor service implementation that runs a web server on a configurable
 * port and returns the metrics for components in Prometheus format. <p> Optional
 * parameters: <p> <tt>port</tt> : The port on which the server should listen
 * to.<p> Returns metrics in Prometheus text format via /metrics endpoint
 */
public class PrometheusHTTPMetricsServer extends HTTPMetricsServer implements MonitorService {

    private static final String PROM_DEFAULT_PREFIX = "Flume_";
    private Server jettyServer;
    private static Logger LOG = LoggerFactory.getLogger(PrometheusHTTPMetricsServer.class);
    private static MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    private FlumePrometheusCollector metricsCollector;

    @Override
    public void start() {

        metricsCollector = new FlumePrometheusCollector();
        metricsCollector.register();

        jettyServer = new Server();
        // We can use Contexts etc if we have many urls to handle. For one url,
        // specifying a handler directly is the most efficient.
        HttpConfiguration httpConfiguration = new HttpConfiguration();
        ServerConnector connector = new ServerConnector(jettyServer, new HttpConnectionFactory(httpConfiguration));
        connector.setReuseAddress(true);
        connector.setPort(getPort());
        jettyServer.addConnector(connector);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        jettyServer.setHandler(context);
        context.addServlet(new ServletHolder(new PrometheusMetricsServlet()), "/metrics");
        try {
            jettyServer.start();
            while (!jettyServer.isStarted()) {
                Thread.sleep(500);
            }
        } catch (Exception ex) {
            LOG.error("Error starting Jetty. Prometheus Metrics may not be available.", ex);
        }
    }

    class FlumePrometheusCollector {
        private final Map<String, Counter> counters = new HashMap<>();
        private final Map<String, Gauge> gauges = new HashMap<>();
        private final PrometheusRegistry registry = PrometheusRegistry.defaultRegistry;

        public void register() {
            collectMetrics();
        }

        private void collectMetrics() {
            Set<ObjectInstance> queryMBeans;
            try {
                queryMBeans = mbeanServer.queryMBeans(null, null);

                for (ObjectInstance obj : queryMBeans) {
                    try {
                        if (obj.getObjectName().toString().startsWith("org.apache.flume")) {
                            processFlumeMetric(obj);
                        } else if ((obj.getObjectName().toString().startsWith("kafka.consumer")
                                        || obj.getObjectName().toString().startsWith("kafka.producer"))
                                && obj.getObjectName().toString().contains("metrics")) {
                            processKafkaMetric(obj);
                        }

                    } catch (Exception e) {
                        LOG.error("Unable to poll JMX for metrics.", e);
                    }
                }

            } catch (Exception ex) {
                LOG.error("Could not get Mbeans for monitoring", ex);
                Throwables.propagate(ex);
            }
        }

        private void processFlumeMetric(ObjectInstance obj)
                throws ClassNotFoundException, InstanceNotFoundException, IntrospectionException, ReflectionException {
            Class<?> mbeanClass = Class.forName(obj.getClassName());

            // First pass: create counters and gauges based on method names
            for (Method method : mbeanClass.getMethods()) {
                String methodName = method.getName();
                if (methodName.startsWith("increment") && methodName.length() > "increment".length()) {
                    String counterName = PROM_DEFAULT_PREFIX + methodName.substring("increment".length());
                    createCounterIfNotExists(counterName);
                } else if (methodName.startsWith("addTo")) {
                    String counterName = PROM_DEFAULT_PREFIX + methodName.substring("addTo".length());
                    createCounterIfNotExists(counterName);
                } else if (methodName.startsWith("set")) {
                    String gaugeName = PROM_DEFAULT_PREFIX + methodName.substring("set".length());
                    createGaugeIfNotExists(gaugeName);
                }
            }

            // Second pass: get attribute values and update metrics
            MBeanAttributeInfo[] attrs =
                    mbeanServer.getMBeanInfo(obj.getObjectName()).getAttributes();
            String[] strAtts = new String[attrs.length];
            for (int i = 0; i < strAtts.length; i++) {
                strAtts[i] = attrs[i].getName();
            }
            AttributeList attrList = mbeanServer.getAttributes(obj.getObjectName(), strAtts);
            String component = obj.getObjectName()
                    .toString()
                    .substring(obj.getObjectName().toString().indexOf('=') + 1);

            for (Object attr : attrList) {
                Attribute localAttr = (Attribute) attr;
                if (!localAttr.getName().equalsIgnoreCase("type")) {
                    String metricName = PROM_DEFAULT_PREFIX + localAttr.getName();
                    double value = Double.parseDouble(localAttr.getValue().toString());

                    Counter counter = counters.get(metricName);
                    if (counter != null) {
                        // For counters, we label by component
                        counter.labelValues(component).inc(value);
                    }

                    Gauge gauge = gauges.get(metricName);
                    if (gauge != null) {
                        // For gauges, we label by component
                        gauge.labelValues(component).set(value);
                    }
                }
            }
        }

        private void processKafkaMetric(ObjectInstance obj)
                throws InstanceNotFoundException, IntrospectionException, ReflectionException {

            ObjectName objectName = obj.getObjectName();
            String qualifiedType = makeStringPromSafe(objectName.getDomain() + "_" + objectName.getKeyProperty("type"));

            TreeMap<String, String> properties = new TreeMap<>();
            for (String key : objectName.getKeyPropertyList().keySet()) {
                properties.put(
                        makeStringPromSafe(key), objectName.getKeyPropertyList().get(key));
            }

            String metricKey = qualifiedType + "_" + String.join("_", properties.keySet()) + "_";

            // Get the attribute list now as we'll need it to create gauges
            MBeanAttributeInfo[] attrs =
                    mbeanServer.getMBeanInfo(obj.getObjectName()).getAttributes();
            String[] strAtts = new String[attrs.length];
            for (int i = 0; i < strAtts.length; i++) {
                strAtts[i] = attrs[i].getName();
            }

            // Pre-create each metric (once) before populating it
            for (String attr : strAtts) {
                String gaugeName = metricKey + "_" + makeStringPromSafe(attr);
                createGaugeIfNotExists(gaugeName);
            }

            AttributeList attrList = mbeanServer.getAttributes(obj.getObjectName(), strAtts);

            for (Object attr : attrList) {
                Attribute localAttr = (Attribute) attr;

                try {
                    String gaugeName = metricKey + "_" + makeStringPromSafe(localAttr.getName());
                    Gauge gauge = gauges.get(gaugeName);
                    if (gauge != null) {
                        double value = Double.parseDouble(localAttr.getValue().toString());
                        gauge.labelValues(new ArrayList<>(properties.values()).toArray(new String[0]))
                                .set(value);
                    }
                } catch (Exception e) {
                    LOG.warn("Metric {} could not be monitored", metricKey, e);
                }
            }
        }

        // Prometheus is really unhappy with metrics with , or - in, so replace them
        private String makeStringPromSafe(String input) {
            return input.replaceAll("[.\\-]", "");
        }

        private void createCounterIfNotExists(String counterName) {
            if (!counters.containsKey(counterName)) {
                Counter counter = Counter.builder()
                        .name(counterName)
                        .help(counterName)
                        .labelNames("component")
                        .register();
                counters.put(counterName, counter);
            }
        }

        private void createGaugeIfNotExists(String gaugeName) {
            if (!gauges.containsKey(gaugeName)) {
                Gauge gauge = Gauge.builder()
                        .name(gaugeName)
                        .help(gaugeName)
                        .labelNames("component")
                        .register();
                gauges.put(gaugeName, gauge);
            }
        }
    }

    @Override
    public void stop() {
        try {
            jettyServer.stop();
            jettyServer.join();
        } catch (Exception ex) {
            LOG.error("Error stopping Jetty. Prometheus Metrics may not be available.", ex);
        }
    }
}
