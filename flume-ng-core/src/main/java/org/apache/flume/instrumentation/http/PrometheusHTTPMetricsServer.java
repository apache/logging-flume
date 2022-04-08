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

import com.google.common.base.Throwables;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.exporter.MetricsServlet;
import org.apache.flume.instrumentation.MonitorService;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
public class PrometheusHTTPMetricsServer extends HTTPMetricsServer implements MonitorService {

  private static final String PROM_DEFAULT_PREFIX = "Flume_";
  private Server jettyServer;
  private static Logger LOG = LoggerFactory.getLogger(PrometheusHTTPMetricsServer.class);
  private static MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

  private FlumePrometheusCollector requests;

  @Override
  public void start() {

    requests = new FlumePrometheusCollector().register();

    jettyServer = new Server();
    //We can use Contexts etc if we have many urls to handle. For one url,
    //specifying a handler directly is the most efficient.
    HttpConfiguration httpConfiguration = new HttpConfiguration();
    ServerConnector connector = new ServerConnector(jettyServer,
        new HttpConnectionFactory(httpConfiguration));
    connector.setReuseAddress(true);
    connector.setPort(getPort());
    jettyServer.addConnector(connector);
    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    jettyServer.setHandler(context);
    context.addServlet(new ServletHolder(new MetricsServlet()),"/metrics");
    try {
      jettyServer.start();
      while (!jettyServer.isStarted()) {
        Thread.sleep(500);
      }
    } catch (Exception ex) {
      LOG.error("Error starting Jetty. JSON Metrics may not be available.", ex);
    }

  }

  class FlumePrometheusCollector extends Collector {

    public List<MetricFamilySamples> collect() {

      Map<Object, Map<String, MetricFamilySamples>> counterMetricMap = new HashMap<>();
      List<Collector.MetricFamilySamples> mfs = new ArrayList<>();

      Set<ObjectInstance> queryMBeans;
      try {
        queryMBeans = mbeanServer.queryMBeans(null, null);

        for (ObjectInstance obj : queryMBeans) {
          try {
            if (obj.getObjectName().toString().startsWith("org.apache.flume")) {
              processFlumeMetric(counterMetricMap, mfs, obj);
            } else if ((obj.getObjectName().toString().startsWith("kafka.consumer") ||
                        obj.getObjectName().toString().startsWith("kafka.producer"))
                      && obj.getObjectName().toString().contains("metrics")) {
              processKafkaMetric(counterMetricMap, mfs, obj);
            }



          } catch (Exception e) {
            LOG.error("Unable to poll JMX for metrics.", e);
          }

        }
        return mfs;

      } catch (Exception ex) {
        LOG.error("Could not get Mbeans for monitoring", ex);
        Throwables.propagate(ex);
        return null;
      }
    }

    private void processFlumeMetric(Map<Object, Map<String, MetricFamilySamples>> counterMetricMap,
                                    List<MetricFamilySamples> mfs, ObjectInstance obj)
        throws ClassNotFoundException, InstanceNotFoundException,
        IntrospectionException, ReflectionException {
      Class mbeanClass = Class.forName(obj.getClassName());
      Map<String, MetricFamilySamples> metricsMap;

      if (!counterMetricMap.containsKey(mbeanClass)) {
        metricsMap = new HashMap<>();

        for (Method method : mbeanClass.getMethods()) {
          String methodName = method.getName();
          if (methodName.startsWith("increment") && methodName.length() > "increment".length()) {
            String counterName = PROM_DEFAULT_PREFIX + methodName.substring("increment".length());
            createCounterIfNotExists(mfs, metricsMap, counterName);
          } else if (methodName.startsWith("addTo")) {
            String counterName = PROM_DEFAULT_PREFIX + methodName.substring("addTo".length());
            createCounterIfNotExists(mfs, metricsMap, counterName);
          } else if (methodName.startsWith("set")) {
            String counterName = PROM_DEFAULT_PREFIX + methodName.substring("set".length());
            createGaugeIfNotExists(mfs, metricsMap, counterName, Arrays.asList("component"));
          }
        }

        counterMetricMap.put(mbeanClass, metricsMap);

      } else {
        metricsMap = counterMetricMap.get(mbeanClass);
      }

      MBeanAttributeInfo[] attrs = mbeanServer.getMBeanInfo(obj.getObjectName()).getAttributes();
      String[] strAtts = new String[attrs.length];
      for (int i = 0; i < strAtts.length; i++) {
        strAtts[i] = attrs[i].getName();
      }
      AttributeList attrList = mbeanServer.getAttributes(obj.getObjectName(), strAtts);
      String component = obj.getObjectName().toString().substring(
              obj.getObjectName().toString().indexOf('=') + 1);

      for (Object attr : attrList) {
        Attribute localAttr = (Attribute) attr;
        if (!localAttr.getName().equalsIgnoreCase("type")) {
          MetricFamilySamples samples = metricsMap.get(PROM_DEFAULT_PREFIX + localAttr.getName());
          if (samples instanceof CounterMetricFamily) {
            ((CounterMetricFamily) samples).addMetric(Arrays.asList(component),
                    Double.valueOf(localAttr.getValue().toString()));
          } else if (samples instanceof GaugeMetricFamily) {
            ((GaugeMetricFamily) samples).addMetric(Arrays.asList(component),
                    Double.valueOf(localAttr.getValue().toString()));
          }
        }
      }
    }

    private void processKafkaMetric(Map<Object, Map<String, MetricFamilySamples>> counterMetricMap,
                                List<MetricFamilySamples> mfs, ObjectInstance obj)
        throws InstanceNotFoundException, IntrospectionException, ReflectionException {

      ObjectName objectName = obj.getObjectName();
      String qualifiedType =
          makeStringPromSafe(objectName.getDomain() + "_" +
              objectName.getKeyProperty("type"));


      TreeMap<String, String> properties = new TreeMap<>();
      for (String key : objectName.getKeyPropertyList().keySet()) {
        properties.put(makeStringPromSafe(key),
            objectName.getKeyPropertyList().get(key));
      }

      // We create a unique name for the metric based on the metric that came from Kafka, plus
      // all of the properties. Unfortunately Kafka does not have unique metric names and therefore
      // you can end up with metrics with differing property lists (which you can't have.
      String metricKey = qualifiedType + "_" + String.join("_",properties.keySet()) + "_";

      Map<String, MetricFamilySamples> metricsMap;

      // Get the attribute list now as we'll need it to create the gauge
      MBeanAttributeInfo[] attrs = mbeanServer.getMBeanInfo(obj.getObjectName()).getAttributes();
      String[] strAtts = new String[attrs.length];
      for (int i = 0; i < strAtts.length; i++) {
        strAtts[i] = attrs[i].getName();
      }

      // We pre-create each metric (once) before populating it once for each matching mbean
      if (!counterMetricMap.containsKey(metricKey)) {
        metricsMap = new HashMap<>();

        for (String attr : strAtts) {
          createGaugeIfNotExists(mfs, metricsMap, metricKey + "_" + makeStringPromSafe(attr),
              new ArrayList<>(properties.keySet()));
        }

        counterMetricMap.put(metricKey, metricsMap);

      } else {
        metricsMap = counterMetricMap.get(metricKey);
      }

      AttributeList attrList = mbeanServer.getAttributes(obj.getObjectName(), strAtts);

      for (Object attr : attrList) {
        Attribute localAttr = (Attribute) attr;

        try {

          GaugeMetricFamily samples =
              (GaugeMetricFamily) metricsMap.get(metricKey + "_" + makeStringPromSafe(localAttr.getName()));
          samples.addMetric(new ArrayList<>(properties.values()),
              Double.valueOf(localAttr.getValue().toString()));
        } catch (Exception e) {
          LOG.warn("Metric {} could not be monitored", metricKey, e);
        }
      }
    }

    //Prometeus is really unhappy with metrics with , or - in, so replace them
    private String makeStringPromSafe(String input) {
      return input.replaceAll("[.\\-]", "");
    }

    private void createCounterIfNotExists(List<MetricFamilySamples> mfs, Map<String,
            MetricFamilySamples> metricsMap, String counterName) {
      if (!metricsMap.containsKey(counterName)) {
        CounterMetricFamily labeledCounter = new CounterMetricFamily(counterName, counterName,
                Arrays.asList(
                        "component"));
        metricsMap.put(counterName, labeledCounter);
        mfs.add(labeledCounter);
      }
    }

    private void createGaugeIfNotExists(List<MetricFamilySamples> mfs, Map<String,
            MetricFamilySamples> metricsMap, String gaugeName, List<String> labelNames) {
      if (!metricsMap.containsKey(gaugeName)) {
        GaugeMetricFamily labelledGauge = new GaugeMetricFamily(gaugeName, gaugeName, labelNames);
        metricsMap.put(gaugeName, labelledGauge);
        mfs.add(labelledGauge);
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
