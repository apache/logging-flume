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
package org.apache.flume.instrumentation;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.api.HostInfo;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Ganglia server that polls JMX based at a configured frequency (defaults to
 * once every 60 seconds). This implementation can send data to ganglia 3 and
 * ganglia 3.1. <p>
 *
 * <b>Mandatory Parameters:</b><p> <tt>hosts: </tt> List of comma separated
 * hostname:ports of ganglia servers to report metrics to. <p> <b>Optional
 * Parameters: </b><p> <tt>pollFrequency:</tt>Interval in seconds between
 * consecutive reports to ganglia servers. Default = 60 seconds.<p>
 * <tt>isGanglia3:</tt> Report to ganglia 3 ? Default = false - reports to
 * ganglia 3.1.
 *
 *
 *
 */
public class GangliaServer implements MonitorService {
  /*
   * The Ganglia protocol specific stuff: the xdr_* methods
   * and the sendToGanglia* methods have been shamelessly ripped off
   * from Hadoop. All hail the yellow elephant!
   */

  private static final Logger logger =
          LoggerFactory.getLogger(GangliaServer.class);
  public static final int BUFFER_SIZE = 1500; // as per libgmond.c
  protected byte[] buffer = new byte[BUFFER_SIZE];
  protected int offset;
  private final List<SocketAddress> addresses = new ArrayList<SocketAddress>();
  private DatagramSocket socket = null;
  private ScheduledExecutorService service =
          Executors.newSingleThreadScheduledExecutor();
  private List<HostInfo> hosts;
  protected final GangliaCollector collectorRunnable;
  private int pollFrequency = 60;
  public static final String DEFAULT_UNITS = "";
  public static final int DEFAULT_TMAX = 60;
  public static final int DEFAULT_DMAX = 0;
  public static final int DEFAULT_SLOPE = 3;
  public static final String GANGLIA_DOUBLE_TYPE = "double";
  private volatile boolean isGanglia3 = false;
  private String hostname;
  public final String CONF_POLL_FREQUENCY = "pollFrequency";
  public final int DEFAULT_POLL_FREQUENCY = 60;
  public final String CONF_HOSTS = "hosts";
  public final String CONF_ISGANGLIA3 = "isGanglia3";
  private static final String GANGLIA_CONTEXT = "flume.";

  public GangliaServer() throws FlumeException {
    collectorRunnable = new GangliaCollector();
  }

  /**
   * Puts a string into the buffer by first writing the size of the string as an
   * int, followed by the bytes of the string, padded if necessary to a multiple
   * of 4.
   *
   * @param s the string to be written to buffer at offset location
   */
  protected void xdr_string(String s) {
    byte[] bytes = s.getBytes();
    int len = bytes.length;
    xdr_int(len);
    System.arraycopy(bytes, 0, buffer, offset, len);
    offset += len;
    pad();
  }

  /**
   * Pads the buffer with zero bytes up to the nearest multiple of 4.
   */
  private void pad() {
    int newOffset = ((offset + 3) / 4) * 4;
    while (offset < newOffset) {
      buffer[offset++] = 0;
    }
  }

  /**
   * Puts an integer into the buffer as 4 bytes, big-endian.
   */
  protected void xdr_int(int i) {
    buffer[offset++] = (byte) ((i >> 24) & 0xff);
    buffer[offset++] = (byte) ((i >> 16) & 0xff);
    buffer[offset++] = (byte) ((i >> 8) & 0xff);
    buffer[offset++] = (byte) (i & 0xff);
  }

  public synchronized void sendToGangliaNodes() {
    DatagramPacket packet;
    for (SocketAddress addr : addresses) {
      try {
        packet = new DatagramPacket(buffer, offset, addr);
        socket.send(packet);
      } catch (Exception ex) {
        logger.warn("Could not send metrics to metrics server: "
                + addr.toString(), ex);
      }
    }
    offset = 0;
  }

  /**
   * Start this server, causing it to poll JMX at the configured frequency.
   */
  @Override
  public void start() {
    try {
      socket = new DatagramSocket();
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (SocketException ex) {
      logger.error("Could not create socket for metrics collection.");
      throw new FlumeException(
              "Could not create socket for metrics collection.", ex);
    } catch (Exception ex2) {
      logger.warn("Unknown error occured", ex2);
    }
    for (HostInfo host : hosts) {
      addresses.add(new InetSocketAddress(
              host.getHostName(), host.getPortNumber()));
    }
    collectorRunnable.server = this;
    if (service.isShutdown() || service.isTerminated()) {
      service = Executors.newSingleThreadScheduledExecutor();
    }
    service.scheduleWithFixedDelay(collectorRunnable, 0,
            pollFrequency, TimeUnit.SECONDS);
  }

  /**
   * Stop this server.
   */
  @Override
  public void stop() {
    service.shutdown();

    while (!service.isTerminated()) {
      try {
        logger.warn("Waiting for ganglia service to stop");
        service.awaitTermination(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ex) {
        logger.warn("Interrupted while waiting"
                + " for ganglia monitor to shutdown", ex);
        service.shutdownNow();
      }
    }
    addresses.clear();
  }

  /**
   *
   * @param pollFrequency Seconds between consecutive JMX polls.
   */
  public void setPollFrequency(int pollFrequency) {
    this.pollFrequency = pollFrequency;
  }

  /**
   *
   * @return Seconds between consecutive JMX polls
   */
  public int getPollFrequency() {
    return pollFrequency;
  }

  /**
   *
   * @param isGanglia3 When true, ganglia 3 messages will be sent, else Ganglia
   * 3.1 formatted messages are sent.
   */
  public void setIsGanglia3(boolean isGanglia3) {
    this.isGanglia3 = isGanglia3;
  }

  /**
   *
   * @return True if the server is currently sending ganglia 3 formatted msgs.
   * False if the server returns Ganglia 3.1
   */
  public boolean isGanglia3() {
    return this.isGanglia3;
  }

  protected void createGangliaMessage(String name, String value) {
    logger.debug("Sending ganglia3 formatted message."
            + name + ": " + value);
    name = hostname + "." + name;
    xdr_int(0);
    String type = "string";
    try {
      Float.parseFloat(value);
      type = "float";
    } catch (NumberFormatException ex) {
      // The param is a string, and so leave the type as is.
    }
    xdr_string(type); // metric type
    xdr_string(name);
    xdr_string(value);
    xdr_string(DEFAULT_UNITS);
    xdr_int(DEFAULT_SLOPE);
    xdr_int(DEFAULT_TMAX);
    xdr_int(DEFAULT_DMAX);
  }

  protected void createGangliaMessage31(String name, String value) {
    logger.debug("Sending ganglia 3.1 formatted message: "
            + name + ": " + value);
    xdr_int(128); // metric_id = metadata_msg
    xdr_string(hostname); // hostname
    xdr_string(name); // metric name
    xdr_int(0); // spoof = False
    String type = "string";
    try {
      Float.parseFloat(value);
      type = "float";
    } catch (NumberFormatException ex) {
      // The param is a string, and so leave the type as is.
    }
    xdr_string(type); // metric type
    xdr_string(name); // metric name
    xdr_string(DEFAULT_UNITS); // units
    xdr_int(DEFAULT_SLOPE); // slope
    xdr_int(DEFAULT_TMAX); // tmax, the maximum time between metrics
    xdr_int(DEFAULT_DMAX); // dmax, the maximum data value
    xdr_int(1); /*Num of the entries in extra_value field for Ganglia 3.1.x*/
    xdr_string("GROUP"); /*Group attribute*/
    xdr_string("flume"); /*Group value*/

    this.sendToGangliaNodes();

    // Now we send out a message with the actual value.
    // Technically, we only need to send out the metadata message once for
    // each metric, but I don't want to have to record which metrics we did and
    // did not send.
    xdr_int(133); // we are sending a string value
    xdr_string(hostname); // hostName
    xdr_string(name); // metric name
    xdr_int(0); // spoof = False
    xdr_string("%s"); // format field
    xdr_string(value); // metric value
  }

  @Override
  public void configure(Context context) {
    this.pollFrequency = context.getInteger(this.CONF_POLL_FREQUENCY, 60);
    String localHosts = context.getString(this.CONF_HOSTS);
    if (localHosts == null || localHosts.isEmpty()) {
      throw new ConfigurationException("Hosts list cannot be empty.");
    }
    this.hosts = this.getHostsFromString(localHosts);
    this.isGanglia3 = context.getBoolean(this.CONF_ISGANGLIA3, false);
  }

  private List<HostInfo> getHostsFromString(String hosts)
          throws FlumeException {
    List<HostInfo> hostInfoList = new ArrayList<HostInfo>();
    String[] hostsAndPorts = hosts.split(",");
    int i = 0;
    for (String host : hostsAndPorts) {
      String[] hostAndPort = host.split(":");
      if (hostAndPort.length < 2) {
        logger.warn("Invalid ganglia host: ", host);
        continue;
      }
      try {
        hostInfoList.add(new HostInfo("ganglia_host-" + String.valueOf(i),
                hostAndPort[0], Integer.parseInt(hostAndPort[1])));
      } catch (Exception e) {
        logger.warn("Invalid ganglia host: " + host, e);
        continue;
      }
    }
    if (hostInfoList.isEmpty()) {
      throw new FlumeException("No valid ganglia hosts defined!");
    }
    return hostInfoList;
  }

  /**
   * Worker which polls JMX for all mbeans with
   * {@link javax.management.ObjectName} within the flume namespace:
   * org.apache.flume. All attributes of such beans are sent to the all hosts
   * specified by the server that owns it's instance.
   *
   */
  protected class GangliaCollector implements Runnable {

    private GangliaServer server;

    @Override
    public void run() {
      try {
        Map<String, Map<String, String>> metricsMap =
                JMXPollUtil.getAllMBeans();
        for (String component : metricsMap.keySet()) {
          Map<String, String> attributeMap = metricsMap.get(component);
          for (String attribute : attributeMap.keySet()) {
            if (isGanglia3) {
              server.createGangliaMessage(GANGLIA_CONTEXT + component + "."
                      + attribute,
                      attributeMap.get(attribute));
            } else {
              server.createGangliaMessage31(GANGLIA_CONTEXT + component + "."
                      + attribute,
                      attributeMap.get(attribute));
            }
            server.sendToGangliaNodes();
          }
        }
      } catch (Throwable t) {
        logger.error("Unexpected error", t);
      }
    }
  }
}
