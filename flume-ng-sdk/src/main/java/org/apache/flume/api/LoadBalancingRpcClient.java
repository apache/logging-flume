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
package org.apache.flume.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.util.SpecificOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>An implementation of RpcClient interface that uses NettyAvroRpcClient
 * instances to load-balance the requests over many different hosts. This
 * implementation supports a round-robin scheme or random scheme of doing
 * load balancing over the various hosts. To specify round-robin scheme set
 * the value of the configuration property <tt>load-balance-type</tt> to
 * <tt>round_robin</tt>. Similarly, for random scheme this value should be
 * set to <tt>random</tt>, and for a custom scheme the full class name of
 * the class that implements the <tt>HostSelector</tt> interface.
 * </p>
 * <p>
 * This implementation also performs basic failover in case the randomly
 * selected host is not available for receiving the event.
 * </p>
 */
public class LoadBalancingRpcClient extends AbstractRpcClient {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(LoadBalancingRpcClient.class);

  private List<HostInfo> hosts;
  private HostSelector selector;
  private Map<String, RpcClient> clientMap;
  private Properties configurationProperties;

  @Override
  public void append(Event event) throws EventDeliveryException {
    boolean eventSent = false;
    Iterator<HostInfo> it = selector.createHostIterator();

    while (it.hasNext()) {
      HostInfo host = it.next();
      try {
        RpcClient client = getClient(host);
        client.append(event);
        eventSent = true;
        break;
      } catch (Exception ex) {
        LOGGER.warn("Failed to send event to host " + host, ex);
      }
    }

    if (!eventSent) {
      throw new EventDeliveryException("Unable to send event to any host");
    }
  }

  @Override
  public void appendBatch(List<Event> events) throws EventDeliveryException {
    boolean batchSent = false;
    Iterator<HostInfo> it = selector.createHostIterator();

    while (it.hasNext()) {
      HostInfo host = it.next();
      RpcClient client = getClient(host);
      try {
        client.appendBatch(events);
        batchSent = true;
        break;
      } catch (Exception ex) {
        LOGGER.warn("Failed to send batch to host " + host, ex);
      }
    }

    if (!batchSent) {
      throw new EventDeliveryException("Unable to send batch to any host");
    }
  }

  @Override
  public boolean isActive() {
    // This client is always active and does not need to be replaced.
    // Internally it will test the delegates and replace them where needed.
    return true;
  }

  @Override
  public void close() throws FlumeException {
    synchronized (this) {
      Iterator<String> it = clientMap.keySet().iterator();
      while (it.hasNext()) {
        String name = it.next();
        RpcClient client = clientMap.get(name);
        if (client != null) {
          try {
            client.close();
          } catch (Exception ex) {
            LOGGER.warn("Failed to close client: " + name, ex);
          }
        }
        it.remove();
      }
    }
  }

  @Override
  protected void configure(Properties properties) throws FlumeException {
    clientMap = new HashMap<String, RpcClient>();
    configurationProperties = new Properties();
    configurationProperties.putAll(properties);
    hosts = HostInfo.getHostInfoList(properties);
    if (hosts.size() < 2) {
      throw new FlumeException("At least two hosts are required to use the "
          + "load balancing RPC client.");
    }

    String lbTypeName = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_HOST_SELECTOR,
        RpcClientConfigurationConstants.HOST_SELECTOR_ROUND_ROBIN);

    if (lbTypeName.equalsIgnoreCase(
        RpcClientConfigurationConstants.HOST_SELECTOR_ROUND_ROBIN)) {
      selector = new RoundRobinHostSelector();
    } else if (lbTypeName.equalsIgnoreCase(
        RpcClientConfigurationConstants.HOST_SELECTOR_RANDOM)) {
      selector = new RandomOrderHostSelector();
    } else {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends HostSelector> klass = (Class<? extends HostSelector>)
            Class.forName(lbTypeName);

        selector = klass.newInstance();
      } catch (Exception ex) {
        throw new FlumeException("Unable to instantiate host selector: "
            + lbTypeName, ex);
      }
    }

    selector.setHosts(hosts);
  }

  private synchronized RpcClient getClient(HostInfo info) {
    String name = info.getReferenceName();
    RpcClient client = clientMap.get(name);
    if (client == null) {
      client = createClient(name);
      clientMap.put(name, client);
    } else if (!client.isActive()) {
      try {
        client.close();
      } catch (Exception ex) {
        LOGGER.warn("Failed to close client for " + info, ex);
      }
      client = createClient(name);
      clientMap.put(name, client);
    }

    return client;
  }

  private RpcClient createClient(String referenceName) {
    Properties props = getClientConfigurationProperties(referenceName);
    return RpcClientFactory.getInstance(props);
  }

  private Properties getClientConfigurationProperties(String referenceName) {
    Properties props = new Properties();
    props.putAll(configurationProperties);
    props.put(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
        RpcClientFactory.ClientType.DEFAULT);
    props.put(RpcClientConfigurationConstants.CONFIG_HOSTS, referenceName);

    return props;
  }

  public interface HostSelector {

    void setHosts(List<HostInfo> hosts);

    Iterator<HostInfo> createHostIterator();
  }

  /**
   * A host selector that implements the round-robin host selection policy.
   */
  private static class RoundRobinHostSelector implements HostSelector {

    private int nextHead;

    private List<HostInfo> hostList;

    @Override
    public synchronized Iterator<HostInfo> createHostIterator() {

      int size = hostList.size();
      int[] indexOrder = new int[size];

      int begin = nextHead++;
      if (nextHead == size) {
        nextHead = 0;
      }

      for (int i=0; i < size; i++) {
        indexOrder[i] = (begin + i)%size;
      }

      return new SpecificOrderIterator<HostInfo>(indexOrder, hostList);
    }

    @Override
    public synchronized void setHosts(List<HostInfo> hosts) {
      List<HostInfo> infos = new ArrayList<HostInfo>();
      infos.addAll(hosts);
      hostList = Collections.unmodifiableList(infos);
    }
  }

  private static class RandomOrderHostSelector implements HostSelector {

    private List<HostInfo> hostList;

    private Random random = new Random(System.currentTimeMillis());

    @Override
    public synchronized Iterator<HostInfo> createHostIterator() {
      int size = hostList.size();
      int[] indexOrder = new int[size];

      List<Integer> indexList = new ArrayList<Integer>();
      for (int i=0; i<size; i++) {
        indexList.add(i);
      }

      while (indexList.size() != 1) {
        int position = indexList.size();
        int pick = random.nextInt(position);
        indexOrder[position - 1] = indexList.remove(pick);
      }

      indexOrder[0] = indexList.get(0);

      return new SpecificOrderIterator<HostInfo>(indexOrder, hostList);
    }

    @Override
    public synchronized void setHosts(List<HostInfo> hosts) {
      List<HostInfo> infos = new ArrayList<HostInfo>();
      infos.addAll(hosts);
      hostList = Collections.unmodifiableList(infos);
    }
  }

}
