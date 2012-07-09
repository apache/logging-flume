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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Avro/Netty implementation of {@link RpcClient} which supports failover. This
 * takes a list of hostname port combinations and connects to the next available
 * (looping back to the first) host, from a given list of agents in the order
 * provided.
 *
 *
 * The properties used to build a FailoverRpcClient must have:
 * <p><tt>hosts</tt> = <i>alias_for_host1</i> <i>alias_for_host2</i></p> ...
 * <p><tt>hosts.alias_for_host1</tt> = <i>hostname1:port1</i>. </p>
 * <p><tt>hosts.alias_for_host2</tt> = <i>hostname2:port2</i>. </p> etc
 * <p>Optionally it can also have a <p>
 * <tt>batch-size</tt> = <i>batchSize</i>
 * <tt>max-attempts</tt> = <i>maxAttempts</i>
 *
 * Given a failure, this client will attempt to append to <i>maxAttempts</i>
 * clients in the <i>hosts</i> list immediately following the failed host
 * (looping back to the beginning of the <i>hosts</i> list.
 */

public class FailoverRpcClient extends AbstractRpcClient implements RpcClient {
  private volatile RpcClient client;
  private List<HostInfo> hosts;
  private Integer maxTries;
  private int lastCheckedhost;
  private boolean isActive;
  private Properties configurationProperties;

  private static final Logger logger = LoggerFactory
      .getLogger(FailoverRpcClient.class);

  protected FailoverRpcClient() {
    lastCheckedhost = -1;
    client = null;
  }

  //This function has to be synchronized to establish a happens-before
  //relationship for different threads that access this object
  //since shared data structures are created here.
  private synchronized void configureHosts(Properties properties)
      throws FlumeException {
    if(isActive){
      logger.error("This client was already configured, " +
          "cannot reconfigure.");
      throw new FlumeException("This client was already configured, " +
          "cannot reconfigure.");
    }
    hosts = HostInfo.getHostInfoList(properties);
    String tries = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_MAX_ATTEMPTS);
    if (tries == null || tries.isEmpty()){
      maxTries = hosts.size();
    } else {
      try {
        maxTries = Integer.parseInt(tries);
      } catch (NumberFormatException e) {
        maxTries = hosts.size();
      }
    }

    String strBatchSize = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_BATCH_SIZE);

    if (strBatchSize != null && strBatchSize.trim().length() > 0) {
      try {
        batchSize = Integer.parseInt(strBatchSize);
        if (batchSize < 1) {
          logger.warn("A batch-size less than 1 was specified: " + batchSize
              + ". Using default instead.");
          batchSize = RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE;
        }
      } catch (NumberFormatException ex) {
        logger.warn("Invalid batch size specified: " + strBatchSize
            + ". Using default instead.");
      }

    }
    isActive = true;
  }

  /**
   * Get the maximum number of "failed" hosts the client will try to establish
   * connection to before throwing an exception. Failed = was able to set up a
   * connection, but failed / returned error when the client tried to send data,
   *
   * @return The maximum number of failed retries
   */
  protected Integer getMaxTries() {
    return maxTries;
  }

  private synchronized RpcClient getClient() {
    if (client == null || !this.client.isActive()) {
      client = getNextClient();
      return client;
    } else {
      return client;
    }
  }

  /**
   * Tries to append an event to the currently connected client. If it cannot
   * send the event, it tries to send to next available host
   *
   * @param event The event to be appended.
   *
   * @throws EventDeliveryException
   */
  @Override
  public void append(Event event) throws EventDeliveryException {
    //Why a local variable rather than just calling getClient()?
    //If we get an EventDeliveryException, we need to call close on
    //that specific client, getClient in this case, will get us
    //the next client - leaving a resource leak.
    RpcClient localClient = null;
    synchronized (this) {
      if (!isActive) {
        logger.error("Attempting to append to an already closed client.");
        throw new EventDeliveryException(
            "Attempting to append to an already closed client.");
      }
    }
    // Sit in an infinite loop and try to append!
    int tries = 0;
    while (tries < maxTries) {
      try {
        tries++;
        localClient = getClient();
        localClient.append(event);
        return;
      } catch (EventDeliveryException e) {
        // Could not send event through this client, try to pick another client.
        logger.warn("Client failed. Exception follows: ", e);
        localClient.close();
        localClient = null;
      } catch (Exception e2) {
        logger.error("Failed to send event: ", e2);
        throw new EventDeliveryException(
            "Failed to send event. Exception follows: ", e2);
      }
    }
    logger.error("Tried many times, could not send event.");
    throw new EventDeliveryException("Failed to send the event!");
  }

  /**
   * Tries to append a list of events to the currently connected client. If it
   * cannot send the event, it tries to send to next available host
   *
   * @param events The events to be appended.
   *
   * @throws EventDeliveryException
   */
  @Override
  public void appendBatch(List<Event> events)
      throws EventDeliveryException {
    RpcClient localClient = null;
    synchronized (this) {
      if (!isActive) {
        logger.error("Attempting to append to an already closed client.");
        throw new EventDeliveryException(
            "Attempting to append to an already closed client!");
      }
    }
    int tries = 0;
    while (tries < maxTries) {
      try {
        tries++;
        localClient = getClient();
        localClient.appendBatch(events);
        return;
      } catch (EventDeliveryException e) {
        // Could not send event through this client, try to pick another client.
        logger.warn("Client failed. Exception follows: ", e);
        localClient.close();
        localClient = null;
      } catch (Exception e1) {
        logger.error("No clients active: ", e1);
        throw new EventDeliveryException("No clients currently active. " +
            "Exception follows: ", e1);
      }
    }
    logger.error("Tried many times, could not send event.");
    throw new EventDeliveryException("Failed to send the event!");
  }

  // Returns false if and only if this client has been closed explicitly.
  // Should we check if any clients are active, if none are then return false?
  // This method has to be lightweight, so not checking if hosts are active.
  @Override
  public synchronized boolean isActive() {
    return isActive;
  }

  /**
   * Close the connection. This function is safe to call over and over.
   */
  @Override
  public synchronized void close() throws FlumeException {
    if (client != null) {
      client.close();
      isActive = false;
    }
  }

  /**
   * Get the last socket address this client connected to. No guarantee this
   * will be the next it will connect to. If this host is down, it will connect
   * to another host. To be used only from the unit tests!
   * @return The last socket address this client connected to
   */
  protected InetSocketAddress getLastConnectedServerAddress() {
    HostInfo hostInfo = hosts.get(lastCheckedhost);
    return new InetSocketAddress(hostInfo.getHostName(),
        hostInfo.getPortNumber());
  }

  private RpcClient getNextClient() throws FlumeException {
    lastCheckedhost =
        (lastCheckedhost == (hosts.size() - 1)) ? -1 : lastCheckedhost;
    RpcClient localClient = null;
    int limit = hosts.size();

    Properties props = new Properties();
    props.putAll(configurationProperties);
    props.put(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
        RpcClientConfigurationConstants.DEFAULT_CLIENT_TYPE);
    //Try to connect to all hosts again, till we find one available
    for (int count = lastCheckedhost + 1; count < limit; count++) {
      HostInfo hostInfo = hosts.get(count);
      try {
        setDefaultProperties(hostInfo, props);
        localClient = RpcClientFactory.getInstance(props);
        lastCheckedhost = count;
        return localClient;
      } catch (FlumeException e) {
        logger.info("Could not connect to " + hostInfo, e);
        continue;
      }
    }
    for(int count = 0; count <= lastCheckedhost; count++) {
      HostInfo hostInfo = hosts.get(count);
      try {
        setDefaultProperties(hostInfo, props);
        localClient = RpcClientFactory.getInstance(props);
        lastCheckedhost = count;
        return localClient;
      } catch (FlumeException e) {
        logger.info("Could not connect to " + hostInfo, e);
        continue;
      }
    }
    if (localClient == null) {
      lastCheckedhost = -1;
      logger.error("No active client found.");
      throw new FlumeException("No active client.");
    }
    // This return should never be reached!
    return localClient;
  }

  private void setDefaultProperties(HostInfo hostInfo, Properties props) {
    props.put(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
        RpcClientFactory.ClientType.DEFAULT.name());
    props.put(RpcClientConfigurationConstants.CONFIG_HOSTS,
        hostInfo.getReferenceName());
  }

  @Override
  public void configure(Properties properties) throws FlumeException {
    configurationProperties = new Properties();
    configurationProperties.putAll(properties);

    configureHosts(configurationProperties);
  }
}
